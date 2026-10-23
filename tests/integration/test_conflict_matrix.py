"""Stage 1: Empirical verification of config/conflict_matrix.yaml.

Uses the CAPL hold mechanism to deterministically interleave pending x committed
operations and record OCC outcomes.

Each rule is tested with:
  TRIALS_OVERLAP  = 20 repeats with data overlap (partition or file)
  TRIALS_CONTROL  = 5  repeats without data overlap (control: expect K1 or success)

Run:
  make matrix-verify
  = docker compose exec runner pytest -q tests/integration/test_conflict_matrix.py
    && python3 scripts/update_matrix.py
"""
from __future__ import annotations

import contextlib
import json
import os
import sys
import threading
import time
from collections import Counter
from dataclasses import dataclass
from typing import Any

import pytest
import requests
import yaml

sys.path.insert(0, "/workspace")
from src.runner.ops.errors import classify

# ── endpoints ────────────────────────────────────────────────────────────────
PROXY_URL   = os.environ.get("CAPL_PROXY_URI",   "http://capl:8180")
ADV_URL     = os.environ.get("CAPL_ADVISOR_URI", "http://capl:8190")
S3_URL      = os.environ.get("S3_ENDPOINT",      "http://toxiproxy:18333")
WAREHOUSE   = os.environ.get("WAREHOUSE",        "s3://warehouse/")

MATRIX_YAML  = "/workspace/config/conflict_matrix.yaml"
RESULTS_FILE = "/workspace/results/stage-1/matrix_results.json"

TRIALS_OVERLAP  = int(os.environ.get("MATRIX_TRIALS_OVERLAP",  "20"))
TRIALS_CONTROL  = int(os.environ.get("MATRIX_TRIALS_CONTROL",  "5"))
HOLD_MAX_MS     = int(os.environ.get("MATRIX_HOLD_MAX_MS", "60000"))
PENDING_SLOT    = "matrix-pending"
COMMITTED_SLOT  = "matrix-committed"
NS              = "matrix_ns"

# Catalog names configured in the SparkSession (see conftest / session fixture):
#   cat_p  → PROXY_URL, header.X-CAPL-Slot=matrix-pending
#   cat_c  → PROXY_URL, header.X-CAPL-Slot=matrix-committed
CAT_P = "cat_p"
CAT_C = "cat_c"


# ── test-case definition ─────────────────────────────────────────────────────

@dataclass
class Case:
    pending:   str
    committed: str
    isolation: str    # "serializable" | "snapshot" | "default"
    fmt_ver:   int    # 2 | 3
    overlap:   str    # "none" | "partition" | "file"
    expected:  str    # "K1" | "K2" | "success"
    note:      str = ""

    @property
    def case_id(self) -> str:
        return (
            f"{self.pending}_x_{self.committed}"
            f"_{self.isolation}_v{self.fmt_ver}"
        )

    @property
    def table_name(self) -> str:
        # Stable table name reused across trials of the same case
        return f"t_{self.pending}_{self.committed}_{self.isolation}_v{self.fmt_ver}"


# ── derive cases from YAML ────────────────────────────────────────────────────

def _load_cases() -> list[Case]:
    with open(MATRIX_YAML) as fh:
        mx = yaml.safe_load(fh)
    _fv_env = os.environ.get("MATRIX_FORMAT_VERSIONS", "")
    fmt_versions: list[int] = (
        [int(x) for x in _fv_env.split(",") if x.strip()]
        if _fv_env
        else mx.get("format_versions", [2, 3])
    )
    default_iso = mx["defaults"].get("isolation", "serializable")
    cases: list[Case] = []

    for rule in mx["rules"]:
        pending    = rule["pending"]
        committed_ = rule["committed"]
        outcome    = rule["outcome"]
        overlap    = rule["overlap"]
        when       = rule.get("when", {})
        note       = rule.get("note", "")
        isolation  = when.get("isolation", default_iso)

        if committed_ == "*":
            # wildcard: test against a representative committed op
            committed_list = ["append"]
        elif isinstance(committed_, list):
            committed_list = committed_
        else:
            committed_list = [committed_]

        for committed in committed_list:
            for fv in fmt_versions:
                # rewrite_deletes needs delete files → only sensible for v2/v3
                # expire has no format dependency
                cases.append(Case(
                    pending=pending,
                    committed=committed,
                    isolation=isolation,
                    fmt_ver=fv,
                    overlap=overlap,
                    expected=outcome,
                    note=note,
                ))

    # Extra: overwrite without explicit isolation-level option (default behaviour)
    for fv in fmt_versions:
        cases.append(Case(
            pending="overwrite",
            committed="append",
            isolation="default",
            fmt_ver=fv,
            overlap="partition",
            expected="K2",
            note="INSERT OVERWRITE без явной опции isolation-level",
        ))

    # Deduplicate (same case_id may arise from symmetric rules)
    seen: set[str] = set()
    unique: list[Case] = []
    for c in cases:
        if c.case_id not in seen:
            seen.add(c.case_id)
            unique.append(c)
    return unique


CASES = _load_cases()


# ── Spark session (module-scoped) ─────────────────────────────────────────────

@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    def _cat_cfg(name: str, slot: str) -> dict[str, str]:
        return {
            f"spark.sql.catalog.{name}":                    "org.apache.iceberg.spark.SparkCatalog",
            f"spark.sql.catalog.{name}.type":               "rest",
            f"spark.sql.catalog.{name}.uri":                PROXY_URL,
            f"spark.sql.catalog.{name}.warehouse":          WAREHOUSE,
            f"spark.sql.catalog.{name}.io-impl":            "org.apache.iceberg.aws.s3.S3FileIO",
            f"spark.sql.catalog.{name}.s3.endpoint":        S3_URL,
            f"spark.sql.catalog.{name}.s3.path-style-access": "true",
            f"spark.sql.catalog.{name}.cache-enabled":      "false",
            f"spark.sql.catalog.{name}.header.X-CAPL-Slot": slot,
            f"spark.sql.catalog.{name}.header.X-CAPL-Writer": "test",
        }

    builder = (
        SparkSession.builder.appName("matrix-verify")
        .master("local[4]")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.driver.memory", "1g")
        .config("spark.executor.memory", "1g")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
    )
    for k, v in _cat_cfg(CAT_P, PENDING_SLOT).items():
        builder = builder.config(k, v)
    for k, v in _cat_cfg(CAT_C, COMMITTED_SLOT).items():
        builder = builder.config(k, v)

    session = builder.getOrCreate()
    session.sparkContext.setLogLevel("ERROR")

    # Bootstrap namespace
    session.sql(f"CREATE NAMESPACE IF NOT EXISTS {CAT_P}.{NS}")

    yield session
    session.stop()


# ── table lifecycle ──────────────────────────────────────────────────────────

def _tblprops(pending: str, committed: str, fmt_ver: int) -> str:
    props = {
        "format-version": str(fmt_ver),
    }
    ops = {pending, committed}
    if "cow_dml" in ops:
        props["write.update.mode"] = "copy-on-write"
        props["write.merge.mode"]  = "copy-on-write"
    if "mor_dml" in ops:
        props["write.delete.mode"] = "merge-on-read"
        if "cow_dml" not in ops:
            props["write.update.mode"] = "merge-on-read"
            props["write.merge.mode"]  = "merge-on-read"
    return ", ".join(f"'{k}' = '{v}'" for k, v in props.items())


def _setup_table(spark: Any, case: Case, need_delete_files: bool = False) -> None:
    """(Re)create table and insert base data for a test case."""
    full = f"{CAT_P}.{NS}.{case.table_name}"
    spark.sql(f"DROP TABLE IF EXISTS {full}")
    tprops = _tblprops(case.pending, case.committed, case.fmt_ver)
    spark.sql(f"""
        CREATE TABLE {full} (id INT, bkt INT, val STRING)
        USING iceberg PARTITIONED BY (bkt)
        TBLPROPERTIES ({tprops})
    """)
    # Insert initial rows: bkt 0 and bkt 1 (4 small files each for compaction tests)
    batches = 4 if case.pending in ("rewrite_data", "rewrite_deletes") else 1
    for b in range(batches):
        for bkt in (0, 1):
            off = b * 100
            spark.sql(f"""
                INSERT INTO {full}
                SELECT (id + {off}) AS id, {bkt} AS bkt,
                       CONCAT('init-{b}-', CAST(id + {off} AS STRING)) AS val
                FROM RANGE(50)
            """)
    if need_delete_files:
        # Create position delete files needed for rewrite_deletes
        spark.sql(f"DELETE FROM {full} WHERE bkt = 0 AND id < 10")


# ── single-trial execution ────────────────────────────────────────────────────

def _exec_op(
    spark: Any,
    op: str,
    cat: str,
    table: str,
    bkt: int,
    seq: int,
) -> None:
    full = f"{cat}.{NS}.{table}"

    if op in ("append", "stream_upsert"):
        off = 10000 + seq * 50
        spark.sql(f"""
            INSERT INTO {full}
            SELECT (id + {off}) AS id, {bkt} AS bkt,
                   CONCAT('{op}-{seq}-', CAST(id + {off} AS STRING)) AS val
            FROM RANGE(10)
        """)

    elif op == "overwrite":
        (
            spark.range(seq * 50, seq * 50 + 10)
            .selectExpr(
                "CAST(id AS INT) AS id",
                f"{bkt} AS bkt",
                f"CONCAT('ow-{seq}-', CAST(id AS STRING)) AS val",
            )
            .writeTo(full)
            .overwritePartitions()
        )

    elif op == "cow_dml":
        spark.sql(f"""
            UPDATE {full}
            SET val = CONCAT('upd-{seq}-', CAST(id AS STRING))
            WHERE bkt = {bkt}
        """)

    elif op == "mor_dml":
        spark.sql(f"""
            DELETE FROM {full}
            WHERE bkt = {bkt} AND id < {seq * 5 + 5}
        """)

    elif op == "rewrite_data":
        spark.sql(f"""
            CALL {cat}.system.rewrite_data_files(table => '{NS}.{table}')
        """)

    elif op == "rewrite_deletes":
        spark.sql(f"""
            CALL {cat}.system.rewrite_position_delete_files(table => '{NS}.{table}')
        """)

    elif op == "expire":
        spark.sql(f"""
            CALL {cat}.system.expire_snapshots(table => '{NS}.{table}', retain_last => 1)
        """)

    else:
        raise ValueError(f"Unknown op: {op}")


def _run_one_trial(
    spark: Any,
    case: Case,
    seq: int,
    with_overlap: bool,
) -> dict[str, Any]:
    """Run one trial; return {'outcome': 'K1'|'K2'|'success'|'error', 'exc': str}."""
    pending_bkt   = 0
    committed_bkt = 0 if with_overlap else 1   # no overlap → different partition

    # 1. Count existing pending commits to detect new arrival
    before_commits = requests.get(
        f"{ADV_URL}/_capl/debug/last-commits", timeout=10
    ).json()
    pre_count = sum(1 for c in before_commits if c["slot"] == PENDING_SLOT)

    # 2. Register hold
    r = requests.post(
        f"{ADV_URL}/v1/debug/hold",
        json={"slot": PENDING_SLOT, "max_ms": HOLD_MAX_MS},
        timeout=10,
    )
    assert r.status_code == 200, f"hold register: {r.status_code}"

    outcome: dict[str, Any] = {}

    def _pending() -> None:
        try:
            _exec_op(spark, case.pending, CAT_P, case.table_name, pending_bkt, seq)
            outcome["result"] = "success"
        except Exception as exc:
            text = str(exc)
            outcome["result"] = classify(text)
            outcome["exc"] = text[:500]

    thr = threading.Thread(target=_pending, daemon=True)
    thr.start()

    # 3. Wait for pending commit to arrive at proxy (max 45 s)
    held = False
    deadline = time.monotonic() + 45
    while time.monotonic() < deadline:
        time.sleep(0.4)
        commits = requests.get(
            f"{ADV_URL}/_capl/debug/last-commits", timeout=10
        ).json()
        cur_count = sum(1 for c in commits if c["slot"] == PENDING_SLOT)
        if cur_count > pre_count:
            held = True
            break

    if not held:
        # Pending op failed before reaching commit (or expired); release and collect
        with contextlib.suppress(Exception):
            requests.delete(
                f"{ADV_URL}/v1/debug/hold/{PENDING_SLOT}", timeout=5
            )
        thr.join(timeout=30)
        if "result" not in outcome:
            outcome["result"] = "no_commit"
        outcome["held"] = False
        return outcome

    outcome["held"] = True

    # 4. Execute committed operation (different slot, no hold)
    with contextlib.suppress(Exception):
        _exec_op(spark, case.committed, CAT_C, case.table_name, committed_bkt, seq)

    # 5. Release hold → pending proceeds
    with contextlib.suppress(Exception):
        requests.delete(
            f"{ADV_URL}/v1/debug/hold/{PENDING_SLOT}", timeout=5
        )

    # 6. Wait for pending thread
    thr.join(timeout=60)
    if "result" not in outcome:
        outcome["result"] = "timeout"

    return outcome


# ── test function ─────────────────────────────────────────────────────────────

_all_results: dict[str, Any] = {}


@pytest.mark.parametrize("case", CASES, ids=lambda c: c.case_id)
def test_pair(spark: Any, case: Case) -> None:
    """Run TRIALS_OVERLAP + TRIALS_CONTROL trials for one conflict matrix pair."""
    need_deletes = case.pending == "rewrite_deletes"
    _setup_table(spark, case, need_delete_files=need_deletes)

    overlap_outcomes: list[str] = []
    control_outcomes: list[str] = []

    # --- overlap trials ---
    for seq in range(TRIALS_OVERLAP):
        result = _run_one_trial(spark, case, seq, with_overlap=True)
        overlap_outcomes.append(result.get("result", "error"))
        # After each trial refresh table data so pending has something to act on
        if case.pending in ("mor_dml", "cow_dml", "rewrite_data", "rewrite_deletes"):
            full = f"{CAT_P}.{NS}.{case.table_name}"
            roff = 20000 + seq * 50
            spark.sql(f"""
                INSERT INTO {full}
                SELECT (id + {roff}) AS id, 0 AS bkt,
                       CONCAT('refill-{seq}-', CAST(id + {roff} AS STRING)) AS val
                FROM RANGE(20)
            """)

    # --- control trials (no overlap) ---
    for seq in range(TRIALS_CONTROL):
        result = _run_one_trial(spark, case, 1000 + seq, with_overlap=False)
        control_outcomes.append(result.get("result", "error"))

    # --- aggregate ---
    overlap_counts  = dict(Counter(overlap_outcomes))
    control_counts  = dict(Counter(control_outcomes))

    expected = case.expected
    match_rate = overlap_counts.get(expected, 0) / max(len(overlap_outcomes), 1)

    _all_results[case.case_id] = {
        "pending":         case.pending,
        "committed":       case.committed,
        "isolation":       case.isolation,
        "format_version":  case.fmt_ver,
        "overlap":         case.overlap,
        "expected":        expected,
        "overlap_counts":  overlap_counts,
        "control_counts":  control_counts,
        "trials_overlap":  TRIALS_OVERLAP,
        "trials_control":  TRIALS_CONTROL,
        "match_rate":      round(match_rate, 3),
        "matches":         match_rate >= 0.95,
    }

    # Persist results incrementally
    os.makedirs(os.path.dirname(RESULTS_FILE), exist_ok=True)
    with open(RESULTS_FILE, "w") as fh:
        json.dump(_all_results, fh, indent=2)

    # Assert ≥ 95% match rate
    assert match_rate >= 0.95, (
        f"Case {case.case_id}: expected {expected} in ≥95% of overlap trials, "
        f"got {overlap_counts} (match_rate={match_rate:.2%})"
    )
    # Control: should NOT be K2 (no overlap → no data conflict)
    if expected == "K2":
        k2_control = control_counts.get("K2", 0)
        assert k2_control == 0, (
            f"Control with no overlap got K2: {control_counts}"
        )
