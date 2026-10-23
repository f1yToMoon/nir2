"""Stage 1: empirical verification of config/conflict_matrix.yaml (iteration 3).

Each trial: fresh rows (unique id range) → pending op is started and its first commit is
held by the CAPL proxy → committed op runs and commits → hold released → outcome of
pending recorded (success / K1 = HTTP 409 then retry / K2 = ValidationException).

A trial counts as valid only if the artifacts show it (REVIEWER.md P5):
  * held:       pending's first commit was held by the proxy, no hold timeout;
  * in_window:  every committed commit got its catalog response while pending was held;
  * effects:    pending and committed commit bodies (proxy log, add-snapshot summary /
                remove-snapshots) show the intended operation and a real data change
                (runner.ops.effects);
  * overlap:    the footprints computed from the manifests of both snapshots intersect as
                the rule requires (file / partition / predicate); in control trials
                they must not intersect.
The pending snapshot's manifests are read while it is still held (after a failed commit
the client deletes them).

Spark commit messages ("Committing … conflictDetectionFilter …") are captured from the
driver log (tests/integration/log4j2-matrix.properties) and stored per trial.

Run:
  make matrix-verify
Env: MATRIX_TRIALS_OVERLAP (20), MATRIX_TRIALS_CONTROL (5), MATRIX_FORMAT_VERSIONS (yaml),
     MATRIX_OUT (results/stage-1/iter3), MATRIX_SPARK_MASTER (spark://spark-master:7077),
     MATRIX_ONLY (substring filter on case id)
"""
from __future__ import annotations

import contextlib
import json
import os
import re
import sys
import threading
import time
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import pytest
import requests
import yaml

sys.path.insert(0, "/workspace/src")
from runner.ops.effects import (
    OP_KIND,
    added_snapshot,
    commit_effect,
    in_hold_window,
    removed_snapshot_ids,
    trial_outcome,
)
from runner.ops.errors import classify

# ── endpoints ────────────────────────────────────────────────────────────────
PROXY_URL    = os.environ.get("CAPL_PROXY_URI",   "http://capl:8180")
ADV_URL      = os.environ.get("CAPL_ADVISOR_URI", "http://capl:8190")
S3_URL       = os.environ.get("S3_ENDPOINT",      "http://toxiproxy:18333")
WAREHOUSE    = os.environ.get("WAREHOUSE",        "s3://warehouse/")
SPARK_MASTER = os.environ.get("MATRIX_SPARK_MASTER", "spark://spark-master:7077")

MATRIX_YAML = "/workspace/config/conflict_matrix.yaml"
OUT_DIR     = os.environ.get("MATRIX_OUT", "/workspace/results/stage-1/iter3")
RESULTS_FILE = f"{OUT_DIR}/matrix_results.json"
COMMIT_LOG   = f"{OUT_DIR}/spark-commits.log"
LOG4J_CONF   = "/workspace/tests/integration/log4j2-matrix.properties"

TRIALS_OVERLAP = int(os.environ.get("MATRIX_TRIALS_OVERLAP", "20"))
TRIALS_CONTROL = int(os.environ.get("MATRIX_TRIALS_CONTROL", "5"))
MIN_VALID      = int(os.environ.get("MATRIX_MIN_VALID", str(TRIALS_OVERLAP // 2)))
ONLY           = os.environ.get("MATRIX_ONLY", "")
SLOT_SUFFIX    = os.environ.get("MATRIX_SLOT_SUFFIX", "")
PENDING_SLOT   = f"matrix-pending{SLOT_SUFFIX}"
COMMITTED_SLOT = f"matrix-committed{SLOT_SUFFIX}"
CORES_MAX      = os.environ.get("MATRIX_CORES_MAX", "4")
NS             = "matrix_ns"

CAT_P = "cat_p"   # pending catalog → proxy with PENDING_SLOT header
CAT_C = "cat_c"   # committed catalog → proxy with COMMITTED_SLOT header

# Per-trial data layout (matrix table: id INT, bkt INT, val STRING, PARTITIONED BY bkt)
TRIAL_ROWS   = 48         # rows per trial per bucket (4 files x 12 for compaction cases)
DML_ROWS     = 10         # DML predicate covers ids [base, base+DML_ROWS-1] — a part of a file
TRIAL_STRIDE = 10_000
BKT_OFFSET   = 1_000_000
EXPIRE_BKT   = 2          # append_expire appends here (outside both footprints)

_MOR_DELETE_OPS = {"mor_dml", "mor_delete", "rewrite_deletes"}
_REWRITE_OPS    = {"rewrite_data", "rewrite_deletes"}
_ROWLEVEL_OPS   = {"cow_dml", "mor_dml", "mor_delete", "mor_update", "mor_merge"}
# '*' in a rule expands to one op of each commit kind
STAR_COMMITTED  = ["append", "cow_dml", "mor_delete", "rewrite_data"]


# ── test-case definition ─────────────────────────────────────────────────────

@dataclass
class Case:
    pending:   str
    committed: str
    isolation: str          # "serializable" | "snapshot"
    fmt_ver:   int
    overlap:   str          # "none" | "partition" | "file" | "predicate"
    expected:  str          # "K1" | "K2" | "success" | other observed class
    variant:   str = ""     # "" | "nostartseq"
    note:      str = ""
    control_expected_k2: bool = False

    @property
    def case_id(self) -> str:
        v = f"_{self.variant}" if self.variant else ""
        return f"{self.pending}_x_{self.committed}_{self.isolation}_v{self.fmt_ver}{v}"

    @property
    def table_name(self) -> str:
        v = f"_{self.variant}" if self.variant else ""
        iso = "ser" if self.isolation == "serializable" else "snap"
        return f"t_{self.pending}_{self.committed}_{iso}_v{self.fmt_ver}{v}"

    @property
    def cow_stmt(self) -> str:
        """SQL statement used for cow_dml: the one the MoR op of the pair does not use."""
        return "merge" if "mor_update" in (self.pending, self.committed) else "update"


def _format_versions() -> list[int]:
    fv_env = os.environ.get("MATRIX_FORMAT_VERSIONS", "")
    if fv_env:
        return [int(x) for x in fv_env.split(",") if x.strip()]
    with open(MATRIX_YAML) as fh:
        return list(yaml.safe_load(fh).get("format_versions", [2]))


FORMAT_VERSIONS = _format_versions()


def _load_cases() -> list[Case]:
    with open(MATRIX_YAML) as fh:
        mx = yaml.safe_load(fh)
    fmt_versions = FORMAT_VERSIONS
    default_iso = mx["defaults"].get("isolation", "serializable")
    cases: list[Case] = []
    for rule in mx["rules"]:
        when = rule.get("when", {})
        committed_ = rule["committed"]
        if committed_ == "*":
            committed_list = STAR_COMMITTED
        elif isinstance(committed_, list):
            committed_list = committed_
        else:
            committed_list = [committed_]
        variant = ""
        if when.get("rewrite_use_starting_sequence_number") is False:
            variant = "nostartseq"
        rule_fv = when.get("format_version")
        rule_fvs = rule_fv if isinstance(rule_fv, list) else [rule_fv] if rule_fv else fmt_versions
        for committed in committed_list:
            for fv in rule_fvs:
                if fv not in fmt_versions:
                    continue
                cases.append(Case(
                    pending=rule["pending"], committed=committed,
                    isolation=when.get("isolation", default_iso), fmt_ver=fv,
                    overlap=rule["overlap"], expected=rule["outcome"], variant=variant,
                    note=rule.get("note", ""),
                ))
    seen: set[str] = set()
    unique: list[Case] = []
    for c in cases:
        if c.case_id not in seen and ONLY in c.case_id:
            seen.add(c.case_id)
            unique.append(c)
    return unique


CASES = _load_cases()


# ── Spark session and Java handles ───────────────────────────────────────────

@pytest.fixture(scope="module")
def spark() -> Any:
    from pyspark.sql import SparkSession

    os.makedirs(OUT_DIR, exist_ok=True)

    def _cat_cfg(name: str, slot: str) -> dict[str, str]:
        return {
            f"spark.sql.catalog.{name}":                      "org.apache.iceberg.spark.SparkCatalog",
            f"spark.sql.catalog.{name}.type":                 "rest",
            f"spark.sql.catalog.{name}.uri":                  PROXY_URL,
            f"spark.sql.catalog.{name}.warehouse":            WAREHOUSE,
            f"spark.sql.catalog.{name}.io-impl":              "org.apache.iceberg.aws.s3.S3FileIO",
            f"spark.sql.catalog.{name}.s3.endpoint":          S3_URL,
            f"spark.sql.catalog.{name}.s3.path-style-access": "true",
            f"spark.sql.catalog.{name}.cache-enabled":        "false",
            f"spark.sql.catalog.{name}.header.X-CAPL-Slot":   slot,
            f"spark.sql.catalog.{name}.header.X-CAPL-Writer": "test",
        }

    builder = (
        SparkSession.builder.appName("matrix-verify-iter3")
        .master(SPARK_MASTER)
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.driver.extraJavaOptions",
                f"-Dlog4j2.configurationFile=file:{LOG4J_CONF} -Dcapl.matrix.commitlog={COMMIT_LOG}")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.cores.max", CORES_MAX)
        .config("spark.driver.memory", "1536m")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .config("spark.eventLog.enabled", "false")
    )
    for k, v in {**_cat_cfg(CAT_P, PENDING_SLOT), **_cat_cfg(CAT_C, COMMITTED_SLOT)}.items():
        builder = builder.config(k, v)

    session = builder.getOrCreate()
    session.sql(f"CREATE NAMESPACE IF NOT EXISTS {CAT_P}.{NS}")
    yield session
    session.stop()


class Jv:
    """Iceberg Java API through py4j: table state and snapshot manifests."""

    def __init__(self, spark: Any) -> None:
        self.jvm = spark._jvm
        self.gw = spark.sparkContext._gateway
        self.cm = spark._jsparkSession.sessionState().catalogManager()

    def table(self, cat: str, name: str) -> Any:
        ns = self.gw.new_array(self.jvm.String, 1)
        ns[0] = NS
        ident = self.jvm.org.apache.spark.sql.connector.catalog.Identifier.of(ns, name)
        tbl = self.cm.catalog(cat).loadTable(ident).table()
        tbl.refresh()
        return tbl

    @staticmethod
    def current_id(tbl: Any) -> int | None:
        s = tbl.currentSnapshot()
        return int(s.snapshotId()) if s is not None else None

    @staticmethod
    def snapshot_ids(tbl: Any) -> list[int]:
        it = tbl.snapshots().iterator()
        out: list[int] = []
        while it.hasNext():
            out.append(int(it.next().snapshotId()))
        return out

    @staticmethod
    def _iter(jiterable: Any) -> list[Any]:
        it = jiterable.iterator()
        out = []
        while it.hasNext():
            out.append(it.next())
        return out

    def footprint(self, tbl: Any, snap: dict[str, Any], key_col: str) -> dict[str, Any]:
        """Files and partitions touched by a snapshot, read from its manifests."""
        sn = self.jvm.org.apache.iceberg.SnapshotParser.fromJson(json.dumps(snap))
        io = tbl.io()
        specs = tbl.specs()
        fld = tbl.schema().findField(key_col)
        conv = self.jvm.org.apache.iceberg.types.Conversions

        def part(f: Any) -> str:
            return str(specs.get(f.specId()).partitionToPath(f.partition()))

        def bounds(f: Any) -> list[int] | None:
            lb, ub = f.lowerBounds(), f.upperBounds()
            if lb is None or ub is None or lb.get(fld.fieldId()) is None:
                return None
            return [int(conv.fromByteBuffer(fld.type(), lb.get(fld.fieldId()))),
                    int(conv.fromByteBuffer(fld.type(), ub.get(fld.fieldId())))]

        def dfile(f: Any) -> dict[str, Any]:
            return {"path": str(f.location()), "partition": part(f), "bounds": bounds(f)}

        def delfile(f: Any) -> dict[str, Any]:
            ref = f.referencedDataFile()
            return {**dfile(f), "content": str(f.content().name()),
                    "referenced": str(ref) if ref is not None else None}

        fp = {
            "operation": str(sn.operation()),
            "added_data":      [dfile(f) for f in self._iter(sn.addedDataFiles(io))],
            "removed_data":    [dfile(f) for f in self._iter(sn.removedDataFiles(io))],
            "added_deletes":   [delfile(f) for f in self._iter(sn.addedDeleteFiles(io))],
            "removed_deletes": [delfile(f) for f in self._iter(sn.removedDeleteFiles(io))],
        }
        touched = {f["path"] for f in fp["removed_data"]}
        touched |= {f["referenced"] for f in fp["added_deletes"] + fp["removed_deletes"]
                    if f["referenced"]}
        fp["touched_files"] = sorted(touched)
        fp["partitions"] = sorted({f["partition"] for k in
                                   ("added_data", "removed_data", "added_deletes", "removed_deletes")
                                   for f in fp[k]})
        fp["unreferenced_pos_deletes"] = sum(
            1 for f in fp["added_deletes"] + fp["removed_deletes"]
            if f["content"] == "POSITION_DELETES" and not f["referenced"])
        return fp


def _overlaps(p_fp: dict[str, Any] | None, c_fp: dict[str, Any] | None,
              p_partition: str, p_range: tuple[int, int]) -> dict[str, bool]:
    """Overlap indicators between pending and committed footprints."""
    if p_fp is None or c_fp is None:
        return {"file": False, "partition": False, "predicate": False}
    file_ov = bool(set(p_fp["touched_files"]) & set(c_fp["touched_files"]))
    part_ov = bool(set(p_fp["partitions"]) & set(c_fp["partitions"]))
    lo, hi = p_range
    pred_ov = any(
        f["partition"] == p_partition and f["bounds"] is not None
        and f["bounds"][0] <= hi and f["bounds"][1] >= lo
        for f in c_fp["added_data"] + [d for d in c_fp["added_deletes"]
                                       if d["content"] == "EQUALITY_DELETES"]
    )
    return {"file": file_ov, "partition": part_ov, "predicate": pred_ov}


# ── proxy log and commit log ─────────────────────────────────────────────────

def _proxy_entries(slot: str, table: str, since_ms: int) -> list[dict[str, Any]]:
    log = requests.get(f"{ADV_URL}/_capl/debug/last-commits", timeout=10).json()
    return [e for e in log if e.get("slot") == slot and e.get("ts", 0) >= since_ms
            and e.get("path", "").endswith(f"/tables/{table}")]


def _commit_log_lines(table: str, since_ms: int) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    with contextlib.suppress(FileNotFoundError), open(COMMIT_LOG) as fh:
        for line in fh:
            parts = line.rstrip("\n").split("\t", 2)
            if len(parts) != 3 or not parts[0].isdigit():
                continue
            ts, cls, msg = int(parts[0]), parts[1], parts[2]
            if ts >= since_ms and msg.startswith("Committing") and msg.endswith(f".{table}"):
                cat = msg.rsplit(" ", 1)[-1].split(".", 1)[0]
                m = re.search(r"conflictDetectionFilter: (.*?)(?:, isolationLevel|\) to table|$)",
                              msg)
                out.append({"ts": ts, "class": cls, "catalog": cat, "msg": msg[:1500],
                            "filter": m.group(1) if m else None})
    return out


# ── table lifecycle ───────────────────────────────────────────────────────────

# SQL statement → write mode for each op that runs a row-level statement
def _stmt_modes(case: Case) -> dict[str, str]:
    want: dict[str, str] = {}
    for op in (case.pending, case.committed):
        if op == "cow_dml":
            stmt, mode = case.cow_stmt, "copy-on-write"
        elif op in _MOR_DELETE_OPS:
            stmt, mode = "delete", "merge-on-read"
        elif op == "mor_update":
            stmt, mode = "update", "merge-on-read"
        elif op == "mor_merge":
            stmt, mode = "merge", "merge-on-read"
        else:
            continue
        if want.get(stmt, mode) != mode:
            raise ValueError(f"{case.case_id}: {stmt} needs both modes")
        want[stmt] = mode
    return want


def _tblprops(case: Case) -> str:
    props: dict[str, str] = {"format-version": str(case.fmt_ver)}
    for stmt, mode in _stmt_modes(case).items():
        props[f"write.{stmt}.mode"] = mode
    if case.isolation == "snapshot":
        for stmt in ("delete", "update", "merge"):
            props[f"write.{stmt}.isolation-level"] = "snapshot"
    return ", ".join(f"'{k}' = '{v}'" for k, v in props.items())


def _setup_table(spark: Any, case: Case) -> None:
    full = f"{CAT_P}.{NS}.{case.table_name}"
    spark.sql(f"DROP TABLE IF EXISTS {full} PURGE")
    spark.sql(f"""
        CREATE TABLE {full} (id INT, bkt INT, val STRING)
        USING iceberg PARTITIONED BY (bkt)
        TBLPROPERTIES ({_tblprops(case)})
    """)


def _trial_base(seq: int, bkt: int) -> int:
    return seq * TRIAL_STRIDE + bkt * BKT_OFFSET


def _insert_trial_data(spark: Any, full: str, case: Case, seq: int, bkt: int) -> int:
    """Fresh rows for (seq, bkt): 4 files of 12 rows for compaction cases, else 1 file."""
    base = _trial_base(seq, bkt)
    nfiles = 4 if {case.pending, case.committed} & _REWRITE_OPS else 1
    chunk = TRIAL_ROWS // nfiles
    for i in range(nfiles):
        spark.sql(f"""
            INSERT INTO {full}
            SELECT CAST({base + i * chunk} + id AS INT), {bkt},
                   CONCAT('t{seq}f{i}-', CAST(id AS STRING))
            FROM RANGE({chunk})
        """)
    if case.pending == "rewrite_deletes":
        # two position-delete files for the first data file (the one holding the DML
        # range [base, base+DML_ROWS-1]), on rows outside that range
        for row in (base + DML_ROWS, base + DML_ROWS + 1):
            spark.sql(f"DELETE FROM {full} WHERE bkt = {bkt} AND id = {row}")
    return base


# ── op execution ─────────────────────────────────────────────────────────────

def _upsert_java(jv: Jv, tbl: Any, bkt: int, lo: int, seq: int) -> None:
    """Equality-delete upsert as Flink commits it: RowDelta(addRows, addDeletes), no
    validations (flink/v1.20 IcebergFilesCommitter.java:361-365)."""
    jvm = jv.jvm
    ff = jvm.org.apache.iceberg.FileFormat
    schema, spec = tbl.schema(), tbl.spec()
    ids = jv.gw.new_array(jvm.int, 1)
    ids[0] = schema.findField("id").fieldId()
    wf = (getattr(jvm.org.apache.iceberg.data, "GenericFileWriterFactory$Builder")(tbl)
          .dataFileFormat(ff.PARQUET).deleteFileFormat(ff.PARQUET)
          .equalityFieldIds(ids).equalityDeleteRowSchema(schema).build())
    off = (jvm.org.apache.iceberg.io.OutputFileFactory
           .builderFor(tbl, bkt, seq * 1000 + int(time.time()) % 1000).format(ff.PARQUET).build())
    rec = jvm.org.apache.iceberg.data.GenericRecord.create(schema)
    rec.setField("id", lo)
    rec.setField("bkt", bkt)
    rec.setField("val", f"upsert-{seq}")
    pk = jvm.org.apache.iceberg.PartitionKey(spec, schema)
    pk.partition(rec)
    dw = wf.newDataWriter(off.newOutputFile(spec, pk), spec, pk)
    ew = wf.newEqualityDeleteWriter(off.newOutputFile(spec, pk), spec, pk)
    for i in range(DML_ROWS):
        r = rec.copy()
        r.setField("id", lo + i)
        ew.write(r)
        dw.write(r)
    dw.close()
    ew.close()
    tbl.newRowDelta().addRows(dw.toDataFile()).addDeletes(ew.toDeleteFile()).commit()


def _exec_op(spark: Any, jv: Jv, case: Case, op: str, cat: str, bkt: int, seq: int,
             base: int, base_snapshot_id: int | None) -> None:
    table = case.table_name
    full = f"{cat}.{NS}.{table}"
    lo, hi = base, base + DML_ROWS - 1
    pred = f"bkt = {bkt} AND id BETWEEN {lo} AND {hi}"

    if op == "append":
        spark.sql(f"""
            INSERT INTO {full}
            SELECT CAST({lo} + id AS INT), {bkt}, CONCAT('{op}-{seq}-', CAST(id AS STRING))
            FROM RANGE({DML_ROWS})
        """)
    elif op == "stream_upsert":
        _upsert_java(jv, jv.table(cat, table), bkt, lo, seq)
    elif op in ("overwrite", "overwrite_opts"):
        w = (spark.range(lo, lo + DML_ROWS)
             .selectExpr("CAST(id AS INT) AS id", f"{bkt} AS bkt",
                         f"CONCAT('{op}-{seq}-', CAST(id AS STRING)) AS val")
             .writeTo(full))
        if op == "overwrite_opts":
            assert base_snapshot_id is not None, "overwrite_opts needs the base snapshot id"
            w = (w.option("isolation-level", "serializable")
                  .option("validate-from-snapshot-id", str(base_snapshot_id)))
        w.overwritePartitions()
    elif (op == "cow_dml" and case.cow_stmt == "update") or op == "mor_update":
        spark.sql(f"UPDATE {full} SET val = CONCAT('{op}-{seq}-', CAST(id AS STRING)) "
                  f"WHERE {pred}")
    elif op == "cow_dml" or op == "mor_merge":
        # target-side predicate in ON → pushed into the scan → conflictDetectionFilter
        spark.sql(f"""
            MERGE INTO {full} t
            USING (SELECT id, bkt, CONCAT('{op}-{seq}-', CAST(id AS STRING)) AS val
                   FROM {full} WHERE {pred}) s
            ON t.id = s.id AND t.bkt = {bkt} AND t.id BETWEEN {lo} AND {hi}
            WHEN MATCHED THEN UPDATE SET val = s.val
        """)
    elif op in ("mor_dml", "mor_delete"):
        spark.sql(f"DELETE FROM {full} WHERE {pred}")
    elif op == "rewrite_data":
        opts = "'min-input-files','1'"
        if case.variant == "nostartseq":
            opts += ",'use-starting-sequence-number','false'"
        spark.sql(f"""
            CALL {cat}.system.rewrite_data_files(
                table => '{NS}.{table}', where => 'bkt = {bkt}', options => map({opts}))
        """)
    elif op == "rewrite_deletes":
        spark.sql(f"""
            CALL {cat}.system.rewrite_position_delete_files(
                table => '{NS}.{table}', where => 'bkt = {bkt}',
                options => map('rewrite-all','true'))
        """)
    elif op == "expire":
        now = datetime.now(timezone.utc)  # noqa: UP017 — runner is Python 3.10.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        spark.sql(f"""
            CALL {cat}.system.expire_snapshots(
                table => '{NS}.{table}', older_than => TIMESTAMP '{now}', retain_last => 1)
        """)
    elif op == "append_expire":
        # new snapshot outside both footprints, then expire everything older → the
        # pending operation's base snapshot is removed
        _exec_op(spark, jv, case, "append", cat, EXPIRE_BKT, seq,
                 _trial_base(seq, EXPIRE_BKT), None)
        _exec_op(spark, jv, case, "expire", cat, bkt, seq, base, None)
    else:
        raise ValueError(f"Unknown op: {op}")


# ── single trial ─────────────────────────────────────────────────────────────

def _exc_class(text: str) -> str:
    m = re.search(r"((?:org\.apache\.iceberg\.exceptions|java\.lang)\.\w+)", text)
    return m.group(1) if m else ""


def _op_effect(op: str, entries: list[dict[str, Any]], base_id: int | None) -> str | None:
    """Effect check for one op over its proxy commit entries (first attempt for pending)."""
    if not entries:
        return "no commit seen by proxy"
    if op == "append_expire":
        if len(entries) != 2:
            return f"expected 2 commits, got {len(entries)}"
        r = commit_effect("append", entries[0].get("updates", [])) or \
            commit_effect("expire", entries[1].get("updates", []))
        if r:
            return r
        if base_id not in removed_snapshot_ids(entries[1].get("updates", [])):
            return "pending base snapshot not removed"
        return None
    return commit_effect(OP_KIND[op], entries[0].get("updates", []))


def _run_one_trial(spark: Any, jv: Jv, case: Case, seq: int, with_overlap: bool
                   ) -> dict[str, Any]:
    pending_bkt = 0
    committed_bkt = 0 if with_overlap else 1
    table = case.table_name
    full_p = f"{CAT_P}.{NS}.{table}"

    base_p = _insert_trial_data(spark, full_p, case, seq, pending_bkt)
    base_c = base_p if with_overlap else _insert_trial_data(spark, full_p, case, seq,
                                                            committed_bkt)
    tbl = jv.table(CAT_P, table)
    base_snap_id = jv.current_id(tbl)
    n_snap_before = len(jv.snapshot_ids(tbl))

    t_start = round(time.time() * 1000)
    r = requests.post(f"{ADV_URL}/v1/debug/hold",
                      json={"slot": PENDING_SLOT, "max_ms": 60000}, timeout=10)
    assert r.status_code == 200, f"hold register: {r.status_code}"

    out: dict[str, Any] = {"seq": seq, "overlap_trial": with_overlap, "base_snapshot": base_snap_id}

    def _pending() -> None:
        try:
            _exec_op(spark, jv, case, case.pending, CAT_P, pending_bkt, seq, base_p, base_snap_id)
            out["op_result"] = "success"
        except Exception as exc:
            text = str(exc)
            out["op_result"] = classify(text)
            out["exc_class"] = _exc_class(text)
            out["exc"] = text[:800]

    thr = threading.Thread(target=_pending, daemon=True)
    thr.start()

    held_entry: dict[str, Any] | None = None
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline and thr.is_alive():
        time.sleep(0.3)
        ents = _proxy_entries(PENDING_SLOT, table, t_start)
        if ents:
            held_entry = ents[0]
            break

    p_fp: dict[str, Any] | None = None
    c_fp: dict[str, Any] | None = None
    if held_entry is not None:
        # committed op while pending is held
        try:
            _exec_op(spark, jv, case, case.committed, CAT_C, committed_bkt, seq, base_c, None)
            out["committed_ok"] = True
        except Exception as exc:
            out["committed_ok"] = False
            out["committed_exc"] = str(exc)[:500]
        # footprints while the pending manifests still exist
        tbl = jv.table(CAT_P, table)
        snap = added_snapshot(held_entry.get("updates", []))
        if snap is not None:
            try:
                p_fp = jv.footprint(tbl, snap, "id")
            except Exception as exc:
                out["pending_fp_error"] = str(exc)[:300]
        c_entries = _proxy_entries(COMMITTED_SLOT, table, t_start)
        c_snaps = [s for e in c_entries if (s := added_snapshot(e.get("updates", [])))]
        if c_snaps:
            try:
                c_fp = jv.footprint(tbl, c_snaps[0], "id")
            except Exception as exc:
                out["committed_fp_error"] = str(exc)[:300]

    with contextlib.suppress(Exception):
        requests.delete(f"{ADV_URL}/v1/debug/hold/{PENDING_SLOT}", timeout=5)
    thr.join(timeout=300)
    if "op_result" not in out:
        out["op_result"] = "timeout"

    p_entries = _proxy_entries(PENDING_SLOT, table, t_start)
    c_entries = _proxy_entries(COMMITTED_SLOT, table, t_start)
    statuses = [e.get("status") for e in p_entries]
    out["outcome"] = trial_outcome(out["op_result"], statuses)
    if out["outcome"] == "unknown":
        out["outcome"] = f"error:{out.get('exc_class') or 'unknown'}"

    first_p = p_entries[0] if p_entries else {}
    out["pending_attempts"] = [{k: e.get(k) for k in ("ts", "ts_fwd", "ts_resp", "status",
                                                      "held", "hold_ms", "hold_timeout",
                                                      "requirements")} for e in p_entries]
    out["committed_commits"] = [{k: e.get(k) for k in ("ts", "ts_resp", "status",
                                                       "requirements")} for e in c_entries]
    out["pending_summary"] = (added_snapshot(first_p.get("updates", [])) or {}).get("summary")
    out["committed_summaries"] = [(added_snapshot(e.get("updates", [])) or {}).get("summary")
                                  for e in c_entries]
    out["pending_removed_snapshots"] = removed_snapshot_ids(first_p.get("updates", []))
    out["committed_removed_snapshots"] = [i for e in c_entries
                                          for i in removed_snapshot_ids(e.get("updates", []))]

    out["held"] = bool(first_p.get("held")) and not first_p.get("hold_timeout")
    out["in_window"] = in_hold_window(first_p, c_entries) if first_p else False
    out["pending_effect_err"] = _op_effect(case.pending, p_entries[:1], base_snap_id)
    out["committed_effect_err"] = _op_effect(case.committed, c_entries, base_snap_id)
    p_range = (base_p, base_p + DML_ROWS - 1)
    out["overlap"] = _overlaps(p_fp, c_fp, f"bkt={pending_bkt}", p_range)
    out["pending_fp"] = p_fp
    out["committed_fp"] = c_fp
    out["commit_log"] = _commit_log_lines(table, t_start)
    tbl = jv.table(CAT_P, table)
    out["snapshots_before"] = n_snap_before
    out["snapshots_after"] = len(jv.snapshot_ids(tbl))

    reasons: list[str] = []
    if not out["held"]:
        reasons.append("pending not held")
    if not out["in_window"]:
        reasons.append("committed not inside hold window")
    if out["pending_effect_err"]:
        reasons.append(f"pending: {out['pending_effect_err']}")
    if out["committed_effect_err"]:
        reasons.append(f"committed: {out['committed_effect_err']}")
    if with_overlap and case.overlap != "none" and not out["overlap"][case.overlap]:
        reasons.append(f"no {case.overlap} overlap")
    if not with_overlap and any(out["overlap"].values()):
        reasons.append(f"control has overlap {out['overlap']}")
    out["invalid_reasons"] = reasons
    out["valid"] = not reasons
    return out


# ── test functions ───────────────────────────────────────────────────────────

_all_results: dict[str, Any] = {}


def _load_results() -> None:
    if not _all_results and os.path.exists(RESULTS_FILE) and os.environ.get("MATRIX_APPEND"):
        with open(RESULTS_FILE) as fh:
            _all_results.update(json.load(fh))


def _save(case_id: str, entry: dict[str, Any]) -> None:
    _load_results()
    _all_results[case_id] = entry
    os.makedirs(OUT_DIR, exist_ok=True)
    with open(RESULTS_FILE, "w") as fh:
        json.dump(_all_results, fh, indent=1)


def _run_case(spark: Any, case: Case, setup: Any, trial: Any) -> dict[str, Any]:
    setup(spark, case)
    jv = Jv(spark)
    trials: list[dict[str, Any]] = []
    for seq in range(TRIALS_OVERLAP):
        trials.append(trial(spark, jv, case, seq, True))
    for seq in range(TRIALS_CONTROL):
        trials.append(trial(spark, jv, case, 1000 + seq, False))

    ov = [t for t in trials if t["overlap_trial"]]
    ctl = [t for t in trials if not t["overlap_trial"]]
    ov_valid = [t for t in ov if t["valid"]]
    overlap_counts = dict(Counter(t["outcome"] for t in ov_valid))
    valid_n = len(ov_valid)
    match_rate = overlap_counts.get(case.expected, 0) / max(valid_n, 1)
    entry = {
        "pending": case.pending, "committed": case.committed, "isolation": case.isolation,
        "format_version": case.fmt_ver, "overlap": case.overlap, "variant": case.variant,
        "expected": case.expected, "control_expected_k2": case.control_expected_k2,
        "overlap_counts": overlap_counts,
        "overlap_counts_all": dict(Counter(t["outcome"] for t in ov)),
        "invalid_reasons": dict(Counter(r for t in ov for r in t["invalid_reasons"])),
        "control_counts": dict(Counter(t["outcome"] for t in ctl)),
        "control_valid": sum(t["valid"] for t in ctl),
        "trials_overlap": TRIALS_OVERLAP, "trials_control": TRIALS_CONTROL,
        "valid_trials": valid_n, "match_rate": round(match_rate, 3),
        "matches": valid_n >= MIN_VALID and match_rate >= 0.95,
        "client": "1.12.0", "catalog_server": "b0df3ca", "spark_master": SPARK_MASTER,
        "trials": trials,
    }
    _save(case.case_id, entry)
    return entry


def _assert_case(case: Case, e: dict[str, Any]) -> None:
    assert e["valid_trials"] >= MIN_VALID, (
        f"{case.case_id}: only {e['valid_trials']}/{TRIALS_OVERLAP} valid trials; "
        f"reasons {e['invalid_reasons']}")
    assert e["match_rate"] >= 0.95, (
        f"{case.case_id}: expected {case.expected} in ≥95% of valid overlap trials, "
        f"got {e['overlap_counts']} (valid={e['valid_trials']})")
    k2_ctl = e["control_counts"].get("K2", 0)
    if case.control_expected_k2:
        assert k2_ctl == TRIALS_CONTROL, f"{case.case_id}: control {e['control_counts']}"
    else:
        assert k2_ctl == 0, f"{case.case_id}: K2 in control without overlap: {e['control_counts']}"
    assert e["control_valid"] >= max(TRIALS_CONTROL - 1, 1), (
        f"{case.case_id}: control valid {e['control_valid']}/{TRIALS_CONTROL}")


@pytest.mark.parametrize("case", CASES, ids=lambda c: c.case_id)
def test_pair(spark: Any, case: Case) -> None:
    e = _run_case(spark, case, _setup_table, _run_one_trial)
    _assert_case(case, e)


# ── scenario pairs (config/workloads S2-S5) ──────────────────────────────────
# Table: schema and partition spec of config/workloads/_base.yaml (dt x bucket(8, key)).
# Pending: the scenario's controlled op; committed: append of late events.
# Overlap trial: append into the pending op's partitions and keys; control: another day,
# keys outside the pending range.

SC_DAY, SC_OTHER_DAY = "2026-11-30", "2026-11-20"
SC_ROWS, SC_KEYS = 200, 50

_SC_DDL = """
    CREATE TABLE {full} (
        event_id BIGINT NOT NULL, key BIGINT NOT NULL, dt DATE NOT NULL, event_ts TIMESTAMP,
        amount DOUBLE, status STRING, version INT, payload STRING)
    USING iceberg PARTITIONED BY (dt, bucket(8, key))
    TBLPROPERTIES ({props})
"""


@dataclass
class ScenarioCase:
    scenario:  str
    pending:   str          # merge_upsert | merge_upsert_unscoped | update_status | ...
    mode:      str          # copy-on-write | merge-on-read
    fmt_ver:   int
    expected:  str
    control_expected_k2: bool = False
    note:      str = ""
    committed: str = "append"
    isolation: str = "serializable"
    overlap:   str = "partition"
    variant:   str = ""
    trials_extra: dict[str, Any] = field(default_factory=dict)

    @property
    def case_id(self) -> str:
        return f"sc_{self.scenario}_{self.pending}_x_append_v{self.fmt_ver}"

    @property
    def table_name(self) -> str:
        return self.case_id


SC_CASES = [c for c in [
    ScenarioCase("S2", "merge_upsert", "copy-on-write", 2, "K2",
                 note="MERGE ON key AND dt (target-side dt predicate) x append"),
    ScenarioCase("S2", "merge_upsert_unscoped", "copy-on-write", 2, "K2",
                 control_expected_k2=True,
                 note="MERGE ON key only: no target filter in the scan → conflictDetectionFilter "
                      "= true (SparkWrite.java:445-455) → K2 for any concurrent append"),
    ScenarioCase("S3", "update_status", "merge-on-read", 2, "K2"),
    ScenarioCase("S3", "update_status", "merge-on-read", 3, "K2"),
    ScenarioCase("S5", "delete_by_keys", "copy-on-write", 2, "K2"),
    ScenarioCase("S2", "compaction_where", "copy-on-write", 2, "K1"),
    ScenarioCase("S3", "compaction_where", "merge-on-read", 3, "K1"),
] if ONLY in c.case_id and c.fmt_ver in FORMAT_VERSIONS]


def _sc_setup(spark: Any, case: ScenarioCase) -> None:
    full = f"{CAT_P}.{NS}.{case.table_name}"
    spark.sql(f"DROP TABLE IF EXISTS {full} PURGE")
    with open("/workspace/config/workloads/_base.yaml") as fh:
        base_props: dict[str, str] = dict(yaml.safe_load(fh)["table"]["properties"])
    base_props.update({"format-version": str(case.fmt_ver), "write.delete.mode": case.mode,
                       "write.update.mode": case.mode, "write.merge.mode": case.mode})
    props = ", ".join(f"'{k}' = '{v}'" for k, v in base_props.items())
    spark.sql(_SC_DDL.format(full=full, props=props))


def _sc_rows(first_key: int, n: int, day: str, tag: str) -> str:
    return f"""
        SELECT CAST({first_key} + id AS BIGINT) * 10 AS event_id,
               CAST({first_key} + id AS BIGINT) AS key, DATE '{day}' AS dt,
               TIMESTAMP '{day} 12:00:00' AS event_ts, CAST(id AS DOUBLE) AS amount,
               '{tag}' AS status, 1 AS version, REPEAT('x', 200) AS payload
        FROM RANGE({n})
    """


def _sc_exec(spark: Any, case: ScenarioCase, op: str, cat: str, k0: int, day: str,
             seq: int) -> None:
    full = f"{cat}.{NS}.{case.table_name}"
    keys = f"key BETWEEN {k0} AND {k0 + SC_KEYS - 1}"
    if op == "append":
        spark.sql(f"INSERT INTO {full} {_sc_rows(k0, 20, day, f'late-{seq}')}")
    elif op in ("merge_upsert", "merge_upsert_unscoped"):
        scope = f" AND t.dt = DATE '{day}'" if op == "merge_upsert" else ""
        spark.sql(f"""
            MERGE INTO {full} t
            USING ({_sc_rows(k0, SC_KEYS, day, f'cdc-{seq}')}) s
            ON t.key = s.key AND t.dt = s.dt{scope}
            WHEN MATCHED THEN UPDATE SET status = s.status, version = t.version + 1
            WHEN NOT MATCHED THEN INSERT *
        """)
    elif op == "update_status":
        spark.sql(f"UPDATE {full} SET status = 'upd-{seq}', version = version + 1 "
                  f"WHERE dt = DATE '{day}' AND {keys}")
    elif op == "delete_by_keys":
        key_list = ", ".join(str(k0 + i) for i in range(0, SC_KEYS, 5))
        spark.sql(f"DELETE FROM {full} WHERE key IN ({key_list})")
    elif op == "compaction_where":
        spark.sql(f"""
            CALL {cat}.system.rewrite_data_files(
                table => '{NS}.{case.table_name}', where => "dt = '{day}'",
                options => map('min-input-files','1'))
        """)
    else:
        raise ValueError(op)


def _sc_trial(spark: Any, jv: Jv, case: ScenarioCase, seq: int, with_overlap: bool
              ) -> dict[str, Any]:
    """Same protocol as _run_one_trial on the scenario table."""
    table = case.table_name
    full_p = f"{CAT_P}.{NS}.{table}"
    k0 = seq * TRIAL_STRIDE
    for i in range(4):   # 4 inserts → several small files per (dt, bucket)
        spark.sql(f"INSERT INTO {full_p} "
                  f"{_sc_rows(k0 + i * (SC_ROWS // 4), SC_ROWS // 4, SC_DAY, f'seed-{seq}')}")
    c_k0, c_day = (k0, SC_DAY) if with_overlap else (k0 + 500_000, SC_OTHER_DAY)
    if not with_overlap:
        spark.sql(f"INSERT INTO {full_p} {_sc_rows(c_k0, SC_ROWS // 4, c_day, f'seed-{seq}')}")

    t_start = round(time.time() * 1000)
    r = requests.post(f"{ADV_URL}/v1/debug/hold",
                      json={"slot": PENDING_SLOT, "max_ms": 60000}, timeout=10)
    assert r.status_code == 200
    out: dict[str, Any] = {"seq": seq, "overlap_trial": with_overlap}

    def _pending() -> None:
        try:
            _sc_exec(spark, case, case.pending, CAT_P, k0, SC_DAY, seq)
            out["op_result"] = "success"
        except Exception as exc:
            text = str(exc)
            out["op_result"] = classify(text)
            out["exc_class"] = _exc_class(text)
            out["exc"] = text[:800]

    thr = threading.Thread(target=_pending, daemon=True)
    thr.start()
    held_entry: dict[str, Any] | None = None
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline and thr.is_alive():
        time.sleep(0.3)
        ents = _proxy_entries(PENDING_SLOT, table, t_start)
        if ents:
            held_entry = ents[0]
            break
    p_fp = c_fp = None
    if held_entry is not None:
        try:
            _sc_exec(spark, case, case.committed, CAT_C, c_k0, c_day, seq)
            out["committed_ok"] = True
        except Exception as exc:
            out["committed_ok"] = False
            out["committed_exc"] = str(exc)[:500]
        tbl = jv.table(CAT_P, table)
        snap = added_snapshot(held_entry.get("updates", []))
        if snap is not None:
            p_fp = jv.footprint(tbl, snap, "key")
        c_snaps = [s for e in _proxy_entries(COMMITTED_SLOT, table, t_start)
                   if (s := added_snapshot(e.get("updates", [])))]
        if c_snaps:
            c_fp = jv.footprint(tbl, c_snaps[0], "key")
    with contextlib.suppress(Exception):
        requests.delete(f"{ADV_URL}/v1/debug/hold/{PENDING_SLOT}", timeout=5)
    thr.join(timeout=300)
    out.setdefault("op_result", "timeout")

    p_entries = _proxy_entries(PENDING_SLOT, table, t_start)
    c_entries = _proxy_entries(COMMITTED_SLOT, table, t_start)
    out["outcome"] = trial_outcome(out["op_result"], [e.get("status") for e in p_entries])
    if out["outcome"] == "unknown":
        out["outcome"] = f"error:{out.get('exc_class') or 'unknown'}"
    first_p = p_entries[0] if p_entries else {}
    out["pending_summary"] = (added_snapshot(first_p.get("updates", [])) or {}).get("summary")
    out["committed_summaries"] = [(added_snapshot(e.get("updates", [])) or {}).get("summary")
                                  for e in c_entries]
    out["held"] = bool(first_p.get("held")) and not first_p.get("hold_timeout")
    out["in_window"] = in_hold_window(first_p, c_entries) if first_p else False
    out["pending_effect_err"] = _op_effect(case.pending, p_entries[:1], None)
    out["committed_effect_err"] = _op_effect(case.committed, c_entries, None)
    part_ov = bool(p_fp and c_fp and set(p_fp["partitions"]) & set(c_fp["partitions"]))
    out["overlap"] = {"partition": part_ov}
    out["pending_fp"] = p_fp
    out["committed_fp"] = c_fp
    out["commit_log"] = _commit_log_lines(table, t_start)
    pl = [c for c in out["commit_log"] if c["catalog"] == CAT_P]
    out["pending_filter"] = pl[0]["filter"] if pl else None

    reasons: list[str] = []
    if not out["held"]:
        reasons.append("pending not held")
    if not out["in_window"]:
        reasons.append("committed not inside hold window")
    if out["pending_effect_err"]:
        reasons.append(f"pending: {out['pending_effect_err']}")
    if out["committed_effect_err"]:
        reasons.append(f"committed: {out['committed_effect_err']}")
    if with_overlap and not part_ov:
        reasons.append("no partition overlap")
    if not with_overlap and part_ov:
        reasons.append("control has partition overlap")
    out["invalid_reasons"] = reasons
    out["valid"] = not reasons
    return out


@pytest.mark.parametrize("case", SC_CASES, ids=lambda c: c.case_id)
def test_scenario_pair(spark: Any, case: ScenarioCase) -> None:
    e = _run_case(spark, case, _sc_setup, _sc_trial)  # type: ignore[arg-type]
    e["scenario"] = case.scenario
    e["note"] = case.note
    e["pending_filters"] = sorted({t.get("pending_filter") or "" for t in e["trials"]})
    _save(case.case_id, e)
    _assert_case(case, e)  # type: ignore[arg-type]
