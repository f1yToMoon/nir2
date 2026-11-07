"""Stage 3 integration checks on the stand (runner container).

  make test-int K=stage3     (pytest -k stage3 inside the runner container)

* test_stage3_insert_overwrite_dynamic_with — SQL `INSERT OVERWRITE t WITH ('isolation-level',
  'validate-from-snapshot-id') SELECT …` in the dynamic mode (the form runner uses for S4
  reprocess_day, tz/05 §2.2): an append to the overwritten partition after the planning snapshot
  gives K2, an append to another partition does not (carried over from stage 2, §8).
* test_stage3_invariants_detect_injected_faults — the invariant checks of the last finished run
  can fail (REVIEWER.md §4, stage 3): each injected fault is detected by its check, the table is
  restored afterwards, and the clean state passes.
"""
from __future__ import annotations

import os
import pickle
import time
from pathlib import Path
from typing import Any

import pytest

from runner.env import REPO, Env
from runner.invariants import Invariants
from runner.ops.errors import classify
from runner.ops.executor import load_table_state
from runner.slots import build_session

RUNS = REPO / "data" / "runs"


def _last_ledger() -> tuple[str, dict[str, Any]] | None:
    run = os.environ.get("STAGE3_RUN")
    cands = [RUNS / run / "ledger.pkl"] if run else sorted(
        RUNS.glob("*/ledger.pkl"), key=lambda p: p.stat().st_mtime, reverse=True)
    for p in cands:
        if p.exists():
            with p.open("rb") as fh:
                return p.parent.name, pickle.load(fh)
    return None


@pytest.fixture(scope="module")
def spark() -> Any:
    found = _last_ledger()
    run_id = found[0] if found else f"stage3-it-{int(time.time())}"
    s = build_session(Env(), run_id, "stage3-it", n_slots=0)
    yield s
    s.stop()


def test_stage3_insert_overwrite_dynamic_with(spark: Any) -> None:
    t = "lake.stage3_it.iow"
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.stage3_it")
    spark.sql(f"DROP TABLE IF EXISTS {t} PURGE")
    spark.sql(f"CREATE TABLE {t} (id INT, d DATE, v STRING) USING iceberg PARTITIONED BY (d) "
              "TBLPROPERTIES ('format-version' = '2')")
    spark.sql(f"INSERT INTO {t} VALUES (1, DATE '2026-11-01', 'a'), (2, DATE '2026-11-02', 'b')")
    env = Env()
    outcomes: dict[str, list[str]] = {"same_partition": [], "other_partition": []}
    for trial in range(3):
        for case, day in (("same_partition", "2026-11-01"), ("other_partition", "2026-11-02")):
            planning = load_table_state(env, "stage3_it.iow").current_snapshot_id
            spark.sql(f"INSERT INTO {t} VALUES ({100 + trial}, DATE '{day}', 'late')")  # concurrent
            try:
                spark.sql(f"INSERT OVERWRITE {t} WITH ('isolation-level' = 'serializable', "
                          f"'validate-from-snapshot-id' = '{planning}') "
                          f"SELECT id, d, 'reprocessed' FROM {t} VERSION AS OF {planning} "
                          f"WHERE d = DATE '2026-11-01'")
                st = load_table_state(env, "stage3_it.iow")
                snap = next(s for s in st.metadata["snapshots"]
                            if s["snapshot-id"] == st.current_snapshot_id)
                summ = snap["summary"]
                assert summ["operation"] == "overwrite"
                assert summ.get("replace-partitions") == "true"      # dynamic overwrite
                assert summ.get("changed-partition-count") == "1"
                outcomes[case].append("success")
            except Exception as exc:
                outcomes[case].append(classify(str(exc)))
    assert outcomes == {"same_partition": ["K2"] * 3, "other_partition": ["success"] * 3}, outcomes
    spark.sql(f"DROP TABLE IF EXISTS {t} PURGE")


def test_stage3_invariants_detect_injected_faults(spark: Any) -> None:
    found = _last_ledger()
    if found is None:
        pytest.skip("no finished run with data/runs/*/ledger.pkl")
    run_id, saved = found
    env = Env()
    from runner.db import DB
    db = DB(env.pg_dsn, env.pg_schema)
    table = saved["table"]
    st = load_table_state(env, table)
    if st.current_snapshot_id != saved["current"]:
        pytest.skip(f"table was reset after run {run_id}")
    led, ops, checks = saved["ledger"], saved["ops"], saved["checks"]
    inv = Invariants(spark, db, run_id, table, saved["n_buckets"], saved["current"], env)

    def run_checks(ledger: Any = led) -> dict[str, Any]:
        inv.current = load_table_state(env, table).current_snapshot_id
        return inv.run(ledger, ops, checks)

    def restore() -> None:
        spark.sql(f"CALL lake.system.set_current_snapshot(table => '{table}', "
                  f"snapshot_id => {saved['current']})")

    clean = run_checks()
    assert clean["ok"], clean
    golden_id = int(spark.sql(f"SELECT min(event_id) AS e FROM lake.{table} WHERE event_id < 1000000000000 "
                              f"AND version = 0").collect()[0]["e"])
    stream_id = int(spark.sql(f"SELECT max(event_id) AS e FROM lake.{table} WHERE event_id >= "
                              f"1000000000000 AND event_id < 9000000000000").collect()[0]["e"])
    detected: dict[str, list[str]] = {}
    faults = {
        "duplicate_row": f"INSERT INTO lake.{table} SELECT * FROM lake.{table} WHERE event_id = {golden_id}",
        "lost_row": f"DELETE FROM lake.{table} WHERE event_id = {stream_id}",
        "extra_update": f"UPDATE lake.{table} SET version = version + 1 WHERE event_id = {golden_id}",
    }
    for name, sql in faults.items():
        spark.sql(sql)
        res = run_checks()
        detected[name] = sorted(k for k, v in res.items() if isinstance(v, dict) and not v.get("ok"))
        restore()
    # Restore a golden row actually removed by DELETE. The factual side must retain it,
    # while the independent expected ledger excludes its exact event id (not the whole key).
    deleted_golden = sorted(i for i in led.deleted_ids if i < 1_000_000_000_000)
    if led.deleted_ids:
        assert deleted_golden, "DELETE scenario must exercise nonzero golden deletions"
        deleted_id = deleted_golden[0]
        try:
            spark.sql(f"INSERT INTO lake.{table} SELECT * FROM lake.{table} "
                      f"VERSION AS OF {led.golden_snapshot_id} WHERE event_id = {deleted_id}")
            res = run_checks()
            detected["retained_deleted_row"] = sorted(
                k for k, v in res.items() if isinstance(v, dict) and not v.get("ok"))
            assert "ledger_row_counts" in detected["retained_deleted_row"], res
        finally:
            restore()
    # a row of an aborted operation in the table
    import copy
    led2 = copy.deepcopy(led)
    led2.aborted_inserted = [*led.aborted_inserted, 8_999_999_999_999]
    spark.sql(f"INSERT INTO lake.{table} SELECT 8999999999999, key, dt, event_ts, amount, status, "
              f"version, payload FROM lake.{table} WHERE event_id = {golden_id}")
    res = run_checks(led2)
    detected["aborted_row"] = sorted(k for k, v in res.items() if isinstance(v, dict) and not v.get("ok"))
    restore()
    # a committed operation's snapshot leaves the lineage
    committed = [o for o in ops if o.get("outcome") == "committed" and o.get("controlled")
                 and o.get("snapshot_id")]
    if committed:
        last = max(committed, key=lambda o: o["finished_at"])
        parent = st.snapshots.get(int(last["snapshot_id"]), {}).get("parent")
        if parent is not None:
            spark.sql(f"CALL lake.system.set_current_snapshot(table => '{table}', snapshot_id => {parent})")
            res = run_checks()
            detected["lineage"] = sorted(k for k, v in res.items()
                                         if isinstance(v, dict) and not v.get("ok"))
            restore()
    after = run_checks()
    out = Path(REPO / "results" / "stage-3" / "invariants_fault_injection.json")
    import json
    out.write_text(json.dumps({"run_id": run_id, "clean": clean["ok"], "detected": detected,
                               "after_restore": after["ok"]}, indent=1))
    assert "no_duplicate_event_ids" in detected["duplicate_row"]
    assert "ledger_row_counts" in detected["duplicate_row"]
    assert "ledger_row_counts" in detected["lost_row"]
    assert "version_monotonic" in detected["extra_update"]
    assert "aborted_ops_absent" in detected["aborted_row"]
    if "lineage" in detected:
        assert "committed_ops_in_lineage" in detected["lineage"]
    assert after["ok"], after
