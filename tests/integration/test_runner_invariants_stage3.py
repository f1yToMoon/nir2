"""Nonzero DELETE invariant control independent of random workload outcomes."""
from __future__ import annotations

import contextlib
import json
import os
import time
from typing import Any

from runner.db import DB
from runner.env import REPO, Env, git_sha
from runner.invariants import Invariants, Ledger
from runner.ops.executor import load_table_state
from runner.slots import build_session


def test_stage3_retained_deleted_row_nonzero() -> None:
    """A retained deleted row must fail, even if every S5 DELETE encountered K2.

    All commits have a separate run tag and table. In the injected state the
    proxy's physical record count is correct, so it cannot detect the fault on
    behalf of the independent expected-vs-factual row comparison.
    """
    stamp = time.time_ns()
    rid = f"stage3-retained-{stamp}"
    name = f"stage3_it.retained_{stamp}"
    table = f"lake.{name}"
    output = REPO / "results/stage-3" / os.environ.get("STAGE3_RETAINED_NAME", rid)
    output.mkdir(parents=True, exist_ok=False)
    env = Env()
    db = DB(env.pg_dsn, env.pg_schema)
    spark = build_session(env, rid, rid, n_slots=0)
    out: dict[str, Any] = {"run_id": rid, "table": name, "git_sha": git_sha(),
                           "case": "retained_deleted_row", "standalone_control": True}
    checks = ["ledger_row_counts", "no_duplicate_event_ids", "committed_ops_in_lineage",
              "aborted_ops_absent", "version_monotonic"]

    def wait_for_snapshot(sid: int) -> None:
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if db.query("SELECT snapshot_id FROM {s}.commit_events WHERE run_id=%s "
                        "AND table_name=%s AND snapshot_id=%s", [rid, name, sid]):
                return
            time.sleep(0.1)
        raise AssertionError(f"snapshot {sid} missing from actual proxy journal")

    try:
        spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.stage3_it")
        spark.sql(f"CREATE TABLE {table} (event_id BIGINT,key BIGINT,dt DATE,version INT) "
                  "USING iceberg PARTITIONED BY (dt,bucket(1,key)) TBLPROPERTIES "
                  "('format-version'='2','write.delete.mode'='copy-on-write')")
        spark.sql(f"INSERT INTO {table} VALUES "
                  "(1,97,DATE '2026-11-01',0),(2,98,DATE '2026-11-01',0)")
        golden = load_table_state(env, name).current_snapshot_id
        assert golden is not None
        spark.sql(f"DELETE FROM {table} WHERE event_id=1")
        deleted = load_table_state(env, name)
        sid = deleted.current_snapshot_id
        assert sid is not None and sid != golden
        wait_for_snapshot(sid)
        out["golden_snapshot_id"], out["delete_snapshot_id"] = golden, sid
        out["delete_metadata"] = deleted.metadata
        assert [r[0] for r in spark.sql(f"SELECT event_id FROM {table}").collect()] == [2]
        ledger = Ledger(golden_snapshot_id=golden, deleted_ids={1})
        ops = [{"op_id": f"{rid}-delete", "op_type": "cow_dml", "outcome": "committed",
                "controlled": True, "snapshot_id": sid}]
        inv = Invariants(spark, db, rid, name, 1, sid, env)
        out["clean"] = inv.run(ledger, ops, checks)
        assert out["clean"]["ok"], out["clean"]
        assert out["clean"]["ledger_row_counts"]["table_live_rows"] == 1
        assert out["clean"]["committed_ops_in_lineage"]["committed_checked"] == 1
        try:
            spark.sql(f"INSERT INTO {table} SELECT * FROM {table} "
                      f"VERSION AS OF {golden} WHERE event_id=1")
            injected = load_table_state(env, name)
            inv.current = injected.current_snapshot_id
            wait_for_snapshot(inv.current)
            out["injected_metadata"] = injected.metadata
            out["injected"] = inv.run(ledger, ops, checks)
            actual = out["injected"]["ledger_row_counts"]
            assert not out["injected"]["ok"] and not actual["ok"], out["injected"]
            assert actual["n_mismatches"] == 1, actual
            assert sum(v["actual"] - v["expected"] for v in actual["mismatches"].values()) == 1
            assert actual["table_live_rows"] == 2
            assert actual["journal_physical_records"] == actual["snapshot_total_records"] == 2
            assert out["injected"]["no_duplicate_event_ids"]["ok"]
        finally:
            spark.sql(f"CALL lake.system.set_current_snapshot(table => '{name}', "
                      f"snapshot_id => {sid})")
        inv.current = load_table_state(env, name).current_snapshot_id
        assert inv.current == sid
        out["after_restore"] = inv.run(ledger, ops, checks)
        assert out["after_restore"]["ok"], out["after_restore"]
        out["commit_attempts"] = db.query(
            "SELECT * FROM {s}.commit_attempts WHERE run_id=%s AND table_name=%s", [rid, name])
    finally:
        (output / "retained_deleted_fixture.json").write_text(json.dumps(out, default=str, indent=2))
        with contextlib.suppress(Exception):
            spark.sql(f"DROP TABLE IF EXISTS {table} PURGE")
        spark.stop()
        db.close()
