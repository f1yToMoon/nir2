"""A matching append after the DML read must not become a ledger UPDATE/DELETE.

The commit hold forces read(DML) < commit(append) < commit(DML). Snapshot isolation
allows the DML; serializable is the rejecting control. Every repeat uses fresh data.
"""
from __future__ import annotations

import contextlib
import json
import os
import time
from types import SimpleNamespace
from typing import Any

import pytest
import requests

from runner.env import REPO, Env, git_sha
from runner.ops.errors import classify
from runner.ops.executor import OpExecutor, load_table_state
from runner.slots import build_session


@pytest.fixture(scope="module")
def spark() -> Any:
    session = build_session(Env(), f"effects-micro-{int(time.time())}", "stage3-effects-micro", n_slots=1)
    yield session
    session.stop()


@pytest.mark.parametrize("mode", ["copy-on-write", "merge-on-read"])
@pytest.mark.parametrize("statement", ["update_status", "delete_by_keys"])
@pytest.mark.parametrize("isolation", ["snapshot", "serializable"])
@pytest.mark.parametrize("planning_lag", [False, True])
def test_stage3_effects_ignore_unread_append(spark: Any, mode: str, statement: str, isolation: str,
                                           planning_lag: bool) -> None:
    from pyspark import InheritableThread

    env = Env()
    key = f"{mode.replace('-', '_')}_{statement}_{isolation}_lag{int(planning_lag)}"
    name = f"stage3_it.effects_{key}"
    table = f"lake.{name}"
    op_id = f"effects-micro-{key}-{time.time_ns()}"
    verb = "update" if statement == "update_status" else "delete"
    op_type = "cow_dml" if mode == "copy-on-write" else f"mor_{verb}"
    out: dict[str, Any] = {"mode": mode, "statement": statement, "isolation": isolation,
                           "git_sha": git_sha()}
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.stage3_it")
    spark.sql(f"DROP TABLE IF EXISTS {table} PURGE")
    spark.sql(f"CREATE TABLE {table} (event_id BIGINT, key BIGINT, dt DATE, version INT) USING iceberg "
              "PARTITIONED BY (dt, bucket(1, key)) TBLPROPERTIES ('format-version'='2', "
              f"'write.{verb}.mode'='{mode}', 'write.{verb}.isolation-level'='{isolation}')")
    spark.sql(f"INSERT INTO {table} VALUES (1,97,DATE '2026-11-01',0),(2,98,DATE '2026-11-01',0)")
    base = load_table_state(env, name).current_snapshot_id
    out["base_snapshot_id"] = base
    if planning_lag:
        spark.sql(f"INSERT INTO {table} VALUES (4,97,DATE '2026-11-01',0)")
    expected_read = load_table_state(env, name).current_snapshot_id
    out["expected_read_snapshot_id"] = expected_read
    assert (expected_read != base) == planning_lag
    sql = (f"UPDATE s0.{name} SET version=version+1 WHERE key=97" if verb == "update" else
           f"DELETE FROM s0.{name} WHERE key=97")

    def pending() -> None:
        try:
            frame = spark.sql(sql)
            frame.collect()
            out["physical_plan"] = frame._jdf.queryExecution().executedPlan().treeString()
            out["frame"] = frame
            out["outcome"] = "success"
        except Exception as exc:
            out["outcome"] = classify(str(exc))
            out["exception"] = str(exc)

    thread = InheritableThread(target=pending, daemon=True)
    try:
        requests.put(f"{env.advisor_uri}/v1/debug/slots/s0",
                     json={"op_id": op_id, "op_type": op_type}, timeout=10).raise_for_status()
        requests.post(f"{env.advisor_uri}/v1/debug/hold", json={"slot": "s0", "max_ms": 60000},
                      timeout=10).raise_for_status()
        thread.start()
        deadline = time.monotonic() + 120
        held = []
        while time.monotonic() < deadline and thread.is_alive():
            entries = requests.get(f"{env.advisor_uri}/_capl/debug/last-commits", timeout=10).json()
            held = [x for x in entries if x.get("op_id") == op_id]
            if held:
                break
            time.sleep(0.1)
        assert held and thread.is_alive(), out
        spark.sql(f"INSERT INTO {table} VALUES (3,97,DATE '2026-11-01',0)")
        appended = load_table_state(env, name).current_snapshot_id
        assert appended != base
        out["append_snapshot_id"] = appended
        requests.delete(f"{env.advisor_uri}/v1/debug/hold/s0", timeout=10).raise_for_status()
        thread.join(timeout=120)
        assert not thread.is_alive(), "DML did not finish after releasing hold"
        actual = {int(r['event_id']): int(r['version']) for r in
                  spark.sql(f"SELECT event_id,version FROM {table}").collect()}
        out["actual_versions"] = actual
        assert actual[3] == 0, out
        if isolation == "serializable":
            assert out["outcome"] == "K2", out
            assert actual == ({1: 0, 2: 0, 3: 0, 4: 0} if planning_lag else {1: 0, 2: 0, 3: 0}), out
        else:
            assert out["outcome"] == "success", out
            expected_versions = {1: 1, 2: 0, 3: 0} if verb == "update" else {2: 0, 3: 0}
            if planning_lag and verb == "update":
                expected_versions[4] = 1
            assert actual == expected_versions, out
            state = load_table_state(env, name)
            sid = state.current_snapshot_id
            assert state.snapshots[sid]["parent"] == appended
            out["committed_snapshot_id"] = sid
            out["snapshots"] = state.metadata["snapshots"]
            journal: dict[str, Any] = ({"deleted_keys": [97]} if verb == "delete" else
                                       {"update_rule": {"mod": 97, "r": 0}})
            from runner.read_snapshot import read_snapshot_id
            journal["read_snapshot_id"] = read_snapshot_id(out["frame"], f"s0.{name}")
            assert journal["read_snapshot_id"] == expected_read, journal
            with pytest.raises(ValueError, match="expected one actual read snapshot"):
                read_snapshot_id(out["frame"], f"lake.{name}")
            row = {"op_id": op_id, "outcome": "committed", "snapshot_id": sid,
                   "planning_snapshot_id": base, "footprint_requested": ["dt=2026-11-01/key_bucket=0"]}
            executor = OpExecutor.__new__(OpExecutor)
            executor.ctx = SimpleNamespace(spark=spark, env=env, table=name,
                                           sc=SimpleNamespace(table=SimpleNamespace(n_buckets=1)))
            executor.record_effects(SimpleNamespace(type=op_type, params={"statement": statement}),
                                    row, journal)
            out["journal"] = journal
            affected = ({r[0] for r in journal["deleted_rows"]} if verb == "delete" else
                        set(journal["effect_updated_ids"]))
            assert affected == ({1, 4} if planning_lag else {1}), out
    finally:
        with contextlib.suppress(Exception):
            requests.delete(f"{env.advisor_uri}/v1/debug/hold/s0", timeout=10)
        if thread.ident is not None:
            thread.join(timeout=120)
        with contextlib.suppress(Exception):
            requests.delete(f"{env.advisor_uri}/v1/debug/slots/s0", timeout=10)
        out.pop("frame", None)
        path = REPO / "results/stage-3" / os.environ.get("STAGE3_EFFECTS_NAME", "effects-snapshot")
        path.mkdir(parents=True, exist_ok=True)
        (path / f"{key}.json").write_text(json.dumps(out, default=str, indent=2))
        if not thread.is_alive():
            spark.sql(f"DROP TABLE IF EXISTS {table} PURGE")
