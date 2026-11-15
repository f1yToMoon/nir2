"""Distinguish eager refresh from catalog/session ownership, on the real cluster."""
from __future__ import annotations

import json
import os
import time
from dataclasses import replace
from typing import Any

import pytest

from runner.env import REPO, Env
from runner.ops.executor import OpExecutor, RunCtx, load_table_state
from runner.slots import build_session


@pytest.fixture(scope="module")
def spark() -> Any:
    session = build_session(Env(), f"sessions-micro-{time.time_ns()}",
                            "stage3-sessions-micro", n_slots=1)
    yield session
    session.stop()


@pytest.mark.parametrize("cached", [False, True])
@pytest.mark.parametrize("separate", [False, True])
def test_stage3_runtime_filter_ownership(spark: Any, cached: bool, separate: bool) -> None:
    """A scan must keep its read snapshot; another session must see fresh commits.

    cache=false reproduces even with separate sessions (eager refresh of the SAME
    SparkTable). cache=true alone still fails if another thread refreshes that
    session's cached Table. Only both ownership boundaries preserve the scan.
    """
    session = spark.newSession()
    session.conf.set("spark.sql.catalog.s0.cache-enabled", str(cached).lower())
    other = session.newSession() if separate else session
    name = f"stage3_it.sessions_{int(cached)}_{int(separate)}"
    table = f"s0.{name}"
    jvm, gateway = spark._jvm, spark.sparkContext._gateway
    options = jvm.org.apache.spark.sql.util.CaseInsensitiveStringMap(
        jvm.java.util.HashMap())
    namespace = gateway.new_array(jvm.java.lang.String, 1)
    namespace[0] = "stage3_it"
    identifier = jvm.org.apache.spark.sql.connector.catalog.Identifier.of(
        namespace, name.split(".")[1])
    out: dict[str, Any] = {"cached": cached, "separate": separate}
    session.sql("CREATE NAMESPACE IF NOT EXISTS s0.stage3_it")
    session.sql(f"CREATE TABLE {table} (id BIGINT) USING iceberg")
    try:
        session.sql(f"INSERT INTO {table} VALUES (1)")
        base = load_table_state(Env(), name).current_snapshot_id
        catalog = session._jsparkSession.sessionState().catalogManager().catalog("s0")
        target = catalog.loadTable(identifier)
        scan = target.newScanBuilder(options).buildCopyOnWriteScan()
        # External writer, then another query's metadata loading in this or a
        # private session. No table/client source is changed.
        other.sql(f"INSERT INTO {table} VALUES (2)")
        current = load_table_state(Env(), name).current_snapshot_id
        assert current != base
        other.catalog.refreshTable(table)
        assert other.sql(f"SELECT count(*) FROM {table}").first()[0] == 2
        # The uncached SparkTable refreshes eagerly even when another session
        # owns the intervening query.
        target.newScanBuilder(options)
        predicates = gateway.new_array(
            jvm.org.apache.spark.sql.connector.expressions.filter.Predicate, 0)
        types = gateway.new_array(jvm.java.lang.Class, 1)
        types[0] = jvm.java.lang.Class.forName(
            "[Lorg.apache.spark.sql.connector.expressions.filter.Predicate;")
        method = scan.getClass().getMethod("filter", types)
        method.setAccessible(True)
        args = gateway.new_array(jvm.java.lang.Object, 1)
        args[0] = predicates
        try:
            method.invoke(scan, args)
            out["filter"] = "stable"
        except Exception as exc:
            out["filter"] = str(exc)
        out.update(base=base, current=current)
        if cached and separate:
            assert out["filter"] == "stable", out
        else:
            assert "Runtime file filtering is not possible" in out["filter"], out
        session.createDataFrame([(1,)], "id long").createOrReplaceTempView("private_view")
        if separate:
            assert not other.catalog.tableExists("private_view")
            assert session.sparkContext is other.sparkContext
    finally:
        path = REPO / "results/stage-3" / os.environ.get("STAGE3_SESSIONS_NAME", "sessions-micro")
        path.mkdir(parents=True, exist_ok=True)
        (path / f"cache{int(cached)}_separate{int(separate)}.json").write_text(
            json.dumps(out, indent=2))
        session.sql(f"DROP TABLE {table} PURGE")


def test_stage3_executor_session_ownership(spark: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """Exercise the real execute wrapper twice, rejecting session/temp-view reuse."""
    from types import SimpleNamespace

    ctx = RunCtx(spark, Env(), None, None, "micro", None, None, None, None)
    executor = OpExecutor(ctx, None, None)
    seen = []

    def inspect(child: OpExecutor, *args: Any, **kwargs: Any) -> Any:
        session = child.ctx.spark
        assert session is not spark
        assert session.sparkContext is spark.sparkContext
        assert not session.catalog.tableExists("owned_by_operation")
        session.range(1).createOrReplaceTempView("owned_by_operation")
        # No shared context mutation; all other run resources retain ownership.
        assert replace(child.ctx, spark=spark) == ctx
        seen.append(session)
        return SimpleNamespace(row={}, journal={})

    monkeypatch.setattr(OpExecutor, "_execute", inspect)
    for _ in range(2):
        executor.execute(None, None, 0, controlled=True, writer_class="controlled",
                         intent_at=None, pool="dml")
    assert seen[0] is not seen[1]
    assert executor.ctx is ctx
    assert not spark.catalog.tableExists("owned_by_operation")
