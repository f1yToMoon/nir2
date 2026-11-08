"""Runner CLI (FR-RUN-1…8, tz/05 §4, tz/06 steps 3.2, 3.8).

    python -m runner.main seed --scenario S2_compaction_vs_cdc_merge [--variant v] [--size lite]
    python -m runner.main run --scenario S0_baseline --policy P0 --concurrency mid --seed 1
    python -m runner.main plan --scenario S0_baseline --seed 1        # intents only (no stand)

Golden table: one data set per size (`data/golden/_data[_lite].json`) with a single snapshot
tagged `golden`; per scenario/variant a metadata.json with the scenario's properties over the
same snapshot. The tag keeps the golden files: expire never removes a snapshot that a reference
retains, nor its files (ExpireSnapshotsSparkAction.fileDS over the retained snapshots), ADR-009.
Before a run: files of earlier runs are removed (everything under the table location that the
golden versions do not reference), the table is dropped without purge and the scenario's golden
metadata.json is registered.
"""
from __future__ import annotations

import argparse
import contextlib
import json
import sys
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests

from runner import scenario as scn
from runner.cost import CostReader
from runner.db import DB
from runner.env import REPO, Env, git_sha
from runner.health import HealthSampler, _aws
from runner.intents import Intent, generate
from runner.invariants import Invariants, Ledger, orphan_scan
from runner.ops.executor import OpExecutor, RunCtx, load_table_state, now
from runner.resume import RESULTS_DIR, require_valid_result
from runner.slots import SlotPool, build_session, job_scope

UTC = timezone.utc  # noqa: UP017 — runner image is Python 3.10
GOLDEN_DIR = REPO / "data" / "golden"
RUNS_DIR = REPO / "data" / "runs"


def log(msg: str, **kw: Any) -> None:
    extra = " ".join(f"{k}={v}" for k, v in kw.items())
    print(f"[{datetime.now(UTC):%H:%M:%S}] {msg} {extra}", flush=True)


# ── golden table ─────────────────────────────────────────────────────────────

def golden_name(sc: scn.Scenario) -> str:
    v = f"__{sc.variant}" if sc.variant else ""
    size = "__lite" if sc.profile.size == "lite" else ""
    return f"{sc.name}{v}{size}"


def data_golden_path(size: str) -> Path:
    return GOLDEN_DIR / ("_data.json" if size == "full" else "_data_lite.json")


def _fs(env: Env) -> Any:
    import pyarrow.fs as pafs
    return pafs.S3FileSystem(endpoint_override=env.s3_endpoint, scheme="http",
                             access_key=_aws("AWS_ACCESS_KEY_ID"),
                             secret_key=_aws("AWS_SECRET_ACCESS_KEY"),
                             region=_aws("AWS_REGION", "us-east-1"))


def drop_table(env: Env, table: str) -> None:
    """DROP without purge through the REST API: unlike Spark DROP TABLE it does not load the
    table, so it works when files of the previous run are already gone."""
    ns, name = table.split(".", 1)
    r = requests.delete(f"{env.catalog_uri}/v1/namespaces/{ns}/tables/{name}",
                        params={"purgeRequested": "false"}, timeout=30)
    if r.status_code not in (200, 204, 404):
        r.raise_for_status()


def reset_table(spark: Any, env: Env, table: str, metadata_location: str) -> None:
    ns = table.split(".")[0]
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS lake.{ns}")
    drop_table(env, table)                                       # without purge: files stay
    spark.sql(f"CALL lake.system.register_table(table => '{table}', "
              f"metadata_file => '{metadata_location}')")


def golden_keep_set(spark: Any, table: str) -> list[str]:
    """Every file the golden snapshot references (data, manifests, manifest list)."""
    keep: set[str] = set()
    for q in (f"SELECT file_path AS p FROM lake.{table}.all_files",
              f"SELECT path AS p FROM lake.{table}.all_manifests",
              f"SELECT manifest_list AS p FROM lake.{table}.snapshots"):
        keep |= {str(r["p"]) for r in spark.sql(q).collect()}
    return sorted(keep)


def gc_table_files(env: Env, location: str, keep: set[str]) -> dict[str, int]:
    """Delete every object under the table location that is not in `keep` (between runs)."""
    import pyarrow.fs as pafs
    fs = _fs(env)
    root = location.split("://", 1)[1].rstrip("/")
    infos = fs.get_file_info(pafs.FileSelector(root, recursive=True, allow_not_found=True))
    keep_k = {k.split("://", 1)[1] for k in keep}
    victims = [i.path for i in infos if i.type == pafs.FileType.File and i.path not in keep_k]
    def _rm(path: str) -> None:
        with contextlib.suppress(Exception):
            fs.delete_file(path)

    with ThreadPoolExecutor(16) as ex:
        list(ex.map(_rm, victims))
    return {"listed": len(infos), "deleted": len(victims), "kept": len(keep_k)}


def seed(args: argparse.Namespace) -> int:
    env = Env()
    sc = scn.load(args.scenario, variant=args.variant, size=args.size)
    table = sc.table.name
    GOLDEN_DIR.mkdir(parents=True, exist_ok=True)
    spark = build_session(env, "seed", "capl-seed", n_slots=0)
    dpath = data_golden_path(sc.profile.size)
    try:
        if args.force_data or not dpath.exists():
            base = scn.Scenario.model_validate(scn.load_raw(scn.WORKLOADS_DIR / "_base.yaml")
                                               | {"name": "_base"})
            base.seed_data = sc.seed_data          # lite: scaled by scenario.load(size=...)
            from runner.datagen import ddl, golden_select, seed_batches
            spark.sql(f"CREATE NAMESPACE IF NOT EXISTS lake.{table.split('.')[0]}")
            spark.sql(f"DROP TABLE IF EXISTS lake.{table} PURGE")
            spark.sql(ddl(base, f"lake.{table}"))
            t = time.monotonic()
            for j, (lo, hi) in enumerate(seed_batches(base)):
                with job_scope(spark, f"seed-{j}", "seed", "admin"):
                    spark.sql(f"INSERT INTO lake.{table} {golden_select(base, lo, hi, base.seed)}")
                log("seed batch", j=j, rows=hi - lo, s=round(time.monotonic() - t))
            spark.sql(f"CALL lake.system.expire_snapshots(table => '{table}', older_than => "
                      f"TIMESTAMP '{datetime.now(UTC) + timedelta(seconds=1):%Y-%m-%d %H:%M:%S}', "
                      f"retain_last => 1)")
            spark.sql(f"ALTER TABLE lake.{table} CREATE TAG golden")
            st = load_table_state(env, table)
            parts = {r["p"]: int(r["n"]) for r in spark.sql(
                f"SELECT concat('dt=', CAST(dt AS STRING), '/key_bucket=', "
                f"CAST(system.bucket({base.table.n_buckets}, key) AS STRING)) AS p, count(*) AS n "
                f"FROM lake.{table} GROUP BY 1").collect()}
            files = spark.sql(f"SELECT count(*) AS n, sum(file_size_in_bytes) AS b, "
                              f"min(file_size_in_bytes) AS mn, max(file_size_in_bytes) AS mx "
                              f"FROM lake.{table}.files").collect()[0]
            keep = golden_keep_set(spark, table)
            dpath.with_suffix(".keep.txt").write_text("\n".join([*keep, st.metadata_location]) + "\n")
            dpath.write_text(json.dumps({
                "metadata_location": st.metadata_location, "snapshot_id": st.current_snapshot_id,
                "location": st.metadata.get("location"), "rows": sum(parts.values()),
                "partitions": len(parts), "rows_by_partition": parts,
                "files": int(files["n"]), "bytes": int(files["b"]), "file_bytes_min": int(files["mn"]),
                "file_bytes_max": int(files["mx"]), "seed": base.seed, "size": sc.profile.size,
                "git_sha": git_sha(), "created_at": datetime.now(UTC).isoformat(),
                "seconds": round(time.monotonic() - t)}, indent=1))
            log("golden data", rows=sum(parts.values()), files=int(files["n"]))
        dg = json.loads(dpath.read_text())
        reset_table(spark, env, table, dg["metadata_location"])
        props = {"format-version": str(sc.table.format_version), **sc.table.properties}
        tp = ", ".join(f"'{k}' = '{v}'" for k, v in props.items())
        spark.sql(f"ALTER TABLE lake.{table} SET TBLPROPERTIES ({tp})")
        st = load_table_state(env, table)
        assert st.current_snapshot_id == dg["snapshot_id"], "golden snapshot changed"
        with dpath.with_suffix(".keep.txt").open("a") as fh:
            fh.write(st.metadata_location + "\n")
        out = GOLDEN_DIR / f"{golden_name(sc)}.json"
        out.write_text(json.dumps({"metadata_location": st.metadata_location,
                                   "snapshot_id": st.current_snapshot_id, "data": dpath.name,
                                   "properties": props, "git_sha": git_sha(),
                                   "created_at": datetime.now(UTC).isoformat()}, indent=1))
        log("golden scenario", file=str(out.relative_to(REPO)))
    finally:
        spark.stop()
    return 0


# ── run ──────────────────────────────────────────────────────────────────────

@dataclass
class OpRecord:
    intent: Intent
    row: dict[str, Any] = field(default_factory=dict)
    journal: dict[str, Any] = field(default_factory=dict)


def set_s3_latency(env: Env, mean: float, jitter: float) -> None:
    with contextlib.suppress(requests.RequestException):
        requests.delete(f"{env.toxiproxy_api}/proxies/s3/toxics/capl_latency", timeout=5)
        if mean > 0:
            requests.post(f"{env.toxiproxy_api}/proxies/s3/toxics", json={
                "name": "capl_latency", "type": "latency", "stream": "downstream",
                "attributes": {"latency": int(mean), "jitter": int(jitter)}}, timeout=5).raise_for_status()


class Run:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.env = Env()
        self.sc = scn.load(args.scenario, variant=args.variant, level=args.concurrency,
                           size=args.size, overrides=json.loads(args.overrides) if args.overrides else None)
        if args.duration:
            self.sc.duration_s = float(args.duration)
        if args.warmup is not None:
            self.sc.warmup_s = float(args.warmup)
        if args.cooldown is not None:
            self.sc.cooldown_s = float(args.cooldown)
        if args.policy != "P0":
            raise SystemExit(f"policy {args.policy}: only P0 before stage 4 (advisor)")
        short = self.sc.name.split("_")[0]
        v = f"-{args.variant}" if args.variant else ""
        self.run_id = args.run_id or f"{args.series}-{short}-{args.policy}-{args.concurrency}-s{args.seed}{v}"
        self.intents = generate(self.sc, args.seed, self.run_id)
        self.records: list[OpRecord] = []
        self.bg_records: list[OpRecord] = []
        self.lock = threading.Lock()
        self.journal_dir = RUNS_DIR / self.run_id
        self.results: dict[str, Any] = {"run_id": self.run_id}

    def _golden(self) -> tuple[dict[str, Any], dict[str, Any]]:
        g = GOLDEN_DIR / f"{golden_name(self.sc)}.json"
        if not g.exists():
            raise SystemExit(f"no golden table {g.name}: make seed SCENARIO={self.sc.name}")
        gj = json.loads(g.read_text())
        dg = json.loads((GOLDEN_DIR / gj["data"]).read_text())
        return gj, dg

    def execute(self) -> int:
        env, sc, args = self.env, self.sc, self.args
        gj, dg = self._golden()
        db = DB(env.pg_dsn, env.pg_schema)
        db.migrate()
        prev = db.query("SELECT status FROM {s}.runs WHERE run_id = %s", [self.run_id])
        if prev and not args.force:
            try:
                if prev[0]["status"] != "finished":
                    raise ValueError(f"Cannot resume {self.run_id}: existing status={prev[0]['status']}; "
                                     "preserve measurements and use a new run_id.")
                require_valid_result(RESULTS_DIR / f"{self.run_id}.json")
            finally:
                db.close()
            log("already finished", run_id=self.run_id)
            return 0
        db.delete_run(self.run_id)
        self.journal_dir.mkdir(parents=True, exist_ok=True)
        (self.journal_dir / "intents.json").write_text(
            json.dumps([i.to_json() for i in self.intents], default=str))
        n_agents = sum(s.writers for s in sc.streams if s.engine == "spark")
        spark = build_session(env, self.run_id, f"capl-{self.run_id}", n_slots=sc.runner.n_slots,
                              n_agents=n_agents,
                              maintenance_writer=sc.expire.writer_class if sc.expire else "maintenance")
        try:
            return self._execute(spark, db, gj, dg)
        except BaseException:
            db.update_run(self.run_id, status="failed", finished_at=now(),
                          notes=traceback.format_exc()[-3000:])
            raise
        finally:
            with contextlib.suppress(Exception):
                spark.stop()
            db.close()

    def _execute(self, spark: Any, db: DB, gj: dict[str, Any], dg: dict[str, Any]) -> int:
        env, sc, args = self.env, self.sc, self.args
        table = sc.table.name
        # 1. reset: files of earlier runs, golden metadata, S3 latency
        keep = set((GOLDEN_DIR / dg_name(gj)).with_suffix(".keep.txt").read_text().split())
        t = time.monotonic()
        drop_table(env, table)                    # before gc: gc removes its metadata.json
        gc = gc_table_files(env, dg["location"], keep) if not args.no_gc else {}
        log("gc", **gc, s=round(time.monotonic() - t))
        reset_table(spark, env, table, gj["metadata_location"])
        st0 = load_table_state(env, table)
        assert st0.current_snapshot_id == gj["snapshot_id"]
        set_s3_latency(env, sc.s3_latency_ms.mean, sc.s3_latency_ms.jitter)
        t0 = now()
        t0m = time.monotonic()
        db.insert_run({
            "run_id": self.run_id, "scenario": sc.name, "policy": args.policy, "policy_params": {},
            "concurrency": args.concurrency, "seed": args.seed, "catalog_impl": env.catalog_impl,
            "iceberg_version": env.iceberg_version, "git_sha": git_sha(), "model_version": None,
            "params": {"variant": args.variant, "size": sc.profile.size, "routing": env.routing,
                       "series": args.series, "scenario": sc.model_dump(mode="json", by_alias=True),
                       "n_intents": len(self.intents), "golden": gj, "gc": gc},
            "started_at": t0, "status": "running"})
        cost = CostReader(spark, env.spark_ui_port)
        health = HealthSampler(spark, env, db, self.run_id, table, sc.runner.health_sample_interval_s)
        ctx = RunCtx(spark, env, db, sc, self.run_id, t0, self.journal_dir, cost, health)
        ex = OpExecutor(ctx, SlotPool([f"s{k}" for k in range(sc.runner.n_slots)], env),
                        SlotPool(["m0", "m1"], env))
        health.sample()
        health.start()
        # 2. uncontrolled writers
        from runner.freshness import StreamFreshness
        from runner.streaming import FlinkStream, KafkaFeed, SparkAgents
        freshness = StreamFreshness(env, self.run_id, table)
        feeds: list[KafkaFeed] = []
        flinks: list[FlinkStream] = []
        agents: list[SparkAgents] = []
        a_off = 0
        for i, s in enumerate(sc.streams):
            if s.engine == "flink":
                f = KafkaFeed(env, sc, s, i, self.run_id, args.seed, t0m)
                f.create_topic()
                fl = FlinkStream(env, sc, s, self.run_id, f)
                freshness.register(f.events)
                feeds.append(f)
                flinks.append(fl)
            else:
                ag = SparkAgents(spark, sc, s, i, self.run_id, args.seed, t0m, slot_offset=a_off)
                a_off += s.writers
                freshness.register(ag.events)
                agents.append(ag)
        freshness.start()
        for fl, feed in zip(flinks, feeds, strict=True):
            fl.start()
            feed.start()
        for ag in agents:
            ag.start()
        log("streams started", flink=len(flinks), agents=sum(len(a.gens) for a in agents))
        # 3. background maintenance (ADR-008)
        stop_bg = threading.Event()
        bg_thr: Any = None
        if sc.expire:
            from pyspark import InheritableThread
            bg_thr = InheritableThread(target=self._bg_expire, args=(ex, t0, t0m, stop_bg), daemon=True)
            bg_thr.start()
        # 4. controlled operations (P0: run now, limited by max_concurrent and slots)
        sems = {o.id: threading.Semaphore(o.max_concurrent) for o in sc.active_ops}
        ops_cfg = {o.id: o for o in sc.active_ops}
        threads: list[threading.Thread] = []
        from pyspark import InheritableThread
        for idx, it in enumerate(self.intents):
            wait = it.t_offset_s - (time.monotonic() - t0m)
            if wait > 0:
                time.sleep(wait)
            rec = OpRecord(it)
            with self.lock:
                self.records.append(rec)
            th = InheritableThread(target=self._op, args=(ex, rec, ops_cfg[it.op_key], sems[it.op_key],
                                                          idx, t0), daemon=True)
            th.start()
            threads.append(th)
        end_gen = sc.warmup_s + sc.duration_s
        time.sleep(max(0.0, end_gen - (time.monotonic() - t0m)))
        log("generation over", intents=len(self.intents))
        # 5. cooldown: wait for operations
        deadline = time.monotonic() + sc.cooldown_s
        for th in threads:
            th.join(timeout=max(0.0, deadline - time.monotonic()))
        overrun = [r.intent.op_id for r, th in zip(self.records, threads, strict=False) if th.is_alive()]
        for th in threads:
            th.join(timeout=3600)
        log("operations done", overrun_after_cooldown=len(overrun))
        # 6. stop writers and maintenance
        for ag in agents:
            ag.stop()
        for f in feeds:
            f.stop()
        flink_out = [fl.drain_and_stop() for fl in flinks]
        self.results["stream_freshness"] = freshness.stop()
        for f in feeds:
            f.delete_topic()
        stop_bg.set()
        if bg_thr is not None:
            bg_thr.join(timeout=1800)
        health.stop()
        health.sample()
        t_end = now()
        log("writers stopped", flink=flink_out)
        # 7. finalize operations, invariants, orphans
        self._footprints_executed(db)
        st = load_table_state(env, table)
        led = self._ledger(dg, gj, feeds, agents)
        ops_rows = [r.row for r in self.records + self.bg_records]
        import pickle
        with (self.journal_dir / "ledger.pkl").open("wb") as fh:     # for the fault-injection test
            pickle.dump({"ledger": led, "ops": ops_rows, "current": st.current_snapshot_id,
                         "checks": list(sc.invariants.checks), "n_buckets": sc.table.n_buckets,
                         "table": table}, fh)
        inv = Invariants(spark, db, self.run_id, table, sc.table.n_buckets, st.current_snapshot_id, env)
        inv_res: dict[str, Any] = {"ok": True}
        if sc.invariants.enabled:
            inv_res = inv.run(led, ops_rows, list(sc.invariants.checks))
        log("invariants", ok=inv_res.get("ok"))
        orph = orphan_scan(spark, env, self.run_id, table, t0, dg["location"])
        db.insert_many("orphan_scan", [{"run_id": self.run_id, "table_name": table, "at": now(),
                                         "orphan_files": orph["orphan_files"],
                                         "orphan_bytes": orph["orphan_bytes"]}])
        app_totals = cost.app_totals()
        self.results.update({
            "scenario": sc.name, "variant": args.variant, "level": args.concurrency, "seed": args.seed,
            "policy": args.policy, "git_sha": git_sha(), "t0": t0.isoformat(), "t_end": t_end.isoformat(),
            "intents": len(self.intents), "overrun_after_cooldown": overrun,
            "streams": {"kafka": [{"stream": f.stream.id, "produced": f.n, "errors": f.errors[:5]}
                                  for f in feeds],
                        "flink": flink_out,
                        "agents": [{"stream": a.stream.id, "ok": a.n_ok, "failed": a.n_fail,
                                    "errors": a.errors[:5]} for a in agents]},
            "invariants": inv_res, "orphans": orph, "spark_app_totals": app_totals,
            "health": {"samples": health.n, "errors": health.errors[:5],
                       "max_snapshots": health.max_snapshots,
                       "max_metadata_bytes": health.max_metadata_bytes},
            "cooldown_s": sc.cooldown_s, "warmup_s": sc.warmup_s, "duration_s": sc.duration_s,
        })
        incomplete_cost = [r["op_id"] for r in ops_rows if r.get("outcome") != "noop"
                           and not (r.get("cost_detail") or {}).get("complete")]
        runner_errors = [r["op_id"] for r in ops_rows
                         if r.get("outcome") in ("error", "commit_unknown", "history_unresolved")]
        self.results["validation"] = {
            "ok": bool(inv_res.get("ok")) and self.results["stream_freshness"]["complete"]
                  and not incomplete_cost and not runner_errors and not health.errors
                  and all(not feed.errors for feed in feeds)
                  and all(fl["final_lag"] == 0 for fl in flink_out),
            "incomplete_cost": incomplete_cost, "runner_errors": runner_errors}
        from analysis.metrics import compute_summary
        summ = compute_summary(db, self.run_id, extra=self.results)
        self.results["summary"] = summ
        db.update_run(self.run_id, status="finished", finished_at=now(),
                      invariants_ok=bool(inv_res.get("ok")))
        RESULTS_DIR.mkdir(parents=True, exist_ok=True)
        (RESULTS_DIR / f"{self.run_id}.json").write_text(json.dumps(self.results, indent=1, default=str))
        log("run finished", run_id=self.run_id, invariants_ok=inv_res.get("ok"))
        return 0 if self.results["validation"]["ok"] else 1

    def _op(self, ex: OpExecutor, rec: OpRecord, op: scn.ControlledOp, sem: threading.Semaphore,
            idx: int, t0: datetime) -> None:
        it = rec.intent
        intent_at = t0 + timedelta(seconds=it.t_offset_s)
        base = {"op_id": it.op_id, "run_id": self.run_id, "table_name": self.sc.table.name,
                "op_type": it.op_type, "op_key": it.op_key, "writer_class": "controlled",
                "controlled": True, "intent_at": intent_at, "footprint_requested": [],
                "sla_deadline": intent_at + timedelta(seconds=it.sla_s)}
        ex.ctx.db.upsert_op(base)
        with sem:
            try:
                res = ex.execute(op, it, idx, controlled=True, writer_class="controlled",
                                 intent_at=intent_at,
                                 pool=self.sc.runner.spark_pool_by_type[it.op_type])
                rec.row, rec.journal = res.row, res.journal
            except Exception as exc:
                rec.row = {**base, "outcome": "error", "error_class": "runner",
                           "error_message": traceback.format_exc()[-3000:], "finished_at": now()}
                log("op runner error", op_id=it.op_id, exc=repr(exc)[:300])
        ex.ctx.db.upsert_op(rec.row)
        log("op", op_id=it.op_id, outcome=rec.row.get("outcome"), attempts=rec.row.get("attempts"),
            cpu_s=round((rec.row.get("cpu_ms") or 0) / 1000, 1))

    def _bg_expire(self, ex: OpExecutor, t0: datetime, t0m: float, stop: threading.Event) -> None:
        mx = self.sc.expire
        assert mx is not None
        op = scn.ControlledOp.model_validate({
            "id": "bg_expire", "type": "expire", "arrival": {"process": "fixed", "interval_s": mx.interval_s},
            "max_concurrent": 1, "sla_s": mx.interval_s, "footprint": {"selector": "table"},
            "params": {"older_than_s": mx.older_than_s, "retain_last": mx.retain_last}})
        n = 0
        pool = self.sc.runner.spark_pool_by_type.get("expire", "maintenance")
        while not stop.wait(max(0.0, (n + 1) * mx.interval_s - (time.monotonic() - t0m))):
            sched = (n + 1) * mx.interval_s
            it = Intent(op_id=f"{self.run_id}-bg_expire-{n:04d}", op_key="bg_expire", op_type="expire",
                        seq=n, t_offset_s=sched, sla_s=mx.interval_s, op_seed=n,
                        footprint={"selector": "table", "partitions": []})
            n += 1
            rec = OpRecord(it)
            try:
                res = ex.execute(op, it, -1, controlled=False, writer_class=mx.writer_class,
                                 intent_at=t0 + timedelta(seconds=sched), pool=pool)
                rec.row, rec.journal = res.row, res.journal
                rec.row["outcome_detail"] = {**(rec.row.get("outcome_detail") or {}),
                                             "late_s": round(time.monotonic() - t0m - sched, 1)}
            except Exception:
                rec.row = {"op_id": it.op_id, "run_id": self.run_id, "table_name": self.sc.table.name,
                           "op_type": "expire", "op_key": "bg_expire", "writer_class": mx.writer_class,
                           "controlled": False, "intent_at": t0 + timedelta(seconds=sched),
                           "outcome": "error", "error_message": traceback.format_exc()[-3000:]}
            self.bg_records.append(rec)
            ex.ctx.db.upsert_op(rec.row)
            log("bg expire", n=n, outcome=rec.row.get("outcome"),
                s=_dur(rec.row), cpu_s=round((rec.row.get("cpu_ms") or 0) / 1000, 1))

    def _footprints_executed(self, db: DB) -> None:
        """footprint_executed = partitions of the operation's snapshots (proxy journal)."""
        if self.env.routing != "proxy":
            return
        deadline = time.monotonic() + 120
        for rec in self.records:
            r = rec.row
            if r.get("outcome") != "committed":
                continue
            att = db.attempts_for_op(r["op_id"])
            sids = [int(a["new_snapshot_id"]) for a in att
                    if a["http_status"] and 200 <= a["http_status"] < 300 and a["new_snapshot_id"]]
            # final read of the journal (the executor may have read it before the last flush)
            r["attempts"] = len(att)
            r["held_ms"] = sum(int(a["hold_ms"] or 0) for a in att)
            if att:
                r["last_attempt_at"] = att[-1]["received_at"]
            if sids:
                r["snapshot_id"] = sids[-1]
            parts: set[str] = set()
            for sid in sids:
                while True:
                    ev = db.query("SELECT 1 FROM {s}.commit_events WHERE table_name = %s AND "
                                  "snapshot_id = %s", [r["table_name"], sid])
                    if ev or time.monotonic() > deadline:
                        break
                    time.sleep(1)
                parts |= {x["partition"] for x in db.query(
                    "SELECT partition FROM {s}.commit_partitions WHERE table_name = %s AND snapshot_id = %s",
                    [r["table_name"], sid])}
            r["footprint_executed"] = sorted(parts)
            db.upsert_op({"op_id": r["op_id"], "run_id": r["run_id"], "table_name": r["table_name"],
                          "op_type": r["op_type"], "writer_class": r["writer_class"],
                          "intent_at": r["intent_at"], "footprint_executed": r["footprint_executed"],
                          "attempts": r["attempts"], "held_ms": r["held_ms"],
                          "last_attempt_at": r.get("last_attempt_at"), "snapshot_id": r.get("snapshot_id")})

    def _ledger(self, dg: dict[str, Any], gj: dict[str, Any], feeds: list[Any], agents: list[Any]
                ) -> Ledger:
        led = Ledger(golden_snapshot_id=int(gj["snapshot_id"]))
        for f in feeds:
            led.stream_events.append(("stream", f.events))
        for a in agents:
            led.stream_events.append(("agent", a.events))
            led.excluded_ranges += a.failed_ids
        ledger_db = DB(self.env.pg_dsn, self.env.pg_schema)
        try:
            events = ledger_db.query(
                "SELECT snapshot_id, parent_snapshot_id FROM {s}.commit_events WHERE run_id = %s",
                [self.run_id])
        finally:
            ledger_db.conn.close()
        parents = {int(e["snapshot_id"]): e["parent_snapshot_id"] for e in events}
        order = {led.golden_snapshot_id: 0}

        def depth(sid: int) -> int:
            trail = []
            while sid not in order:
                if sid in trail or sid not in parents or parents[sid] is None:
                    raise ValueError(f"incomplete operation lineage at {sid}")
                trail.append(sid)
                sid = int(parents[sid])
            for child in reversed(trail):
                order[child] = order[sid] + 1
                sid = child
            return order[sid]
        committed = [rec for rec in self.records if rec.row.get("outcome") == "committed"]
        committed.sort(key=lambda rec: depth(int(rec.row["snapshot_id"])))
        for rec in self.records:
            if rec.row.get("outcome") != "committed":
                led.aborted_inserted += [int(i) for i in rec.journal.get("inserted_ids", [])]
        for rec in committed:
            jr = rec.journal
            if not jr.get("effects_recorded") and rec.row.get("op_type") in (
                    "cow_dml", "mor_delete", "mor_update", "mor_merge"):
                raise ValueError(f"missing exact effects: {rec.row['op_id']}")
            led.apply_effects(jr)
        return led


def dg_name(gj: dict[str, Any]) -> str:
    return str(gj["data"])


def _dur(row: dict[str, Any]) -> float | None:
    s, f = row.get("started_at"), row.get("finished_at")
    return round((f - s).total_seconds(), 1) if s and f else None


def plan(args: argparse.Namespace) -> int:
    sc = scn.load(args.scenario, variant=args.variant, level=args.concurrency, size=args.size)
    out = [i.to_json() for i in generate(sc, args.seed, args.run_id or "plan")]
    print(json.dumps(out, indent=1, default=str))
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="runner.main")
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name in ("seed", "run", "plan"):
        p = sub.add_parser(name)
        p.add_argument("--scenario", required=True)
        p.add_argument("--variant", default=None)
        p.add_argument("--size", default=None, choices=[None, "full", "lite"])
        if name == "seed":
            p.add_argument("--force-data", action="store_true")
        else:
            p.add_argument("--policy", default="P0")
            p.add_argument("--concurrency", default="mid")
            p.add_argument("--seed", type=int, default=11)
            p.add_argument("--run-id", default=None)
        if name == "run":
            p.add_argument("--series", default="stage3")
            p.add_argument("--duration", type=float, default=None)
            p.add_argument("--warmup", type=float, default=None)
            p.add_argument("--cooldown", type=float, default=None)
            p.add_argument("--overrides", default=None, help="JSON merged into the scenario")
            p.add_argument("--force", action="store_true")
            p.add_argument("--no-gc", action="store_true")
    args = ap.parse_args(argv)
    if args.cmd == "seed":
        return seed(args)
    if args.cmd == "plan":
        return plan(args)
    return Run(args).execute()


if __name__ == "__main__":
    sys.exit(main())
