"""Execution of one operation: SQL of every `statement` and maintenance op, outcome, cost.

Outcome labels (AGENTS.md constraint 6, FR-RUN-4; author decision 2026-11-04):
  committed         statement returned (attempts > 1 with HTTP 409 = K1 with successful retry)
  data_conflict     ValidationException (K2)
  retry_exhausted   CommitFailedException (K1 failure)
  base_expired      history removed by expire, CONFIRMED by data: the removed snapshot (base, or
                    the one after it) is absent from the table metadata at the failure, and the
                    proxy journal has a successful commit that removed it between the start of
                    the operation and the failure
  history_unresolved  same exception text without that confirmation: an error of the runner
                    (e.g. a wrong validate-from-snapshot-id); counted separately and kept in the
                    loss metric (REVIEWER.md P5)
  commit_unknown    CommitStateUnknownException
  noop              nothing to do (empty footprint, rewrite/expire without a commit)
  error             anything else
"""
from __future__ import annotations

import json
import re
import time
import traceback
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

import requests

from runner.datagen import merge_insert_rows
from runner.env import Env
from runner.intents import Intent, bucket_long, parse_partition, partition_str
from runner.ops.errors import classify, expire_cause, root_iceberg_exception
from runner.scenario import ControlledOp, Scenario
from runner.slots import SlotPool, job_scope

if TYPE_CHECKING:
    from runner.cost import CostReader
    from runner.db import DB

UTC = timezone.utc  # noqa: UP017 — runner image is Python 3.10

_RX_BASE = re.compile(r"Snapshot (-?\d+) is not an ancestor of (-?\d+)")
_RX_HIST = re.compile(r"Cannot determine history between starting snapshot (-?\d+) and the last known "
                      r"ancestor (-?\d+)")

ROW_LEVEL = {"cow_dml", "mor_delete", "mor_update", "mor_merge"}


def now() -> datetime:
    return datetime.now(UTC)


# ── table metadata through the catalog REST API (no Spark cache) ─────────────

@dataclass
class TableState:
    current_snapshot_id: int | None
    snapshots: dict[int, dict[str, Any]]           # id -> {parent, ts_ms}
    metadata_location: str
    metadata: dict[str, Any] = field(repr=False, default_factory=dict)

    def ts(self, sid: int | None) -> datetime | None:
        s = self.snapshots.get(sid) if sid is not None else None
        return datetime.fromtimestamp(s["ts_ms"] / 1000, UTC) if s else None


def load_table_state(env: Env, table: str) -> TableState:
    ns, name = table.split(".", 1)
    r = requests.get(f"{env.direct_uri}/v1/namespaces/{ns}/tables/{name}", timeout=30)
    r.raise_for_status()
    body = r.json()
    md = body["metadata"]
    snaps = {int(s["snapshot-id"]): {"parent": s.get("parent-snapshot-id"),
                                      "ts_ms": int(s["timestamp-ms"])}
             for s in md.get("snapshots", [])}
    cur = md.get("current-snapshot-id")
    cur = int(cur) if cur is not None and int(cur) != -1 else None
    return TableState(cur, snaps, body.get("metadata-location", ""), md)


# ── footprint predicates ──────────────────────────────────────────────────────

def fp_predicate(partitions: list[str], n_buckets: int, alias: str = "") -> str:
    """(dt, bucket) pairs as a predicate grouped by bucket; uses system.bucket (tz/05 §2)."""
    a = f"{alias}." if alias else ""
    by_b: dict[int, list[date]] = {}
    for p in partitions:
        d, b = parse_partition(p)
        by_b.setdefault(b, []).append(d)
    terms = []
    for b, days in sorted(by_b.items()):
        ds = ", ".join(f"DATE '{d.isoformat()}'" for d in sorted(days))
        day_term = f"{a}dt = {ds}" if len(days) == 1 else f"{a}dt IN ({ds})"
        terms.append(f"(system.bucket({n_buckets}, {a}key) = {b} AND {day_term})")
    return "(" + " OR ".join(terms) + ")"


# ── execution context ─────────────────────────────────────────────────────────

@dataclass
class RunCtx:
    spark: Any
    env: Env
    db: DB
    sc: Scenario
    run_id: str
    t0: datetime                      # start of the run (warm-up included)
    journal_dir: Path
    cost: CostReader
    health: Any                       # runner.health.HealthSampler (partition stats cache)

    @property
    def table(self) -> str:
        return self.sc.table.name


@dataclass
class OpResult:
    row: dict[str, Any]
    journal: dict[str, Any]


class OpExecutor:
    def __init__(self, ctx: RunCtx, slots: SlotPool, maint_slots: SlotPool) -> None:
        self.ctx = ctx
        self.slots = slots
        self.maint_slots = maint_slots

    # ── footprints of state selectors ──
    def _state_footprint(self, fp: dict[str, Any]) -> list[str]:
        stats = self.ctx.health.partition_stats(max_age_s=30.0)
        sel = fp["selector"]
        today = self.ctx.sc.table.logical_today
        rows = []
        for part, st in stats.items():
            if not part:
                continue
            d, _ = parse_partition(part)
            age = (today - d).days
            if sel == "small_files_topn":
                if age >= fp.get("min_age_days", 0) and st["small_files"] >= fp.get("min_files", 0):
                    rows.append((st["small_files"], part))
            elif sel == "delete_files_topn" and st["delete_files"] >= 2:
                rows.append((st["delete_files"], part))
        rows.sort(key=lambda x: (-x[0], x[1]))
        return sorted(p for _, p in rows[: fp["top_n"]])

    def footprint(self, intent: Intent) -> list[str]:
        if intent.state_dependent:
            return self._state_footprint(intent.footprint)
        return list(intent.footprint.get("partitions", []))

    # ── statements ──
    def _sql(self, op: ControlledOp, intent: Intent, slot: str, parts: list[str],
             planning: int | None, op_index: int, jr: dict[str, Any]) -> list[str]:
        sc = self.ctx.sc
        t = f"{slot}.{sc.table.name}"
        nb = sc.table.n_buckets
        p = op.params
        pred = fp_predicate(parts, nb) if parts else "TRUE"
        st = p.get("statement")
        if op.type in ("cow_dml", "mor_merge") and st in ("merge_upsert", "merge_update"):
            return [self._merge(op, intent, t, parts, planning, op_index, jr)]
        if st == "update_status":
            r = intent.op_seed % 97
            jr["update_rule"] = {"mod": 97, "r": r, "partitions": parts}
            return [f"UPDATE {t} SET status = 'u{intent.seq}', version = version + 1 "
                    f"WHERE {pred} AND pmod(key, 97) = {r}"]
        if st == "delete_by_keys":
            keys = intent.footprint["keys"]
            jr["deleted_keys"] = keys
            return [f"DELETE FROM {t} WHERE {pred} AND key IN ({', '.join(map(str, keys))})"]
        if op.type == "overwrite":
            day = intent.footprint["day"]
            opts = ""
            if p.get("isolation_level_option"):
                # ADR-006: isolation-level only together with validate-from-snapshot-id
                opts = (f"WITH ('isolation-level' = '{p['isolation_level_option']}', "
                        f"'validate-from-snapshot-id' = '{planning}') ")
            jr["overwrite_day"] = day
            return [f"INSERT OVERWRITE {t} {opts}SELECT event_id, key, dt, event_ts, amount, "
                    f"'reprocessed' AS status, version, payload FROM lake.{sc.table.name} "
                    f"VERSION AS OF {planning} WHERE dt = DATE '{day}'"]
        if op.type == "rewrite_data":
            o = {"min-input-files": str(p.get("min_input_files", 5)),
                 "partial-progress.enabled": str(bool(p.get("partial_progress", False))).lower()}
            if p.get("partial_progress_max_commits"):
                o["partial-progress.max-commits"] = str(p["partial_progress_max_commits"])
            if p.get("delete_file_threshold") is not None:
                o["delete-file-threshold"] = str(p["delete_file_threshold"])
            om = ", ".join(f"'{k}', '{v}'" for k, v in o.items())
            return [f"CALL {slot}.system.rewrite_data_files(table => '{sc.table.name}', "
                    f"strategy => '{p.get('strategy', 'binpack')}', where => \"{pred}\", "
                    f"options => map({om}))"]
        if op.type == "rewrite_deletes":
            return [f"CALL {slot}.system.rewrite_position_delete_files(table => '{sc.table.name}', "
                    f"where => \"{pred}\", options => map('rewrite-all', 'true'))"]
        if op.type == "expire":
            older = (now() - timedelta(seconds=float(p["older_than_s"]))).strftime(
                "%Y-%m-%d %H:%M:%S.%f")[:-3]
            return [f"CALL {slot}.system.expire_snapshots(table => '{sc.table.name}', "
                    f"older_than => TIMESTAMP '{older}', retain_last => {int(p['retain_last'])})"]
        raise ValueError(f"{op.id}: no statement for type {op.type} / {st}")

    def _merge(self, op: ControlledOp, intent: Intent, t: str, parts: list[str],
               planning: int | None, op_index: int, jr: dict[str, Any]) -> str:
        """CDC batch: existing rows of the footprint (update_fraction) + new rows; the batch is
        materialised before the MERGE (it does not read the target in the MERGE)."""
        from pyspark.sql.types import (
            DateType,
            DoubleType,
            IntegerType,
            LongType,
            StringType,
            StructField,
            StructType,
            TimestampNTZType,
        )
        sc = self.ctx.sc
        spark = self.ctx.spark
        p = op.params
        n_rows = int(intent.footprint.get("rows") or op.footprint.rows or 0)
        uf = 1.0 if p["statement"] == "merge_update" else float(p.get("update_fraction", 0.8))
        n_upd = round(n_rows * uf)
        n_new = n_rows - n_upd
        pred = fp_predicate(parts, sc.table.n_buckets)
        sv = f" VERSION AS OF {planning}" if planning is not None else ""
        upd = spark.sql(
            f"SELECT event_id, key, dt, event_ts, payload FROM lake.{sc.table.name}{sv} "
            f"WHERE {pred} ORDER BY xxhash64(event_id, {intent.op_seed % 2**31}) LIMIT {n_upd}"
        ).collect()
        jr["updated_ids"] = [int(r["event_id"]) for r in upd]
        import numpy as np
        rnd = np.random.Generator(np.random.PCG64(intent.op_seed))
        amounts = rnd.uniform(0, 10_000, size=max(1, len(upd)))
        rows: list[tuple[Any, ...]] = [
            (int(r["event_id"]), int(r["key"]), r["dt"], r["event_ts"], float(round(amounts[i], 2)),
             f"m{intent.seq}", 0, r["payload"]) for i, r in enumerate(upd)]
        pk = [parse_partition(x) for x in parts]
        ins = merge_insert_rows(sc, op_index, intent.op_seed, pk, n_new)
        jr["inserted_ids"] = [int(x[0]) for x in ins]
        jr["inserted_rows"] = [(int(x[1]), partition_str(x[2], bucket_long(int(x[1]), sc.table.n_buckets)))
                               for x in ins]                       # (key, partition) per inserted id
        rows += ins
        schema = StructType([
            StructField("event_id", LongType(), False), StructField("key", LongType(), False),
            StructField("dt", DateType(), False), StructField("event_ts", TimestampNTZType()),
            StructField("amount", DoubleType()), StructField("status", StringType()),
            StructField("version", IntegerType()), StructField("payload", StringType())])
        view = "src_" + re.sub(r"\W", "_", intent.op_id)
        spark.createDataFrame(rows, schema).createOrReplaceTempView(view)
        jr["view"] = view
        tgt = fp_predicate(parts, sc.table.n_buckets, alias="t")
        return (f"MERGE INTO {t} t USING {view} s "
                f"ON t.event_id = s.event_id AND t.dt = s.dt AND t.key = s.key AND {tgt} "
                f"WHEN MATCHED THEN UPDATE SET amount = s.amount, status = s.status, "
                f"version = t.version + 1 WHEN NOT MATCHED THEN INSERT *")

    def record_effects(self, op: ControlledOp, row: dict[str, Any], jr: dict[str, Any]) -> None:
        """Record logical effects against the actual input snapshot of the DML scan.

        Call after commit telemetry has provided snapshot_id. These diagnostic reads use an
        admin job tag, outside the operation cost scope. Failure invalidates the run; it must
        never silently fall back to key-wide exclusions or planning-time UPDATE predicates.
        """
        if row.get("outcome") != "committed" or op.type not in ROW_LEVEL:
            return
        ctx = self.ctx
        sid = int(row["snapshot_id"])
        st = load_table_state(ctx.env, ctx.table)
        snap = st.snapshots[sid]
        parent = snap["parent"]
        if parent is None:
            raise ValueError(f"operation snapshot {sid} has no predecessor")
        nb = ctx.sc.table.n_buckets
        part = (f"concat('dt=', CAST(dt AS STRING), '/key_bucket=', "
                f"CAST(system.bucket({nb}, key) AS STRING))")
        read_sid = int(jr["read_snapshot_id"])
        table = f"lake.{ctx.table} VERSION AS OF {read_sid}"
        statement = op.params.get("statement")
        with job_scope(ctx.spark, f"effects-{row['op_id']}", "exact effects", "admin"):
            if statement in ("delete_by_keys", "update_status"):
                pred = fp_predicate(row["footprint_requested"], nb)
                if statement == "delete_by_keys":
                    keys = ", ".join(str(int(k)) for k in jr["deleted_keys"])
                    pred += f" AND key IN ({keys})"
                else:
                    rule = jr["update_rule"]
                    pred += f" AND pmod(key, {int(rule['mod'])}) = {int(rule['r'])}"
                rows = ctx.spark.sql(f"SELECT event_id, key, {part} AS p FROM {table} "
                                     f"WHERE {pred}").collect()
                if statement == "delete_by_keys":
                    jr["deleted_rows"] = [(int(r["event_id"]), int(r["key"]), r["p"]) for r in rows]
                else:
                    jr["effect_updated_ids"] = [int(r["event_id"]) for r in rows]
            elif statement in ("merge_upsert", "merge_update"):
                source_ids = [int(i) for i in jr.get("updated_ids", [])]
                planned_inserts = [(int(i), int(k), p) for i, (k, p) in
                                   zip(jr["inserted_ids"], jr["inserted_rows"], strict=True)]
                source_ids += [i for i, _k, _p in planned_inserts]
                ids = ", ".join(map(str, source_ids)) or "NULL"
                present = {int(r["event_id"]) for r in ctx.spark.sql(
                    f"SELECT event_id FROM {table} WHERE event_id IN ({ids})").collect()}
                jr["effect_updated_ids"] = sorted(present)
                inserted = [r for r in planned_inserts if r[0] not in present]
                missing = sorted(set(jr.get("updated_ids", [])) - present)
                if missing:
                    planning = int(row["planning_snapshot_id"])
                    missing_sql = ", ".join(map(str, missing))
                    restored = ctx.spark.sql(
                        f"SELECT event_id, key, {part} AS p FROM lake.{ctx.table} "
                        f"VERSION AS OF {planning} WHERE event_id IN ({missing_sql})").collect()
                    if len(restored) != len(missing):
                        raise ValueError("MERGE source rows unavailable for exact effect ledger")
                    inserted += [(int(r["event_id"]), int(r["key"]), r["p"]) for r in restored]
                jr["effect_inserted_rows"] = inserted
            else:
                raise ValueError(f"no exact effect recorder for {statement}")
        jr["effects_parent_snapshot_id"] = int(parent)
        jr["effects_read_snapshot_id"] = read_sid
        jr["effects_recorded"] = True

    # ── outcome ──
    def _confirm_expired(self, text: str, t_start: datetime, t_fail: datetime) -> dict[str, Any]:
        """Evidence for base_expired (author decision 2026-11-04, REVIEWER.md P5)."""
        cause = expire_cause(text)
        ev: dict[str, Any] = {"cause": cause}
        m = _RX_BASE.search(text) if cause == "base_snapshot" else _RX_HIST.search(text)
        if not m:
            ev["confirmed"] = False
            ev["why"] = "snapshot id not found in the message"
            return ev
        x = int(m.group(1))
        ev["snapshot"] = x
        # base_snapshot: the base itself; validation_history: the snapshot after the base
        targets = [x] if cause == "base_snapshot" else self.ctx.db.validation_path(
            self.ctx.table, int(m.group(2)), x)
        ev["removed_target"] = targets
        try:
            stt = load_table_state(self.ctx.env, self.ctx.table)
            absent = [s for s in targets if s not in stt.snapshots]
        except requests.RequestException as exc:
            absent = []
            ev["metadata_error"] = repr(exc)
        ev["absent_in_metadata"] = absent
        time.sleep(1.0)                                   # proxy telemetry flush (0.5 s batches)
        commits = self.ctx.db.expire_commits(self.ctx.table, t_start, t_fail)
        hits = [c for c in commits if set(c["removed_snapshot_ids"] or []) & set(absent)]
        ev["expire_commits"] = [{"op_id": c["op_id"], "responded_at": str(c["responded_at"])}
                                for c in hits]
        ev["confirmed"] = bool(targets) and bool(absent) and bool(hits)
        return ev

    def _outcome(self, exc_text: str, t_start: datetime, t_fail: datetime
                 ) -> tuple[str, dict[str, Any]]:
        cls = classify(exc_text)
        detail: dict[str, Any] = {"classified": cls}
        if cls == "base_expired":
            ev = self._confirm_expired(exc_text, t_start, t_fail)
            detail["expire_evidence"] = ev
            return ("base_expired" if ev["confirmed"] else "history_unresolved"), detail
        if cls == "K2":
            return "data_conflict", detail
        if cls == "K1":
            return "retry_exhausted", detail
        if root_iceberg_exception(exc_text) == "CommitStateUnknownException":
            return "commit_unknown", detail
        return "error", detail

    # ── main entry ──
    def execute(self, op: ControlledOp | None, intent: Intent, op_index: int, *, controlled: bool,
                writer_class: str, intent_at: datetime, pool: str) -> OpResult:
        ctx = self.ctx
        op_cfg = op
        assert op_cfg is not None
        row: dict[str, Any] = {
            "op_id": intent.op_id, "run_id": ctx.run_id, "table_name": ctx.table,
            "op_type": intent.op_type, "op_key": intent.op_key, "writer_class": writer_class,
            "controlled": controlled, "intent_at": intent_at, "decision": "run_now",
            "n_deferrals": 0, "sla_deadline": intent_at + timedelta(seconds=intent.sla_s),
            "mode": _mode(ctx.sc, intent.op_type, op_cfg),
            "isolation": _isolation(ctx.sc, op_cfg),
            "in_metrics": controlled and intent.t_offset_s >= ctx.sc.warmup_s,
        }
        jr: dict[str, Any] = {"op_id": intent.op_id}
        parts = self.footprint(intent)
        row["footprint_requested"] = parts
        if not parts and intent.op_type != "expire":
            row.update(outcome="noop", started_at=now(), finished_at=now(), attempts=0,
                       outcome_detail={"reason": "empty footprint"})
            return OpResult(row, jr)
        pool_slots = self.slots if controlled else self.maint_slots
        with pool_slots.lease(intent.op_id, intent.op_type) as slot:
            row["slot"] = slot
            try:
                stt = load_table_state(ctx.env, ctx.table)
                planning = stt.current_snapshot_id
                row["planning_snapshot_id"] = planning
                row["planning_snapshot_at"] = stt.ts(planning)
            except requests.RequestException as exc:
                planning = None
                jr["planning_error"] = repr(exc)
            t_start = now()
            row["started_at"] = t_start
            row["decision_delay_ms"] = int((t_start - intent_at).total_seconds() * 1000)
            exc_text = None
            result_rows: list[Any] = []
            frame = None
            try:
                with job_scope(ctx.spark, intent.op_id, f"{intent.op_type}:{intent.op_key}", pool):
                    stmts = self._sql(op_cfg, intent, slot, parts, planning, op_index, jr)
                    jr["sql"] = [s[:4000] for s in stmts]
                    for s in stmts:
                        frame = ctx.spark.sql(s)
                        result_rows = [r.asDict() for r in frame.collect()]
            except Exception as exc:              # Py4J / AnalysisException: text has the Java chain
                exc_text = str(exc) + "\n" + traceback.format_exc()[-2000:]
            t_end = now()
            row["finished_at"] = t_end
            if jr.get("view"):
                ctx.spark.catalog.dropTempView(jr["view"])
        jr["result"] = [{k: str(v) for k, v in r.items()} for r in result_rows][:5]
        if exc_text is None:
            row["outcome"] = "committed"
            row["outcome_detail"] = {"result": jr["result"]}
        else:
            outcome, detail = self._outcome(exc_text, t_start, t_end)
            row["outcome"] = outcome
            row["outcome_detail"] = detail
            row["error_class"] = root_iceberg_exception(exc_text) or _java_exc(exc_text)
            row["error_message"] = exc_text[:4000]
        self._attempts(row)
        if row["outcome"] == "committed" and ctx.env.routing == "proxy" and not row.get("attempts"):
            row["outcome"] = "noop"               # rewrite / expire with nothing to commit
        c = ctx.cost.read(intent.op_id, allow_empty=row["outcome"] == "noop")
        row["cpu_ms"], row["run_ms"] = c.cpu_ms, c.run_ms
        row["bytes_written"] = c.output_bytes
        row["cost_detail"] = c.to_json()
        if row["outcome"] == "committed" and controlled:
            if op_cfg.type in ROW_LEVEL:
                from runner.read_snapshot import read_snapshot_id
                jr["read_snapshot_id"] = read_snapshot_id(frame, f"{row['slot']}.{ctx.table}")
            self.record_effects(op_cfg, row, jr)
        _write_journal(ctx.journal_dir, intent.op_id, jr)
        return OpResult(row, jr)

    def _attempts(self, row: dict[str, Any]) -> None:
        """Commit attempts of the operation from the proxy journal (op_id binding of the slot)."""
        if self.ctx.env.routing != "proxy":
            return
        att: list[dict[str, Any]] = []
        for _ in range(20):
            att = self.ctx.db.attempts_for_op(row["op_id"])
            ok_seen = any(a["http_status"] and 200 <= a["http_status"] < 300 for a in att)
            # the proxy journal is written in batches: a committed op waits for its 2xx attempt
            if (row["outcome"] == "committed" and ok_seen) or (row["outcome"] != "committed" and att):
                break
            time.sleep(0.5)
        row["attempts"] = len(att)
        row["held_ms"] = sum(int(a["hold_ms"] or 0) for a in att)
        if att:
            row["commit_requested_at"] = att[0]["received_at"]
            row["last_attempt_at"] = att[-1]["received_at"]
            ok = [a for a in att if a["http_status"] and 200 <= a["http_status"] < 300]
            if ok and ok[-1]["new_snapshot_id"] is not None:
                row["snapshot_id"] = int(ok[-1]["new_snapshot_id"])
            d = row.get("outcome_detail") or {}
            d["http_statuses"] = [a["http_status"] for a in att]
            row["outcome_detail"] = d


def _java_exc(text: str) -> str:
    m = re.search(r"((?:java|org|scala)\.[\w.$]+(?:Exception|Error))", text)
    return m.group(1) if m else "python"


def _mode(sc: Scenario, op_type: str, op: ControlledOp) -> str | None:
    st = op.params.get("statement")
    key = {"merge_upsert": "merge", "merge_update": "merge", "update_status": "update",
           "delete_by_keys": "delete"}.get(st or "")
    return sc.table.properties.get(f"write.{key}.mode") if key else None


def _isolation(sc: Scenario, op: ControlledOp) -> str | None:
    st = op.params.get("statement")
    key = {"merge_upsert": "merge", "merge_update": "merge", "update_status": "update",
           "delete_by_keys": "delete"}.get(st or "")
    if key:
        return sc.table.properties.get(f"write.{key}.isolation-level", "serializable")
    if op.type == "overwrite":
        return op.params.get("isolation_level_option")
    return None


def _write_journal(d: Path, op_id: str, jr: dict[str, Any]) -> None:
    d.mkdir(parents=True, exist_ok=True)
    (d / f"{op_id}.json").write_text(json.dumps(jr, default=str))
