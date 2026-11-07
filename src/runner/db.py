"""Telemetry DB access of the runner (psycopg 3, synchronous; schema `stand`)."""
from __future__ import annotations

import json
from collections.abc import Iterable, Sequence
from typing import Any

import psycopg
from psycopg.rows import dict_row

# Columns added by stage 3 (idempotent; also in infra/postgres/init/01_telemetry.sql)
MIGRATIONS = (
    "ALTER TABLE {s}.ops ADD COLUMN IF NOT EXISTS op_key text",
    "ALTER TABLE {s}.ops ADD COLUMN IF NOT EXISTS planning_snapshot_id bigint",
    "ALTER TABLE {s}.ops ADD COLUMN IF NOT EXISTS planning_snapshot_at timestamptz",
    "ALTER TABLE {s}.ops ADD COLUMN IF NOT EXISTS last_attempt_at timestamptz",
    "ALTER TABLE {s}.ops ADD COLUMN IF NOT EXISTS outcome_detail jsonb",
    "ALTER TABLE {s}.ops ADD COLUMN IF NOT EXISTS cost_detail jsonb",
    "ALTER TABLE {s}.ops ADD COLUMN IF NOT EXISTS in_metrics boolean",
    "ALTER TABLE {s}.commit_attempts ADD COLUMN IF NOT EXISTS removed_snapshot_ids bigint[]",
    """CREATE TABLE IF NOT EXISTS {s}.table_meta (
        run_id text NOT NULL, table_name text NOT NULL, at timestamptz NOT NULL,
        snapshots integer NOT NULL, metadata_bytes bigint NOT NULL,
        current_snapshot_id bigint, metadata_location text,
        PRIMARY KEY (run_id, table_name, at))""",
    "ALTER TABLE {s}.run_summary ADD COLUMN IF NOT EXISTS n_base_expired integer",
    "ALTER TABLE {s}.run_summary ADD COLUMN IF NOT EXISTS n_history_unresolved integer",
    "ALTER TABLE {s}.run_summary ADD COLUMN IF NOT EXISTS n_errors integer",
    "ALTER TABLE {s}.run_summary ADD COLUMN IF NOT EXISTS wasted_cpu_base_expired_s double precision",
    "ALTER TABLE {s}.run_summary ADD COLUMN IF NOT EXISTS wasted_share double precision",
    "ALTER TABLE {s}.run_summary ADD COLUMN IF NOT EXISTS background_cpu_s double precision",
    "ALTER TABLE {s}.run_summary ADD COLUMN IF NOT EXISTS detail jsonb",
)

OP_COLUMNS = (
    "op_id", "run_id", "table_name", "op_type", "writer_class", "mode", "isolation", "controlled",
    "footprint_requested", "footprint_executed", "files_est", "bytes_est", "sla_deadline",
    "intent_at", "decision", "n_deferrals", "decision_delay_ms", "scope_narrowed", "slot",
    "started_at", "commit_requested_at", "finished_at", "outcome", "attempts", "held_ms", "cpu_ms",
    "run_ms", "bytes_written", "snapshot_id", "error_class", "error_message", "op_key",
    "planning_snapshot_id", "planning_snapshot_at", "last_attempt_at", "outcome_detail",
    "cost_detail", "in_metrics",
)
JSON_COLS = {"outcome_detail", "cost_detail", "params", "policy_params", "detail"}


class DB:
    def __init__(self, dsn: str, schema: str = "stand") -> None:
        self.schema = schema
        self.conn = psycopg.connect(dsn, autocommit=True, row_factory=dict_row)

    def close(self) -> None:
        self.conn.close()

    def migrate(self) -> None:
        for sql in MIGRATIONS:
            self.conn.execute(sql.format(s=self.schema))

    @staticmethod
    def _v(col: str, v: Any) -> Any:
        if col in JSON_COLS and v is not None and not isinstance(v, str):
            return json.dumps(v, default=str)
        return v

    def execute(self, sql: str, params: Sequence[Any] | dict[str, Any] | None = None) -> None:
        self.conn.execute(sql.replace("{s}", self.schema), params)  # type: ignore[arg-type]

    def query(self, sql: str, params: Sequence[Any] | dict[str, Any] | None = None
              ) -> list[dict[str, Any]]:
        cur = self.conn.execute(sql.replace("{s}", self.schema), params)  # type: ignore[arg-type]
        return list(cur.fetchall())

    # ── runs ──
    def insert_run(self, row: dict[str, Any]) -> None:
        cols = list(row)
        self.conn.execute(
            f"INSERT INTO {self.schema}.runs ({', '.join(cols)}) VALUES "
            f"({', '.join(['%s'] * len(cols))}) ON CONFLICT (run_id) DO UPDATE SET "
            + ", ".join(f"{c} = EXCLUDED.{c}" for c in cols if c != "run_id"),
            [self._v(c, row[c]) for c in cols])

    def update_run(self, run_id: str, **fields: Any) -> None:
        sets = ", ".join(f"{k} = %s" for k in fields)
        self.conn.execute(f"UPDATE {self.schema}.runs SET {sets} WHERE run_id = %s",
                          [self._v(k, v) for k, v in fields.items()] + [run_id])

    def delete_run(self, run_id: str) -> None:
        for t in ("ops", "decisions", "table_health", "table_meta", "orphan_scan", "run_summary"):
            self.conn.execute(f"DELETE FROM {self.schema}.{t} WHERE run_id = %s", [run_id])
        self.conn.execute(f"DELETE FROM {self.schema}.runs WHERE run_id = %s", [run_id])

    # ── ops ──
    def upsert_op(self, row: dict[str, Any]) -> None:
        cols = [c for c in OP_COLUMNS if c in row]
        self.conn.execute(
            f"INSERT INTO {self.schema}.ops ({', '.join(cols)}) VALUES "
            f"({', '.join(['%s'] * len(cols))}) ON CONFLICT (op_id) DO UPDATE SET "
            + ", ".join(f"{c} = EXCLUDED.{c}" for c in cols if c != "op_id"),
            [self._v(c, row[c]) for c in cols])

    def insert_many(self, table: str, rows: Iterable[dict[str, Any]]) -> None:
        rows = list(rows)
        if not rows:
            return
        cols = list(rows[0])
        with self.conn.cursor() as cur:
            cur.executemany(
                f"INSERT INTO {self.schema}.{table} ({', '.join(cols)}) VALUES "
                f"({', '.join(['%s'] * len(cols))}) ON CONFLICT DO NOTHING",
                [[self._v(c, r[c]) for c in cols] for r in rows])

    # ── proxy journal ──
    def attempts_for_op(self, op_id: str) -> list[dict[str, Any]]:
        return self.query(
            "SELECT received_at, forwarded_at, responded_at, hold_ms, http_status, base_snapshot_id,"
            " new_snapshot_id, iceberg_op, op_class, summary, error_type, removed_snapshot_ids"
            " FROM {s}.commit_attempts WHERE op_id = %s ORDER BY received_at", [op_id])

    def expire_commits(self, table: str, t0: Any, t1: Any) -> list[dict[str, Any]]:
        """Successful commits that removed snapshots, answered in [t0, t1]."""
        return self.query(
            "SELECT op_id, received_at, responded_at, removed_snapshot_ids FROM {s}.commit_attempts"
            " WHERE table_name = %s AND http_status BETWEEN 200 AND 299"
            " AND removed_snapshot_ids IS NOT NULL AND responded_at >= %s AND responded_at <= %s"
            " ORDER BY responded_at", [table, t0, t1])

    def snapshot_committed_at(self, table: str, snapshot_id: int) -> Any:
        rows = self.query("SELECT committed_at FROM {s}.commit_events WHERE table_name = %s"
                          " AND snapshot_id = %s", [table, snapshot_id])
        return rows[0]["committed_at"] if rows else None

    def children_of(self, table: str, snapshot_id: int) -> list[int]:
        rows = self.query("SELECT snapshot_id FROM {s}.commit_events WHERE table_name = %s"
                          " AND parent_snapshot_id = %s", [table, snapshot_id])
        return [int(r["snapshot_id"]) for r in rows]

    def validation_path(self, table: str, ancestor: int, base: int) -> list[int]:
        """Journal path from the reported last known ancestor to the validation base.

        Empty means the relation is not established; another branch's child is not evidence.
        """
        rows = self.query("SELECT snapshot_id, parent_snapshot_id FROM {s}.commit_events "
                          "WHERE table_name = %s", [table])
        parents = {int(r["snapshot_id"]): r["parent_snapshot_id"] for r in rows}
        path: list[int] = []
        cur: int | None = ancestor
        while cur is not None and cur != base and cur not in path:
            path.append(cur)
            parent = parents.get(cur)
            cur = int(parent) if parent is not None else None
        return path if cur == base else []
