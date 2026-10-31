"""Asynchronous batched writes of proxy/enricher telemetry to Postgres (asyncpg).

Writes never block or fail a proxied request: rows are queued with put_nowait, a full queue
drops the row (counted in capl_telemetry_dropped_total), a failed batch is retried and then
dropped (counted as well).
"""
from __future__ import annotations

import asyncio
import contextlib
import json
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol

import structlog

from capl import metrics

log = structlog.get_logger("capl.telemetry")

COLUMNS: dict[str, tuple[str, ...]] = {
    "commit_attempts": (
        "run_id", "op_id", "slot", "writer_class", "table_name", "idempotency_key",
        "received_at", "forwarded_at", "responded_at", "hold_ms", "hold_reason", "http_status",
        "base_snapshot_id", "new_snapshot_id", "sequence_number", "iceberg_op", "op_class",
        "op_class_body", "op_class_ambiguity", "op_class_source", "summary", "error_type",
    ),
    "commit_events": (
        "run_id", "table_name", "snapshot_id", "parent_snapshot_id", "sequence_number",
        "committed_at", "iceberg_op", "op_class", "op_class_source", "op_id", "writer_class",
        "writer_id",
        "added_data_files", "deleted_data_files", "added_delete_files", "added_bytes",
        "n_partitions", "enriched_at",
    ),
    "commit_partitions": (
        "table_name", "snapshot_id", "partition", "added_data_files", "deleted_data_files",
        "added_delete_files", "added_bytes", "deleted_file_paths",
    ),
    "client_reports": ("run_id", "table_name", "received_at", "report_type", "report"),
}
JSONB = {"summary", "report"}
# columns added after the first deployment of the DDL (stage 2, ADR-007); idempotent
MIGRATIONS = (
    "ALTER TABLE {s}.commit_attempts ADD COLUMN IF NOT EXISTS op_class_body text",
    "ALTER TABLE {s}.commit_attempts ADD COLUMN IF NOT EXISTS op_class_ambiguity text",
    "ALTER TABLE {s}.commit_attempts ADD COLUMN IF NOT EXISTS op_class_source text",
    "ALTER TABLE {s}.commit_events ADD COLUMN IF NOT EXISTS op_class_source text",
)
# idempotent re-insert of an enriched snapshot
ON_CONFLICT = {
    "commit_events": "ON CONFLICT (table_name, snapshot_id) DO NOTHING",
    "commit_partitions": "ON CONFLICT (table_name, snapshot_id, partition) DO NOTHING",
}


def ts(epoch_ms: float | None) -> datetime | None:
    return None if epoch_ms is None else datetime.fromtimestamp(epoch_ms / 1000.0, tz=UTC)


class TelemetrySink(Protocol):
    def write(self, table: str, row: dict[str, Any]) -> None: ...
    async def start(self) -> None: ...
    async def stop(self) -> None: ...


class NullTelemetry:
    """Used when no DSN is configured (unit tests, proxy without Postgres)."""

    def __init__(self) -> None:
        self.rows: dict[str, list[dict[str, Any]]] = {t: [] for t in COLUMNS}
        self.keep = False

    def write(self, table: str, row: dict[str, Any]) -> None:
        if self.keep:
            self.rows[table].append(row)

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None


@dataclass
class _Item:
    table: str
    row: dict[str, Any]


class PgTelemetry:
    def __init__(self, dsn: str, schema: str, batch_size: int = 200, flush_interval_s: float = 0.5,
                 queue_max: int = 100000, retries: int = 3) -> None:
        self.dsn = dsn
        self.schema = schema
        self.batch_size = batch_size
        self.flush_interval_s = flush_interval_s
        self.retries = retries
        self._queue: asyncio.Queue[_Item] = asyncio.Queue(maxsize=queue_max)
        self._pool: Any = None
        self._task: asyncio.Task[None] | None = None
        self._sql = {t: self._insert_sql(t) for t in COLUMNS}

    def _insert_sql(self, table: str) -> str:
        cols = COLUMNS[table]
        ph = ", ".join(f"${i + 1}" for i in range(len(cols)))
        return (f'INSERT INTO "{self.schema}".{table} ({", ".join(cols)}) VALUES ({ph}) '
                f'{ON_CONFLICT.get(table, "")}')

    def write(self, table: str, row: dict[str, Any]) -> None:
        try:
            self._queue.put_nowait(_Item(table, row))
        except asyncio.QueueFull:
            metrics.TELEMETRY_DROPPED.labels(table=table, reason="queue_full").inc()
        metrics.TELEMETRY_QUEUE.set(self._queue.qsize())

    async def start(self) -> None:
        import asyncpg

        self._pool = await asyncpg.create_pool(self.dsn, min_size=1, max_size=4)
        async with self._pool.acquire() as conn:
            for stmt in MIGRATIONS:
                await conn.execute(stmt.format(s=f'"{self.schema}"'))
        self._task = asyncio.create_task(self._loop(), name="telemetry-writer")

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
        await self._drain()
        if self._pool is not None:
            await self._pool.close()

    async def flush(self) -> None:
        """Write everything queued so far (used by tests and at run finish)."""
        await self._drain()

    async def _loop(self) -> None:
        while True:
            await asyncio.sleep(self.flush_interval_s)
            await self._drain()

    async def _drain(self) -> None:
        while not self._queue.empty():
            batch: dict[str, list[dict[str, Any]]] = {}
            n = 0
            while n < self.batch_size and not self._queue.empty():
                it = self._queue.get_nowait()
                batch.setdefault(it.table, []).append(it.row)
                n += 1
            for table, rows in batch.items():
                await self._write_batch(table, rows)
        metrics.TELEMETRY_QUEUE.set(self._queue.qsize())

    def _values(self, table: str, rows: Sequence[dict[str, Any]]) -> list[tuple[Any, ...]]:
        out = []
        for r in rows:
            out.append(tuple(json.dumps(r.get(c)) if c in JSONB and r.get(c) is not None else r.get(c)
                             for c in COLUMNS[table]))
        return out

    async def _write_batch(self, table: str, rows: list[dict[str, Any]]) -> None:
        values = self._values(table, rows)
        for attempt in range(1, self.retries + 1):
            try:
                async with self._pool.acquire() as conn:
                    await conn.executemany(self._sql[table], values)
                metrics.TELEMETRY_ROWS.labels(table=table).inc(len(rows))
                return
            except Exception as exc:
                log.warning("telemetry_write_failed", table=table, rows=len(rows), attempt=attempt,
                            error=repr(exc))
                await asyncio.sleep(0.2 * attempt)
        metrics.TELEMETRY_DROPPED.labels(table=table, reason="write_failed").inc(len(rows))
