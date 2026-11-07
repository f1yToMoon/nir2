"""Exact per-event source production → successful catalog response latency.

Diagnostic observer uses manifest and Parquet reads, never Spark. Start before writers;
register their EventLogs before starting them. Stop only after writer flush/drain. Source
append snapshots are sampled immediately to stay inside snapshot retention. Missing events
or unreadable snapshots make the result incomplete and suppress the published percentile.
"""
from __future__ import annotations

import math
import os
import threading
import time
from collections.abc import Callable, Iterable
from typing import IO, TYPE_CHECKING, Any

import fastavro
import numpy as np

from runner.env import Env
from runner.ops.executor import load_table_state
from runner.streaming import EventLog

if TYPE_CHECKING:
    from runner.db import DB



Opener = Callable[[str], IO[bytes]]


def _records(opener: Opener, path: str) -> Iterable[dict[str, Any]]:
    with opener(path) as fh:
        yield from fastavro.reader(fh)


def pyiceberg_opener(properties: dict[str, str]) -> Opener:
    from pyiceberg.io.pyarrow import PyArrowFileIO
    io = PyArrowFileIO(properties)
    return lambda path: io.new_input(path).open()


def added_data_files(opener: Opener, manifest_list: str, snapshot_id: int) -> Iterable[str]:
    """Only files introduced by this snapshot (not carried manifests/entries/deletes)."""
    for manifest in _records(opener, manifest_list):
        if manifest.get("added_snapshot_id") != snapshot_id:
            continue
        for entry in _records(opener, str(manifest["manifest_path"])):
            if entry.get("status") != 1 or entry.get("snapshot_id") not in (None, snapshot_id):
                continue
            data = entry["data_file"]
            if data.get("content", 0) == 0:
                yield str(data["file_path"])


def event_ids(opener: Opener, path: str) -> Iterable[int]:
    import pyarrow.parquet as pq
    with opener(path) as fh:
        for batch in pq.ParquetFile(fh).iter_batches(columns=["event_id"], batch_size=65536):
            yield from batch.column(0).to_pylist()


class StreamFreshness:
    def __init__(self, env: Env, run_id: str, table: str, interval_s: float = 1.0) -> None:
        self.env, self.run_id, self.table, self.interval_s = env, run_id, table, interval_s
        self.logs: list[EventLog] = []
        self.errors: list[str] = []
        self._done: set[int] = set()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self.cpu_s = 0.0
        self.n_snapshots = 0
        self._unresolved: dict[int, str] = {}

    def register(self, events: EventLog) -> None:
        """Register each KafkaFeed.events / SparkAgents.events before starting observer."""
        if self._thread is not None:
            raise RuntimeError("register logs before start")
        self.logs.append(events)

    def record(self, ids: Iterable[int], committed_at: float) -> bool:
        """Return false for source rows not yet registered (agent append/log race)."""
        complete = True
        for event_id in ids:
            found = False
            for log in self.logs:
                with log.lock:
                    i = log.positions.get(event_id)
                    if i is None:
                        continue
                    found = True
                    if committed_at < log.generated_at[i]:
                        raise ValueError(f"negative freshness for event {event_id}")
                    old = log.committed_at[i]
                    log.committed_at[i] = committed_at if math.isnan(old) else min(old, committed_at)
                break
            complete &= found
        return complete

    def sample(self, db: DB, opener: Opener) -> None:
        rows = db.query(
            "SELECT new_snapshot_id, responded_at FROM {s}.commit_attempts WHERE run_id = %s"
            " AND table_name = %s AND writer_class = 'stream' AND iceberg_op = 'append'"
            " AND http_status BETWEEN 200 AND 299 AND responded_at IS NOT NULL"
            " AND new_snapshot_id IS NOT NULL ORDER BY responded_at", [self.run_id, self.table])
        pending = [r for r in rows if int(r["new_snapshot_id"]) not in self._done]
        if not pending:
            return
        metadata = load_table_state(self.env, self.table).metadata
        snapshots = {int(s["snapshot-id"]): s for s in metadata.get("snapshots", [])}
        for row in pending:
            sid = int(row["new_snapshot_id"])
            try:
                snap = snapshots[sid]
                if snap.get("summary", {}).get("operation") != "append":
                    raise ValueError("source snapshot is not append")
                at = row["responded_at"].timestamp()
                complete = True
                for path in added_data_files(opener, snap["manifest-list"], sid):
                    complete &= self.record(event_ids(opener, path), at)
                if complete:
                    self._done.add(sid)
                    self.n_snapshots += 1
                    self._unresolved.pop(sid, None)
                else:
                    self._unresolved[sid] = "source event not registered"
            except Exception as exc:
                self._unresolved[sid] = repr(exc)[:500]

    def _loop(self) -> None:
        from runner.db import DB

        db = DB(self.env.pg_dsn, self.env.pg_schema)
        try:
            opener = pyiceberg_opener({
                "s3.endpoint": self.env.s3_endpoint,
                "s3.access-key-id": os.environ.get("AWS_ACCESS_KEY_ID", ""),
                "s3.secret-access-key": os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
                "s3.region": os.environ.get("AWS_REGION", "us-east-1")})
            while True:
                cpu0 = time.thread_time()
                try:
                    self.sample(db, opener)
                except Exception as exc:
                    self.errors.append(repr(exc)[:500])
                self.cpu_s += time.thread_time() - cpu0
                if self._stop.wait(self.interval_s):
                    # Writers have drained: final scan covers commits since the last poll.
                    deadline = time.monotonic() + 30
                    while True:
                        cpu0 = time.thread_time()
                        self.sample(db, opener)
                        self.cpu_s += time.thread_time() - cpu0
                        if self.result()["complete"] or time.monotonic() >= deadline:
                            return
                        time.sleep(0.5)
        except Exception as exc:
            self.errors.append(repr(exc)[:500])
        finally:
            db.close()

    def start(self) -> None:
        self._thread = threading.Thread(target=self._loop, name="stream-freshness", daemon=True)
        self._thread.start()

    def stop(self, timeout_s: float = 120) -> dict[str, Any]:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=timeout_s)
            if self._thread.is_alive():
                self.errors.append("observer did not stop within timeout")
        return self.result()

    def result(self) -> dict[str, Any]:
        chunks: list[Any] = []
        n_expected = 0
        for log in self.logs:
            with log.lock:
                n_expected += len(log.ids)
                latency = np.asarray(log.committed_at) - np.asarray(log.generated_at)
                chunks.append(latency[~np.isnan(latency)])
        samples = np.concatenate(chunks) if chunks else np.array([], dtype=float)
        missing = n_expected - len(samples)
        complete = missing == 0 and not self.errors and not self._unresolved
        return {"complete": complete, "n_expected": n_expected, "n_samples": len(samples),
                "missing": missing, "p95_s": float(np.percentile(samples, 95))
                if len(samples) and complete else None, "errors": self.errors[:20],
                "unresolved_snapshots": dict(self._unresolved), "n_snapshots": self.n_snapshots,
                "observer_cpu_s": self.cpu_s,
                "clock": "producer UTC time.time → successful commit_attempts.responded_at"}
