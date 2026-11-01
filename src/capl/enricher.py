"""Reconstruction of commit footprints from manifests (FR-PROXY-5).

After a successful commit the proxy enqueues a job; workers read the manifest list of the new
snapshot and the manifests added by it (added_snapshot_id = new snapshot), take the entries
ADDED/DELETED by this snapshot and aggregate them by partition. Reading is blocking I/O and runs
in a thread; the commit itself is never delayed.
"""
from __future__ import annotations

import asyncio
import datetime as dt
import time
from collections.abc import Callable, Iterable
from concurrent.futures import Executor
from dataclasses import dataclass, field
from typing import IO, Any, Protocol

import fastavro
import structlog

from capl import metrics
from capl.telemetry import TelemetrySink, ts

log = structlog.get_logger("capl.enricher")

STATUS_ADDED, STATUS_DELETED = 1, 2
CONTENT_DATA = 0


def partition_to_str(partition: dict[str, Any] | None) -> str:
    """{'dt': date(2026,11,1), 'key_bucket': 3} -> 'dt=2026-11-01/key_bucket=3' (field order kept)."""
    if not partition:
        return ""
    parts = []
    for name, value in partition.items():
        if value is None:
            v = "null"
        elif isinstance(value, dt.date):      # also datetime
            v = value.isoformat()
        elif isinstance(value, bool):
            v = str(value).lower()
        else:
            v = str(value)
        parts.append(f"{name}={v}")
    return "/".join(parts)


@dataclass
class PartitionFootprint:
    partition: str
    added_data_files: int = 0
    deleted_data_files: int = 0
    added_delete_files: int = 0
    removed_delete_files: int = 0
    added_bytes: int = 0
    added_records: int = 0
    deleted_file_paths: list[str] = field(default_factory=list)


@dataclass
class Footprint:
    snapshot_id: int
    partitions: dict[str, PartitionFootprint]
    manifests_read: int

    def totals(self) -> dict[str, int]:
        ps = self.partitions.values()
        return {
            "added_data_files": sum(p.added_data_files for p in ps),
            "deleted_data_files": sum(p.deleted_data_files for p in ps),
            "added_delete_files": sum(p.added_delete_files for p in ps),
            "removed_delete_files": sum(p.removed_delete_files for p in ps),
            "added_bytes": sum(p.added_bytes for p in ps),
        }


Opener = Callable[[str], IO[bytes]]


def pyiceberg_opener(properties: dict[str, str]) -> Opener:
    """Open s3://... through PyIceberg FileIO (PyArrow S3)."""
    from pyiceberg.io.pyarrow import PyArrowFileIO

    io = PyArrowFileIO(properties)

    def _open(path: str) -> IO[bytes]:
        return io.new_input(path).open()  # type: ignore[return-value]

    return _open


_OPENERS: dict[tuple[tuple[str, str], ...], Opener] = {}


@dataclass(frozen=True)
class S3FootprintReader:
    """Picklable footprint reader for a process pool: Avro decoding (fastavro) is CPU-bound and
    holds the GIL; in a separate process it does not delay the proxy's event loop (S2-R1-02).
    The FileIO is created once per worker process."""
    properties: tuple[tuple[str, str], ...]

    def __call__(self, manifest_list: str, snapshot_id: int, keep_deleted_paths: bool) -> Footprint:
        opener = _OPENERS.get(self.properties)
        if opener is None:
            opener = _OPENERS.setdefault(self.properties, pyiceberg_opener(dict(self.properties)))
        return read_footprint(opener, manifest_list, snapshot_id, keep_deleted_paths)


Reader = Callable[[str, int, bool], Footprint]


def empty_footprint(manifest_list: str, snapshot_id: int, keep_deleted_paths: bool) -> Footprint:
    """Reader that reads nothing (debug: separates the cost of reading manifests from the
    cost of the executor round trip and bookkeeping in overhead experiments)."""
    return Footprint(snapshot_id, {}, 0)


def _records(opener: Opener, path: str) -> Iterable[dict[str, Any]]:
    with opener(path) as f:
        yield from fastavro.reader(f)  # type: ignore[misc]


def read_footprint(opener: Opener, manifest_list: str, snapshot_id: int,
                   keep_deleted_paths: bool) -> Footprint:
    """Footprint of snapshot `snapshot_id` from its manifest list."""
    parts: dict[str, PartitionFootprint] = {}
    n_manifests = 0
    for m in _records(opener, manifest_list):
        if m.get("added_snapshot_id") != snapshot_id:
            continue          # manifest carried over from an earlier snapshot
        n_manifests += 1
        for e in _records(opener, str(m["manifest_path"])):
            status = e.get("status")
            # v2+: ADDED entries inherit snapshot_id from the manifest (null in the file)
            esid = e.get("snapshot_id")
            if status not in (STATUS_ADDED, STATUS_DELETED) or (esid is not None and esid != snapshot_id):
                continue
            df = e["data_file"]
            key = partition_to_str(df.get("partition"))
            p = parts.setdefault(key, PartitionFootprint(key))
            is_data = df.get("content", CONTENT_DATA) == CONTENT_DATA
            if status == STATUS_ADDED:
                if is_data:
                    p.added_data_files += 1
                    p.added_records += int(df.get("record_count") or 0)
                else:
                    p.added_delete_files += 1
                p.added_bytes += int(df.get("file_size_in_bytes") or 0)
            elif is_data:
                p.deleted_data_files += 1
                if keep_deleted_paths:
                    p.deleted_file_paths.append(str(df["file_path"]))
            else:
                p.removed_delete_files += 1
    return Footprint(snapshot_id, parts, n_manifests)


@dataclass
class EnrichJob:
    run_id: str | None
    table: str
    snapshot_id: int
    parent_snapshot_id: int | None
    sequence_number: int | None
    manifest_list: str
    iceberg_op: str | None
    op_class: str
    op_class_source: str             # op_id (slot binding) | body (commit_parser.classify)
    op_id: str | None
    writer_class: str | None
    writer_id: str | None
    committed_at_ms: float           # time of the catalog's 200 response


class EventSink(Protocol):
    def add_commit_event(self, event: dict[str, Any]) -> None: ...


class Enricher:
    def __init__(self, reader: Reader, telemetry: TelemetrySink, state: EventSink | None,
                 workers: int = 4, queue_max: int = 10000,
                 read_deleted_paths_for: Iterable[str] = (), executor: Executor | None = None) -> None:
        """reader(manifest_list, snapshot_id, keep_deleted_paths) runs in `executor`
        (a process pool in the service; None = default thread pool)."""
        self._reader = reader
        self._executor = executor
        self._telemetry = telemetry
        self._state = state
        self._n_workers = workers
        self._queue: asyncio.Queue[EnrichJob] = asyncio.Queue(maxsize=queue_max)
        self._tasks: list[asyncio.Task[None]] = []
        self._deleted_paths_for = set(read_deleted_paths_for)
        self.published = 0
        self.failed = 0
        self.read_manifests = True    # debug: False = empty_footprint in the same executor

    def submit(self, job: EnrichJob) -> bool:
        try:
            self._queue.put_nowait(job)
        except asyncio.QueueFull:
            metrics.ENRICH_TOTAL.labels(result="dropped").inc()
            return False
        metrics.ENRICH_QUEUE.set(self._queue.qsize())
        return True

    def qsize(self) -> int:
        return self._queue.qsize()

    async def start(self) -> None:
        self._tasks = [asyncio.create_task(self._worker(), name=f"enricher-{i}")
                       for i in range(self._n_workers)]

    async def stop(self) -> None:
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        if self._executor is not None:
            self._executor.shutdown(wait=False, cancel_futures=True)

    async def join(self) -> None:
        await self._queue.join()

    async def _worker(self) -> None:
        while True:
            job = await self._queue.get()
            try:
                await self._process(job)
            except Exception as exc:     # never let one bad snapshot stop the worker
                self.failed += 1
                metrics.ENRICH_TOTAL.labels(result="error").inc()
                log.warning("enrich_failed", table=job.table, snapshot_id=job.snapshot_id,
                            error=repr(exc))
            finally:
                self._queue.task_done()
                metrics.ENRICH_QUEUE.set(self._queue.qsize())

    async def _process(self, job: EnrichJob) -> None:
        keep_paths = job.op_class in self._deleted_paths_for
        reader = self._reader if self.read_manifests else empty_footprint
        fp = await asyncio.get_running_loop().run_in_executor(
            self._executor, reader, job.manifest_list, job.snapshot_id, keep_paths)
        now_ms = time.time() * 1000
        totals = fp.totals()
        event = {
            "run_id": job.run_id, "table": job.table, "snapshot_id": job.snapshot_id,
            "parent_snapshot_id": job.parent_snapshot_id, "sequence_number": job.sequence_number,
            "committed_at": job.committed_at_ms / 1000.0, "iceberg_op": job.iceberg_op,
            "op_class": job.op_class, "op_class_source": job.op_class_source, "op_id": job.op_id,
            "writer_class": job.writer_class or "unknown", "writer_id": job.writer_id,
            "totals": totals,
            "partitions": [
                {"partition": p.partition, "added_data_files": p.added_data_files,
                 "deleted_data_files": p.deleted_data_files,
                 "added_delete_files": p.added_delete_files,
                 "removed_delete_files": p.removed_delete_files, "added_bytes": p.added_bytes,
                 **({"deleted_file_paths": p.deleted_file_paths} if keep_paths else {})}
                for p in fp.partitions.values()
            ],
        }
        self._telemetry.write("commit_events", {
            "run_id": job.run_id, "table_name": job.table, "snapshot_id": job.snapshot_id,
            "parent_snapshot_id": job.parent_snapshot_id, "sequence_number": job.sequence_number,
            "committed_at": ts(job.committed_at_ms), "iceberg_op": job.iceberg_op,
            "op_class": job.op_class, "op_class_source": job.op_class_source, "op_id": job.op_id,
            "writer_class": job.writer_class,
            "writer_id": job.writer_id, **totals, "n_partitions": len(fp.partitions),
            "enriched_at": ts(now_ms),
        })
        for p in fp.partitions.values():
            self._telemetry.write("commit_partitions", {
                "table_name": job.table, "snapshot_id": job.snapshot_id, "partition": p.partition,
                "added_data_files": p.added_data_files, "deleted_data_files": p.deleted_data_files,
                "added_delete_files": p.added_delete_files,
                "removed_delete_files": p.removed_delete_files, "added_bytes": p.added_bytes,
                "deleted_file_paths": p.deleted_file_paths if keep_paths else None,
            })
        if self._state is not None:
            self._state.add_commit_event(event)
        self.published += 1
        metrics.ENRICH_TOTAL.labels(result="ok").inc()
        metrics.ENRICH_LAG.observe(max(0.0, (now_ms - job.committed_at_ms) / 1000.0))
