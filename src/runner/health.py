"""Table health samples (FR-RUN-6, tz/06 step 3.7) and history size of the table.

Every `health_sample_interval_s`:
  * table_health — per partition: data files, small data files (< 32 MiB), delete files
    (position/equality deletes and DVs), bytes; from PyIceberg manifests of one metadata version;
  * table_meta — number of snapshots and bytes of the current metadata.json (S3 object size):
    the quantities of the NFR-1 applicability bound (ADR-008, review S2-R4-02).
The last per-partition sample is also the state for the selectors small_files_topn and
delete_files_topn.
"""
from __future__ import annotations

import contextlib
import threading
import time
from datetime import datetime, timezone
from itertools import pairwise
from typing import Any

from runner.db import DB
from runner.env import Env
from runner.ops.executor import load_table_state

SMALL_FILE_BYTES = 32 * 1024 * 1024
UTC = timezone.utc  # noqa: UP017 — runner image is Python 3.10


def s3_object_size(env: Env, location: str) -> int:
    import pyarrow.fs as pafs
    fs = pafs.S3FileSystem(endpoint_override=env.s3_endpoint, scheme="http",
                           access_key=_aws("AWS_ACCESS_KEY_ID"), secret_key=_aws("AWS_SECRET_ACCESS_KEY"),
                           region=_aws("AWS_REGION", "us-east-1"))
    info = fs.get_file_info(location.removeprefix("s3://").removeprefix("s3a://"))
    return int(info.size or 0)


def _aws(name: str, default: str = "") -> str:
    import os
    return os.environ.get(name, default)


class HealthSampler:
    def __init__(self, spark: Any, env: Env, db: DB, run_id: str, table: str, interval_s: float
                 ) -> None:
        self.spark, self.env, self.db = spark, env, db
        self.run_id, self.table, self.interval_s = run_id, table, interval_s
        self._stats: dict[str, dict[str, int]] = {}
        self._stats_at = 0.0
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thr: threading.Thread | None = None
        self.n = 0
        self.errors: list[str] = []
        self.sample_times: list[datetime] = []
        self.metadata_locations: set[str] = set()
        self.max_snapshots = 0
        self.max_metadata_bytes = 0

    def sample(self) -> None:
        # One immutable metadata location ties file statistics and table size to one state.
        # No Spark job can wait behind the workload and backdate a later REST response.
        with self._lock:
            started = datetime.now(UTC)
            st = load_table_state(self.env, self.table)
            observed = datetime.now(UTC)
            stats = inspect_partition_files(self.env, st.metadata_location)
            mb = s3_object_size(self.env, st.metadata_location)
            finished = datetime.now(UTC)
            self.n += 1
            self._stats, self._stats_at = stats, time.monotonic()
            self.sample_times.append(finished)
            self.metadata_locations.add(st.metadata_location)
            provenance = {"sample_started_at": started, "metadata_observed_at": observed,
                          "at": finished, "current_snapshot_id": st.current_snapshot_id,
                          "metadata_location": st.metadata_location}
            self.db.insert_many("table_health", [
                {"run_id": self.run_id, "table_name": self.table, "partition": p,
                 **provenance, **v} for p, v in stats.items()])
            self.max_snapshots = max(self.max_snapshots, len(st.snapshots))
            self.max_metadata_bytes = max(self.max_metadata_bytes, mb)
            self.db.insert_many("table_meta", [{
                "run_id": self.run_id, "table_name": self.table, **provenance,
                "snapshots": len(st.snapshots), "metadata_bytes": mb}])

    def partition_stats(self, max_age_s: float = 30.0) -> dict[str, dict[str, int]]:
        if time.monotonic() - self._stats_at > max_age_s:
            self.sample()
        return dict(self._stats)

    def _loop(self) -> None:
        deadline = (self._stats_at or time.monotonic()) + self.interval_s
        while not self._stop.wait(max(0.0, deadline - time.monotonic())):
            try:
                self.sample()
            except Exception as exc:
                self.errors.append(repr(exc)[:500])
            deadline += self.interval_s

    def start(self) -> None:
        from pyspark import InheritableThread
        self._thr = InheritableThread(target=self._loop, name="health", daemon=True)
        self._thr.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thr is not None:
            with contextlib.suppress(Exception):
                self._thr.join(timeout=120)


def inspect_partition_files(env: Env, metadata_location: str) -> dict[str, dict[str, int]]:
    """Read live manifest entries without submitting work to the measured Spark cluster."""
    from pyiceberg.table import StaticTable
    table = StaticTable.from_metadata(metadata_location, properties={
        "s3.endpoint": f"http://{env.s3_endpoint.removeprefix('http://')}",
        "s3.access-key-id": _aws("AWS_ACCESS_KEY_ID"),
        "s3.secret-access-key": _aws("AWS_SECRET_ACCESS_KEY"),
        "s3.region": _aws("AWS_REGION", "us-east-1"),
    })
    stats: dict[str, dict[str, int]] = {}
    for row in table.inspect.files().to_pylist():
        part = row['partition']
        label = f"dt={part['dt']}/key_bucket={part['key_bucket']}"
        v = stats.setdefault(label, dict(data_files=0, small_files=0, delete_files=0, total_bytes=0))
        size = int(row['file_size_in_bytes'])
        if int(row['content']) == 0:
            v['data_files'] += 1
            v['small_files'] += size < SMALL_FILE_BYTES
        else:
            v['delete_files'] += 1
        v['total_bytes'] += size
    return stats


def health_coverage(times: list[datetime], started: datetime, finished: datetime,
                    interval_s: float) -> dict[str, Any]:
    """Check acquisitions, including both edges. One second is scheduling/IO jitter,
    not a replacement sampling period; a missed 30-second interval still fails.
    """
    points = sorted(times)
    gaps = [(b - a).total_seconds() for a, b in pairwise(points)]
    if points:
        gaps += [max(0.0, (points[0] - started).total_seconds()),
                 max(0.0, (finished - points[-1]).total_seconds())]
    limit = interval_s + 1.0
    return {"ok": bool(points) and max(gaps, default=0.0) <= limit,
            "samples": len(points), "interval_s": interval_s, "jitter_budget_s": 1.0,
            "max_gap_s": max(gaps, default=None),
            "missed_intervals": sum(g > limit for g in gaps), "gaps_s": gaps}


def validate_health_rows(rows: list[dict[str, Any]], mutations: list[dict[str, Any]],
                         started: datetime, finished: datetime, interval_s: float) -> dict[str, Any]:
    """Recompute coverage and causal timestamps from raw DB/export rows."""
    created = {int(m['new_snapshot_id']): m['forwarded_at'] for m in mutations
               if m.get('new_snapshot_id') is not None}
    invalid = []
    for row in rows:
        begin, observed, end = (row.get(k) for k in ('sample_started_at', 'metadata_observed_at', 'at'))
        sid = row.get('current_snapshot_id')
        if (begin is None or observed is None or end is None or not begin <= observed <= end
                or (sid in created and observed < created[sid]) or not row.get('metadata_location')):
            invalid.append(str(row.get('at')))
    coverage = health_coverage([r['at'] for r in rows], started, finished, interval_s)
    return {**coverage, 'ok': coverage['ok'] and not invalid, 'invalid_timestamps': invalid}
