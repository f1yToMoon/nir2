"""Table health samples (FR-RUN-6, tz/06 step 3.7) and history size of the table.

Every `health_sample_interval_s`:
  * table_health — per partition: data files, small data files (< 32 MiB), delete files
    (position/equality deletes and DVs), bytes; from Spark `<table>.files` (pool `admin`);
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
from typing import Any

from runner.db import DB
from runner.env import Env
from runner.ops.executor import load_table_state
from runner.slots import job_scope

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
        self.max_snapshots = 0
        self.max_metadata_bytes = 0

    def sample(self) -> None:
        with self._lock:
            at = datetime.now(UTC)
            self.n += 1
            with job_scope(self.spark, f"health-{self.run_id}-{self.n}", "health", "admin"):
                rows = self.spark.sql(f"""
                    SELECT concat('dt=', CAST(partition.dt AS STRING), '/key_bucket=',
                                  CAST(partition.key_bucket AS STRING)) AS part,
                           sum(CASE WHEN content = 0 THEN 1 ELSE 0 END) AS data_files,
                           sum(CASE WHEN content = 0 AND file_size_in_bytes < {SMALL_FILE_BYTES}
                                    THEN 1 ELSE 0 END) AS small_files,
                           sum(CASE WHEN content <> 0 THEN 1 ELSE 0 END) AS delete_files,
                           sum(file_size_in_bytes) AS total_bytes
                    FROM lake.{self.table}.files GROUP BY 1""").collect()
            stats = {r["part"]: {"data_files": int(r["data_files"]),
                                 "small_files": int(r["small_files"]),
                                 "delete_files": int(r["delete_files"]),
                                 "total_bytes": int(r["total_bytes"] or 0)} for r in rows}
            self._stats, self._stats_at = stats, time.monotonic()
            self.db.insert_many("table_health", [
                {"run_id": self.run_id, "table_name": self.table, "at": at, "partition": p, **v}
                for p, v in stats.items()])
            st = load_table_state(self.env, self.table)
            mb = s3_object_size(self.env, st.metadata_location)
            self.max_snapshots = max(self.max_snapshots, len(st.snapshots))
            self.max_metadata_bytes = max(self.max_metadata_bytes, mb)
            self.db.insert_many("table_meta", [{
                "run_id": self.run_id, "table_name": self.table, "at": at,
                "snapshots": len(st.snapshots), "metadata_bytes": mb,
                "current_snapshot_id": st.current_snapshot_id,
                "metadata_location": st.metadata_location}])

    def partition_stats(self, max_age_s: float = 30.0) -> dict[str, dict[str, int]]:
        if time.monotonic() - self._stats_at > max_age_s:
            self.sample()
        return dict(self._stats)

    def _loop(self) -> None:
        while not self._stop.is_set():
            t = time.monotonic()
            try:
                self.sample()
            except Exception as exc:          # a failed sample must not stop the run
                self.errors.append(repr(exc)[:500])
            self._stop.wait(max(0.0, self.interval_s - (time.monotonic() - t)))

    def start(self) -> None:
        from pyspark import InheritableThread
        self._thr = InheritableThread(target=self._loop, name="health", daemon=True)
        self._thr.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thr is not None:
            with contextlib.suppress(Exception):
                self._thr.join(timeout=120)
