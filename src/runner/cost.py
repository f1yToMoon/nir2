"""Cost of an operation from the Spark REST API of the driver (FR-RUN-5, tz/06 step 3.5).

Jobs of an operation are found by its job tag (= op_id): Iceberg actions replace the job group
(BaseSparkAction.java:129-130), tags are kept and inherited by the threads of the action. For each
job, all attempts of its stages are summed: executorRunTime (ms), executorCpuTime (ns → ms),
outputBytes. Data that is not final yet (job running, stage not complete) is re-read with
back-off; `complete=False` is recorded when the data is still not final after the deadline.
"""
from __future__ import annotations

import time
from dataclasses import asdict, dataclass, field
from typing import Any

import requests


@dataclass
class Cost:
    cpu_ms: int = 0
    run_ms: int = 0
    output_bytes: int = 0
    jobs: list[int] = field(default_factory=list)
    stages: int = 0
    complete: bool = True
    job_groups: list[str] = field(default_factory=list)

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


class CostReader:
    def __init__(self, spark: Any, ui_port: int = 4040) -> None:
        self.sc = spark.sparkContext
        self.app = self.sc.applicationId
        self.base = f"http://localhost:{ui_port}/api/v1/applications/{self.app}"
        self._tracker = self.sc._jsc.sc().statusTracker()

    def job_ids(self, tag: str) -> list[int]:
        return sorted(int(j) for j in self._tracker.getJobIdsForTag(tag))

    def _get(self, path: str) -> Any:
        r = requests.get(self.base + path, timeout=10)
        r.raise_for_status()
        return r.json()

    def read(self, tag: str, deadline_s: float = 60.0) -> Cost:
        t_end = time.monotonic() + deadline_s
        delay = 0.5
        while True:
            cost = self._read_once(tag)
            if cost.complete or time.monotonic() > t_end:
                return cost
            time.sleep(delay)
            delay = min(delay * 2, 5.0)

    def _read_once(self, tag: str) -> Cost:
        c = Cost(jobs=self.job_ids(tag))
        groups: set[str] = set()
        seen_stages: set[int] = set()
        for jid in c.jobs:
            try:
                job = self._get(f"/jobs/{jid}")
            except requests.RequestException:
                c.complete = False
                continue
            if job.get("status") == "RUNNING" or job.get("status") == "UNKNOWN":
                c.complete = False
            if job.get("jobGroup"):
                groups.add(str(job["jobGroup"]))
            for sid in job.get("stageIds", []):
                if sid in seen_stages:
                    continue
                seen_stages.add(sid)
                try:
                    attempts = self._get(f"/stages/{sid}")
                except requests.HTTPError as exc:
                    if exc.response is not None and exc.response.status_code == 404:
                        continue              # skipped stage never submitted
                    c.complete = False
                    continue
                except requests.RequestException:
                    c.complete = False
                    continue
                for a in attempts:
                    st = a.get("status")
                    if st in ("ACTIVE", "PENDING"):
                        c.complete = False
                    if st == "SKIPPED":
                        continue
                    c.stages += 1
                    c.run_ms += int(a.get("executorRunTime", 0) or 0)
                    c.cpu_ms += int(a.get("executorCpuTime", 0) or 0) // 1_000_000
                    c.output_bytes += int(a.get("outputBytes", 0) or 0)
        c.job_groups = sorted(groups)
        return c

    def app_totals(self) -> dict[str, int]:
        """Sum over all stages of the application (sanity bound for Σ cpu_ms of operations)."""
        cpu = run = 0
        n = 0
        for a in self._get("/stages?status=complete") + self._get("/stages?status=failed"):
            cpu += int(a.get("executorCpuTime", 0) or 0) // 1_000_000
            run += int(a.get("executorRunTime", 0) or 0)
            n += 1
        return {"cpu_ms": cpu, "run_ms": run, "stages": n}
