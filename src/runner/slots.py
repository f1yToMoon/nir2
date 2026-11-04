"""Spark session of a run with the slot catalogs (tz/04 §5, tz/06 step 3.3).

Catalogs (all through `Env.catalog_uri`, metadata cache off, run id in `X-CAPL-Run`):
  lake        admin: reset, health, invariants (writer `user`, no slot)
  s0..sN-1    controlled operations (writer `controlled`)
  m0, m1      background maintenance, expire (writer = maintenance.writer_class)
  a0..aK-1    Spark append «agents» of a stream (writer `stream`)

A slot is bound to the operation in the proxy (`PUT /v1/debug/slots/{slot}`) before the
operation starts and released after it: the proxy journals the true class (op_type) and op_id of
each commit. Cost attribution: job group = op_id (tz/04 §5) and job tag = op_id, because Iceberg
actions replace the job group (BaseSparkAction.java:129-130, JobGroupUtils.withJobGroupInfo).
"""
from __future__ import annotations

import contextlib
import queue
import threading
from collections.abc import Iterator
from typing import Any

import requests

from runner.env import REPO, Env

FAIR_FILE = REPO / "infra" / "spark" / "fairscheduler.xml"


def catalog_conf(name: str, env: Env, run_id: str, writer: str, slot: str | None) -> dict[str, str]:
    p = f"spark.sql.catalog.{name}"
    conf = {
        p: "org.apache.iceberg.spark.SparkCatalog",
        f"{p}.type": "rest",
        f"{p}.uri": env.catalog_uri,
        f"{p}.warehouse": env.warehouse,
        f"{p}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        f"{p}.s3.endpoint": env.s3_endpoint,
        f"{p}.s3.path-style-access": "true",
        f"{p}.cache-enabled": "false",
        f"{p}.rest.client.connection-timeout-ms": "10000",
        f"{p}.rest.client.socket-timeout-ms": "120000",
        f"{p}.header.X-CAPL-Writer": writer,
        f"{p}.header.X-CAPL-Run": run_id,
    }
    if slot:
        conf[f"{p}.header.X-CAPL-Slot"] = slot
    return conf


def build_session(env: Env, run_id: str, app_name: str, *, n_slots: int, n_agents: int = 0,
                  maintenance_writer: str = "maintenance", event_log_dir: str | None = None
                  ) -> Any:
    from pyspark.sql import SparkSession

    b = (SparkSession.builder.appName(app_name).master(env.spark_master)
         .config("spark.sql.extensions",
                 "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.defaultCatalog", "lake")
         .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
         .config("spark.driver.memory", env.driver_memory)
         .config("spark.executor.memory", env.executor_memory)
         .config("spark.executor.cores", env.executor_cores)
         .config("spark.cores.max", env.cores_max)
         .config("spark.scheduler.mode", "FAIR")
         .config("spark.scheduler.allocation.file", str(FAIR_FILE))
         .config("spark.ui.enabled", "true")
         .config("spark.ui.port", str(env.spark_ui_port))
         .config("spark.ui.retainedJobs", "20000")
         .config("spark.ui.retainedStages", "40000")
         .config("spark.ui.retainedTasks", "200000")
         .config("spark.sql.shuffle.partitions", "32")
         .config("spark.sql.adaptive.enabled", "true")
         .config("spark.ui.showConsoleProgress", "false"))
    if event_log_dir:
        b = b.config("spark.eventLog.enabled", "true").config("spark.eventLog.dir", event_log_dir)
    else:
        b = b.config("spark.eventLog.enabled", "false")
    confs: dict[str, str] = {}
    confs.update(catalog_conf("lake", env, run_id, "user", None))
    for k in range(n_slots):
        confs.update(catalog_conf(f"s{k}", env, run_id, "controlled", f"s{k}"))
    for k in range(2):
        confs.update(catalog_conf(f"m{k}", env, run_id, maintenance_writer, f"m{k}"))
    for k in range(n_agents):
        confs.update(catalog_conf(f"a{k}", env, run_id, "stream", f"a{k}"))
    for k, v in confs.items():
        b = b.config(k, v)
    return b.getOrCreate()


class SlotPool:
    """Blocking pool of slot names; binds a slot to an operation in the proxy."""

    def __init__(self, names: list[str], env: Env) -> None:
        self.env = env
        self._q: queue.Queue[str] = queue.Queue()
        for n in names:
            self._q.put(n)
        self._lock = threading.Lock()

    def bind(self, slot: str, op_id: str, op_type: str) -> None:
        if self.env.routing != "proxy":
            return
        r = requests.put(f"{self.env.advisor_uri}/v1/debug/slots/{slot}",
                         json={"op_id": op_id, "op_type": op_type}, timeout=10)
        r.raise_for_status()

    def unbind(self, slot: str) -> None:
        if self.env.routing != "proxy":
            return
        with contextlib.suppress(requests.RequestException):
            requests.delete(f"{self.env.advisor_uri}/v1/debug/slots/{slot}", timeout=10)

    @contextlib.contextmanager
    def lease(self, op_id: str, op_type: str, timeout: float | None = None) -> Iterator[str]:
        slot = self._q.get(timeout=timeout)
        try:
            self.bind(slot, op_id, op_type)
            yield slot
        finally:
            self.unbind(slot)
            self._q.put(slot)


@contextlib.contextmanager
def job_scope(spark: Any, op_id: str, description: str, pool: str) -> Iterator[None]:
    """Job group, job tag and FAIR pool of the calling thread (pinned thread mode)."""
    sc = spark.sparkContext
    sc.setJobGroup(op_id, description, interruptOnCancel=True)
    sc.setLocalProperty("spark.scheduler.pool", pool)
    sc.addJobTag(op_id)
    try:
        yield
    finally:
        with contextlib.suppress(Exception):
            sc.removeJobTag(op_id)
        sc.setLocalProperty("spark.scheduler.pool", None)
        with contextlib.suppress(Exception):
            sc.clearJobGroup()
