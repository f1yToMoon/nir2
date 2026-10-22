#!/usr/bin/env python3
"""Concurrent load for peak memory measurement.

Runs Kafka→Flink→Iceberg streaming + heavy Spark cluster write (5M rows) concurrently.
Phase 1: create target Iceberg table with local Spark, then stop session.
Phase 2: start Flink job in background thread (HTTP only, no Spark) +
         write 5M rows via Spark cluster in main thread simultaneously.
"""
from __future__ import annotations

import json
import os
import sys
import threading
import time

import requests

sys.path.insert(0, "/workspace")

from scripts.smoke import (
    CATALOG_URL,
    FLINK_GW_URL,
    FLINK_REST_URL,
    KAFKA_BOOTSTRAP,
    PROXY_URL,
    S3_URL,
    SPARK_MASTER,
    TIMEOUT,
    WAREHOUSE,
    _make_spark,
)

# ── shared state ──────────────────────────────────────────────────────────────
_flink_result: dict[str, object] = {}
_flink_error: str | None = None

TABLE_FLINK = "peak_flink"
TABLE_SPARK = "peak_spark"


# ── phase 1 helpers ───────────────────────────────────────────────────────────

def _setup_flink_table() -> None:
    spark = _make_spark("peak-setup")
    try:
        spark.sparkContext.setLogLevel("WARN")
        spark.sql("CREATE NAMESPACE IF NOT EXISTS smoke_cat.smoke")
        spark.sql(f"DROP TABLE IF EXISTS smoke_cat.smoke.{TABLE_FLINK}")
        spark.sql(
            f"CREATE TABLE smoke_cat.smoke.{TABLE_FLINK} "
            f"(id BIGINT, val STRING) USING iceberg"
        )
    finally:
        spark.stop()


# ── phase 2: Flink thread (HTTP only) ─────────────────────────────────────────

def _run_flink_job(topic: str) -> None:
    global _flink_error
    from confluent_kafka import Producer  # type: ignore[import]
    from confluent_kafka.admin import AdminClient, NewTopic  # type: ignore[import]

    ts = int(time.time())
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
    futs = admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])
    for _, fut in futs.items():
        try:
            fut.result(timeout=10)
        except Exception as exc:
            if "already exists" not in str(exc).lower():
                _flink_error = f"topic create: {exc}"
                return

    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    stop_evt = threading.Event()

    def _produce(start: int) -> None:
        idx = start
        while not stop_evt.is_set():
            for _ in range(50):
                producer.produce(
                    topic,
                    key=str(idx % 8).encode(),
                    value=json.dumps({"id": idx, "val": f"msg-{idx}"}).encode(),
                )
                idx += 1
            producer.poll(0)
            stop_evt.wait(0.5)
        producer.flush(10)

    # Prime
    for i in range(300):
        producer.produce(topic, key=str(i % 8).encode(),
                         value=json.dumps({"id": i, "val": f"msg-{i}"}).encode())
    producer.flush()

    bg = threading.Thread(target=_produce, args=(300,), daemon=True)
    bg.start()

    sid: str | None = None
    job_id: str | None = None

    try:
        r = requests.post(
            f"{FLINK_GW_URL}/v1/sessions",
            json={"properties": {
                "execution.runtime-mode": "streaming",
                "execution.checkpointing.interval": "5s",
                "state.checkpoints.dir": "file:///flink-checkpoints/peak",
                "pipeline.name": "capl-peak-kfi",
            }},
            timeout=10,
        )
        assert r.status_code == 200
        sid = r.json()["sessionHandle"]

        def _sql(stmt: str, wait: bool = True) -> str:
            resp = requests.post(
                f"{FLINK_GW_URL}/v1/sessions/{sid}/statements",
                json={"statement": stmt}, timeout=30,
            )
            assert resp.status_code == 200, f"{resp.status_code}: {resp.text[:200]}"
            op_id: str = resp.json()["operationHandle"]
            if not wait:
                return op_id
            for _ in range(90):
                time.sleep(1)
                sr = requests.get(
                    f"{FLINK_GW_URL}/v1/sessions/{sid}/operations/{op_id}/status",
                    timeout=10,
                )
                st = sr.json().get("status", "UNKNOWN")
                if st == "FINISHED":
                    return op_id
                if st in ("ERROR", "CLOSED", "CANCELED"):
                    raise RuntimeError(f"SQL {st}: {stmt[:120]}")
            raise TimeoutError(f"SQL timeout: {stmt[:60]}")

        _sql(
            f"CREATE CATALOG smoke_cat WITH ("
            f"  'type'='iceberg','catalog-type'='rest','uri'='{PROXY_URL}',"
            f"  'warehouse'='{WAREHOUSE}',"
            f"  'io-impl'='org.apache.iceberg.aws.s3.S3FileIO',"
            f"  's3.endpoint'='{S3_URL}','s3.path-style-access'='true',"
            f"  'header.X-CAPL-Writer'='stream'"
            f")"
        )
        _sql("USE CATALOG smoke_cat")
        _sql(
            f"CREATE TEMPORARY TABLE kafka_src_peak ("
            f"  `id` BIGINT, `val` STRING"
            f") WITH ("
            f"  'connector'='kafka','topic'='{topic}',"
            f"  'properties.bootstrap.servers'='{KAFKA_BOOTSTRAP}',"
            f"  'properties.group.id'='peak-kfi-{ts}',"
            f"  'scan.startup.mode'='earliest-offset',"
            f"  'format'='json','json.fail-on-missing-field'='false',"
            f"  'json.ignore-parse-errors'='true'"
            f")"
        )
        insert_op = _sql(
            f"INSERT INTO smoke.{TABLE_FLINK} SELECT id, val FROM kafka_src_peak",
            wait=False,
        )
        time.sleep(4)

        for _ in range(30):
            time.sleep(2)
            jr = requests.get(f"{FLINK_REST_URL}/jobs", timeout=10)
            if jr.status_code != 200:
                continue
            for jinfo in jr.json().get("jobs", []):
                if jinfo.get("status") != "RUNNING":
                    continue
                det = requests.get(f"{FLINK_REST_URL}/jobs/{jinfo['id']}", timeout=10)
                if "capl-peak-kfi" in det.json().get("name", ""):
                    job_id = jinfo["id"]
                    break
            if job_id:
                break

        ckpt_counts: dict = {}
        for _ in range(60):
            time.sleep(3)
            cr = requests.get(f"{FLINK_REST_URL}/jobs/{job_id}/checkpoints", timeout=10)
            if cr.status_code == 200:
                ckpt_counts = cr.json().get("counts", {})
                if ckpt_counts.get("completed", 0) >= 3:
                    break

        _flink_result.update({"checkpoints": ckpt_counts, "job_id": job_id})
    except Exception as exc:
        _flink_error = str(exc)
    finally:
        if job_id:
            try:
                requests.patch(f"{FLINK_REST_URL}/jobs/{job_id}?mode=cancel", timeout=15)
            except Exception:
                pass
        if sid:
            try:
                requests.delete(f"{FLINK_GW_URL}/v1/sessions/{sid}", timeout=10)
            except Exception:
                pass
        stop_evt.set()
        bg.join(timeout=15)


# ── phase 2: Spark heavy write ─────────────────────────────────────────────────

def _run_spark_heavy() -> str:
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.appName("capl-peak-spark")
        .master(SPARK_MASTER)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.smoke_cat", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.smoke_cat.type", "rest")
        .config("spark.sql.catalog.smoke_cat.uri", PROXY_URL)
        .config("spark.sql.catalog.smoke_cat.warehouse", WAREHOUSE)
        .config(
            "spark.sql.catalog.smoke_cat.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO",
        )
        .config("spark.sql.catalog.smoke_cat.s3.endpoint", S3_URL)
        .config("spark.sql.catalog.smoke_cat.s3.path-style-access", "true")
        .config("spark.sql.catalog.smoke_cat.cache-enabled", "false")
        .config("spark.driver.memory", "512m")
        .config("spark.executor.memory", "512m")
        .config("spark.ui.enabled", "true")
        .config("spark.ui.port", "4040")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    try:
        spark.sparkContext.setLogLevel("WARN")
        app_id = spark.sparkContext.applicationId
        spark.sql("CREATE NAMESPACE IF NOT EXISTS smoke_cat.smoke")
        spark.sql(f"DROP TABLE IF EXISTS smoke_cat.smoke.{TABLE_SPARK}")
        spark.sql(
            f"CREATE TABLE smoke_cat.smoke.{TABLE_SPARK} "
            f"(id BIGINT, val STRING) USING iceberg"
        )
        for batch in range(4):
            (
                spark.range(0, 1_250_000)
                .selectExpr("id", f"CONCAT('b{batch}-', CAST(id AS STRING)) AS val")
                .writeTo(f"smoke_cat.smoke.{TABLE_SPARK}")
                .append()
            )
        spark.sql(
            f"CALL smoke_cat.system.rewrite_data_files("
            f"  table => 'smoke_cat.smoke.{TABLE_SPARK}',"
            f"  options => map('min-input-files', '1', 'target-file-size-bytes', '134217728')"
            f")"
        )
        exec_r = requests.get(
            f"http://localhost:4040/api/v1/applications/{app_id}/executors",
            timeout=TIMEOUT,
        )
        executors = exec_r.json() if exec_r.status_code == 200 else []
        workers = [e for e in executors if str(e.get("id")) != "driver"]
        total_tasks = sum(e.get("totalTasks", 0) for e in workers)
        return f"5M rows written; workers={len(workers)}; tasks={total_tasks}"
    finally:
        spark.stop()


# ── main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    ts = int(time.time())
    topic = f"peak-kfi-{ts}"

    print("Phase 1: creating Iceberg target table (local Spark)...")
    _setup_flink_table()
    print("  table created, local Spark stopped.")

    print("Phase 2: starting Flink job + Spark heavy write concurrently...")
    flink_thread = threading.Thread(
        target=_run_flink_job, args=(topic,), name="flink-load", daemon=True
    )
    flink_thread.start()

    # Give Flink SQL session setup a head start before Spark competes for S3/catalog
    time.sleep(15)

    try:
        spark_result = _run_spark_heavy()
        print(f"  Spark done: {spark_result}")
    except Exception as exc:
        print(f"  Spark ERROR: {exc}", file=sys.stderr)
        spark_result = f"ERROR: {exc}"

    flink_thread.join(timeout=600)

    if _flink_error:
        print(f"  Flink ERROR: {_flink_error}", file=sys.stderr)
    else:
        print(f"  Flink done: checkpoints={_flink_result.get('checkpoints')}")

    print("Phase 2 complete.")
    sys.exit(1 if _flink_error or "ERROR" in spark_result else 0)


if __name__ == "__main__":
    main()
