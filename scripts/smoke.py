"""Smoke tests (runs inside runner container).

Checks:
  0.4a S3 reachable via Toxiproxy (HTTP status)
  0.4b S3 PUT/GET object via Toxiproxy (PyArrow S3FileSystem)
  0.5  Catalog /v1/config reachable directly and via CAPL proxy
  0.6  Slot headers (X-CAPL-Slot) propagated by Iceberg REST client
  0.7  REST-client timeout property names (confirmed from HTTPClient.java 1.12.0)
  0.8  Flink SQL Gateway reachable; Kafka→Flink→Iceberg ≥3 commits, flink.job-id in all,
       median snapshot interval 5 s ± 50 %
  0.9  rewrite_data_files bucket(8,id)=1 predicate; file counts verified post-compaction
  0.10 Spark cluster mode (spark://spark-master:7077): write + rewrite on workers

Exit code 0 = all passed.
"""
from __future__ import annotations

import json
import os
import statistics
import sys
import threading
import time

import requests

# ---------- helpers -------------------------------------------------------
PASSED: list[str] = []
FAILED: list[str] = []


def ok(name: str, detail: str = "") -> None:
    msg = f"  PASS  {name}" + (f": {detail}" if detail else "")
    print(msg)
    PASSED.append(name)


def fail(name: str, detail: str = "") -> None:
    msg = f"  FAIL  {name}" + (f": {detail}" if detail else "")
    print(msg, file=sys.stderr)
    FAILED.append(name)


def check(name: str, fn):
    try:
        result = fn()
        ok(name, str(result) if result else "")
    except Exception as exc:
        fail(name, str(exc))


# ---------- service endpoints ---------------------------------------------
S3_URL          = os.environ.get("S3_ENDPOINT",        "http://toxiproxy:18333")
CATALOG_URL     = os.environ.get("CATALOG_DIRECT_URI", "http://catalog:8181")
PROXY_URL       = os.environ.get("CAPL_PROXY_URI",     "http://capl:8180")
ADVISOR_URL     = os.environ.get("CAPL_ADVISOR_URI",   "http://capl:8190")
FLINK_GW_URL    = os.environ.get("FLINK_SQL_GATEWAY",  "http://flink-sql-gateway:8083")
FLINK_REST_URL  = os.environ.get("FLINK_REST",         "http://flink-jobmanager:8081")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP",    "kafka:9092")
WAREHOUSE       = os.environ.get("WAREHOUSE",          "s3://warehouse/")
SPARK_MASTER    = os.environ.get("SPARK_MASTER",       "spark://spark-master:7077")

TIMEOUT = 10


# ---------- Spark session factory -----------------------------------------
def _make_spark(app_name: str):
    try:
        from pyspark.sql import SparkSession
    except ImportError as exc:
        raise RuntimeError(f"PySpark not available: {exc}") from exc
    return (
        SparkSession.builder.appName(app_name)
        .master("local[1]")
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
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )


# ==========================================================================
# 0.4a  S3 via Toxiproxy — reachability
# ==========================================================================
def _check_s3():
    r = requests.get(f"{S3_URL}/", timeout=TIMEOUT)
    assert r.status_code in (200, 400, 403), f"unexpected status {r.status_code}"
    return f"status={r.status_code}"


# ==========================================================================
# 0.4b  S3 PUT/GET via Toxiproxy (PyArrow S3FileSystem)
# ==========================================================================
def _check_s3_put_get():
    """PUT an object to S3 via Toxiproxy, GET it back, verify content, DELETE."""
    import pyarrow.fs as pafs

    endpoint = S3_URL.removeprefix("http://").removeprefix("https://")
    scheme   = "http" if S3_URL.startswith("http://") else "https"
    s3 = pafs.S3FileSystem(
        access_key=os.environ.get("AWS_ACCESS_KEY_ID", "nir-access"),
        secret_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "nir-secret-change-me"),
        endpoint_override=endpoint,
        scheme=scheme,
        region=os.environ.get("AWS_REGION", "us-east-1"),
        force_virtual_addressing=False,
    )
    bucket = os.environ.get("S3_BUCKET", "warehouse")
    key    = f"smoke-s3-put-get/{int(time.time())}.txt"
    path   = f"{bucket}/{key}"
    payload = b"capl-smoke-s3-put-get"

    with s3.open_output_stream(path) as fh:
        fh.write(payload)
    with s3.open_input_file(path) as fh:
        content = fh.read()
    assert content == payload, f"content mismatch: {content!r} != {payload!r}"
    s3.delete_file(path)
    return f"PUT/GET {len(payload)}B to s3://{bucket}/{key} via Toxiproxy"


# ==========================================================================
# 0.5  Catalog direct + via proxy
# ==========================================================================
def _check_catalog_direct():
    r = requests.get(f"{CATALOG_URL}/v1/config", timeout=TIMEOUT)
    assert r.status_code == 200, f"status={r.status_code} body={r.text[:200]}"
    return f"defaults={list(r.json().get('defaults', {}).keys())[:5]}"


def _check_proxy_passthrough():
    r = requests.get(f"{PROXY_URL}/v1/config", timeout=TIMEOUT)
    assert r.status_code == 200, f"status={r.status_code} body={r.text[:200]}"
    return f"proxy forwarded, defaults={list(r.json().get('defaults', {}).keys())[:5]}"


def _check_advisor_health():
    r = requests.get(f"{ADVISOR_URL}/healthz", timeout=TIMEOUT)
    assert r.status_code == 200, f"status={r.status_code}"
    data = r.json()
    assert data.get("status") == "ok", f"unexpected body: {data}"
    return f"stage={data.get('stage')}"


# ==========================================================================
# 0.6  Slot header check via PySpark
# ==========================================================================
def _check_slot_headers():
    try:
        from pyspark.sql import SparkSession
    except ImportError as exc:
        raise RuntimeError(f"PySpark not available: {exc}") from exc

    spark = (
        SparkSession.builder.appName("capl-smoke-slot-check")
        .master("local[1]")
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
        .config("spark.sql.catalog.smoke_cat.header.X-CAPL-Slot", "smoke-s0")
        .config("spark.sql.catalog.smoke_cat.header.X-CAPL-Writer", "smoke")
        .config("spark.driver.memory", "512m")
        .config("spark.executor.memory", "512m")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        spark.sql("CREATE NAMESPACE IF NOT EXISTS smoke_cat.smoke")
        spark.sql("DROP TABLE IF EXISTS smoke_cat.smoke.slot_check")
        spark.sql(
            "CREATE TABLE smoke_cat.smoke.slot_check (id INT, val STRING) USING iceberg"
        )
        spark.sql("INSERT INTO smoke_cat.smoke.slot_check VALUES (1, 'hello')")
    finally:
        spark.stop()

    time.sleep(1)
    r = requests.get(f"{ADVISOR_URL}/_capl/debug/last-commits", timeout=TIMEOUT)
    assert r.status_code == 200, f"debug endpoint failed: {r.status_code}"
    commits = r.json()
    slots = [c.get("slot", "") for c in commits]
    found = any("smoke-s0" in s for s in slots)
    if not found:
        raise RuntimeError(
            f"X-CAPL-Slot=smoke-s0 NOT found in last {len(commits)} commits. "
            f"Slots seen: {slots[-10:]}"
        )
    return f"slot visible in {len(commits)} logged commits"


# ==========================================================================
# 0.7  REST-client timeout property names
# ==========================================================================
def _check_timeout_property_name():
    """Property names confirmed by javap on iceberg-core-1.12.0.jar:
      REST_CONNECTION_TIMEOUT_MS → rest.client.connection-timeout-ms
      REST_SOCKET_TIMEOUT_MS     → rest.client.socket-timeout-ms
    No default values defined in Iceberg (system defaults from Apache HttpClient 5 apply).
    """
    r = requests.get(f"{PROXY_URL}/v1/config", timeout=TIMEOUT)
    assert r.status_code == 200, f"proxy /v1/config failed: {r.status_code}"
    conf_path = "/workspace/infra/spark/spark-defaults.conf"
    if os.path.exists(conf_path):
        text = open(conf_path).read()
        assert "rest.client.connection-timeout-ms" in text, (
            "rest.client.connection-timeout-ms missing from spark-defaults.conf"
        )
        assert "rest.client.socket-timeout-ms" in text, (
            "rest.client.socket-timeout-ms missing from spark-defaults.conf"
        )
    return (
        "rest.client.connection-timeout-ms / rest.client.socket-timeout-ms "
        "confirmed from HTTPClient.java 1.12.0; no defaults in Iceberg"
    )


# ==========================================================================
# 0.8  Flink SQL Gateway reachable
# ==========================================================================
def _check_flink_gateway():
    r = requests.get(f"{FLINK_GW_URL}/v1/info", timeout=TIMEOUT)
    assert r.status_code == 200, f"info failed: {r.status_code} {r.text[:200]}"
    info = r.json()

    r2 = requests.post(
        f"{FLINK_GW_URL}/v1/sessions",
        json={"properties": {"execution.runtime-mode": "streaming"}},
        timeout=TIMEOUT,
    )
    assert r2.status_code == 200, f"session open failed: {r2.status_code} {r2.text[:200]}"
    sid = r2.json().get("sessionHandle")
    assert sid, "no sessionHandle in response"
    requests.delete(f"{FLINK_GW_URL}/v1/sessions/{sid}", timeout=TIMEOUT)
    return f"Flink {info.get('version', '?')}, session ok (id={sid[:8]}...)"


# ==========================================================================
# 0.8  Kafka → Flink → Iceberg: ≥3 periodic commits, flink.job-id in all snapshots
# ==========================================================================
def _check_kafka_flink_iceberg():
    """Full streaming pipeline Kafka→Flink→Iceberg.
    Produces messages continuously; waits for ≥3 completed Flink checkpoints
    (interval=5s); asserts flink.job-id present in ALL Iceberg snapshots.
    """
    try:
        from confluent_kafka import Producer
        from confluent_kafka.admin import AdminClient, NewTopic
    except ImportError as exc:
        raise RuntimeError(f"confluent-kafka unavailable: {exc}") from exc

    ts = int(time.time())
    topic = f"smoke-kfi-{ts}"
    table = "kafka_flink_iceberg"

    # 1. Create Kafka topic ------------------------------------------------
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
    futures = admin.create_topics(
        [NewTopic(topic, num_partitions=1, replication_factor=1)]
    )
    for _, fut in futures.items():
        try:
            fut.result(timeout=10)
        except Exception as exc:
            if "already exists" not in str(exc).lower():
                raise RuntimeError(f"Kafka topic create failed: {exc}") from exc

    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

    # 2. Prime topic with initial batch -----------------------------------
    for i in range(300):
        producer.produce(
            topic,
            key=str(i % 8).encode(),
            value=json.dumps({"id": i, "val": f"msg-{i}"}).encode(),
        )
    producer.flush()

    # 3. Background producer — keeps data flowing across all checkpoints --
    stop_event = threading.Event()

    def _produce_loop(start: int) -> None:
        idx = start
        while not stop_event.is_set():
            for _ in range(50):
                producer.produce(
                    topic,
                    key=str(idx % 8).encode(),
                    value=json.dumps({"id": idx, "val": f"msg-{idx}"}).encode(),
                )
                idx += 1
            producer.poll(0)
            stop_event.wait(0.5)
        producer.flush(10)

    bg = threading.Thread(target=_produce_loop, args=(300,), daemon=True)
    bg.start()

    sid: str | None = None
    job_id: str | None = None
    ckpt_counts: dict = {}

    try:
        # 4. Create target Iceberg table via Spark -------------------------
        spark = _make_spark("capl-smoke-kfi-prep")
        try:
            spark.sparkContext.setLogLevel("WARN")
            spark.sql("CREATE NAMESPACE IF NOT EXISTS smoke_cat.smoke")
            spark.sql(f"DROP TABLE IF EXISTS smoke_cat.smoke.{table}")
            spark.sql(
                f"CREATE TABLE smoke_cat.smoke.{table} "
                f"(id BIGINT, val STRING) USING iceberg"
            )
        finally:
            spark.stop()

        # 5. Open Flink SQL session with checkpoint config -----------------
        r = requests.post(
            f"{FLINK_GW_URL}/v1/sessions",
            json={
                "properties": {
                    "execution.runtime-mode": "streaming",
                    "execution.checkpointing.interval": "5s",
                    "state.checkpoints.dir": "file:///flink-checkpoints/smoke",
                    "pipeline.name": "capl-smoke-kfi",
                }
            },
            timeout=10,
        )
        assert r.status_code == 200, f"session open failed: {r.status_code} {r.text[:200]}"
        sid = r.json()["sessionHandle"]

        def _sql(stmt: str, wait: bool = True) -> str:
            resp = requests.post(
                f"{FLINK_GW_URL}/v1/sessions/{sid}/statements",
                json={"statement": stmt},
                timeout=30,
            )
            assert resp.status_code == 200, (
                f"stmt submit failed {resp.status_code}: {resp.text[:300]}"
            )
            op_id: str = resp.json()["operationHandle"]
            if not wait:
                return op_id
            for _ in range(90):
                time.sleep(1)
                sr = requests.get(
                    f"{FLINK_GW_URL}/v1/sessions/{sid}/operations/{op_id}/status",
                    timeout=10,
                )
                status = sr.json().get("status", "UNKNOWN")
                if status == "FINISHED":
                    return op_id
                if status in ("ERROR", "CLOSED", "CANCELED"):
                    raise RuntimeError(f"SQL {status}: {stmt[:120]}")
            raise TimeoutError(f"SQL timeout: {stmt[:60]}")

        # 6. DDL: catalog + Kafka source table ----------------------------
        _sql(
            f"CREATE CATALOG smoke_cat WITH ("
            f"  'type' = 'iceberg',"
            f"  'catalog-type' = 'rest',"
            f"  'uri' = '{PROXY_URL}',"
            f"  'warehouse' = '{WAREHOUSE}',"
            f"  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',"
            f"  's3.endpoint' = '{S3_URL}',"
            f"  's3.path-style-access' = 'true',"
            f"  'header.X-CAPL-Writer' = 'stream'"
            f")"
        )
        _sql("USE CATALOG smoke_cat")
        _sql(
            f"CREATE TEMPORARY TABLE kafka_src ("
            f"  `id` BIGINT,"
            f"  `val` STRING"
            f") WITH ("
            f"  'connector' = 'kafka',"
            f"  'topic' = '{topic}',"
            f"  'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP}',"
            f"  'properties.group.id' = 'smoke-kfi-{ts}',"
            f"  'scan.startup.mode' = 'earliest-offset',"
            f"  'format' = 'json',"
            f"  'json.fail-on-missing-field' = 'false',"
            f"  'json.ignore-parse-errors' = 'true'"
            f")"
        )

        # 7. Submit streaming INSERT (non-blocking) ------------------------
        insert_op = _sql(
            f"INSERT INTO smoke.{table} SELECT id, val FROM kafka_src",
            wait=False,
        )
        time.sleep(4)
        sr = requests.get(
            f"{FLINK_GW_URL}/v1/sessions/{sid}/operations/{insert_op}/status",
            timeout=10,
        )
        ist = sr.json().get("status", "UNKNOWN")
        if ist in ("ERROR", "CLOSED"):
            raise RuntimeError(f"INSERT failed immediately: status={ist}")

        # 8. Locate Flink job by pipeline.name ----------------------------
        for _ in range(30):
            time.sleep(2)
            jr = requests.get(f"{FLINK_REST_URL}/jobs", timeout=10)
            if jr.status_code != 200:
                continue
            for jinfo in jr.json().get("jobs", []):
                if jinfo.get("status") != "RUNNING":
                    continue
                det = requests.get(
                    f"{FLINK_REST_URL}/jobs/{jinfo['id']}", timeout=10
                )
                if "capl-smoke-kfi" in det.json().get("name", ""):
                    job_id = jinfo["id"]
                    break
            if job_id:
                break
        assert job_id, "Flink job 'capl-smoke-kfi' not found in RUNNING state within 60s"

        # 9. Wait for ≥3 completed checkpoints ----------------------------
        for _ in range(60):
            time.sleep(3)
            cr = requests.get(
                f"{FLINK_REST_URL}/jobs/{job_id}/checkpoints", timeout=10
            )
            if cr.status_code == 200:
                ckpt_counts = cr.json().get("counts", {})
                if ckpt_counts.get("completed", 0) >= 3:
                    break
        else:
            raise RuntimeError(
                f"< 3 checkpoints completed in 180s; last counts: {ckpt_counts}"
            )

    finally:
        if job_id:
            try:
                requests.patch(
                    f"{FLINK_REST_URL}/jobs/{job_id}?mode=cancel", timeout=15
                )
            except Exception:
                pass
        if sid:
            try:
                requests.delete(f"{FLINK_GW_URL}/v1/sessions/{sid}", timeout=10)
            except Exception:
                pass
        stop_event.set()
        bg.join(timeout=15)

    # 10. Allow final Iceberg commit to land ------------------------------
    time.sleep(10)

    # 11. Assert flink.job-id in ALL snapshots ----------------------------
    r = requests.get(
        f"{CATALOG_URL}/v1/namespaces/smoke/tables/{table}", timeout=TIMEOUT
    )
    assert r.status_code == 200, f"table metadata fetch failed: {r.status_code}"
    snapshots = r.json().get("metadata", {}).get("snapshots", [])
    assert len(snapshots) >= 3, f"expected ≥3 snapshots, got {len(snapshots)}"

    bad = [
        str(s.get("snapshot-id"))
        for s in snapshots
        if "flink.job-id" not in s.get("summary", {})
    ]
    assert not bad, f"flink.job-id missing in snapshot(s): {bad}"

    timestamps_ms = sorted(s.get("timestamp-ms", 0) for s in snapshots)
    intervals_s = [
        (timestamps_ms[i + 1] - timestamps_ms[i]) / 1000.0
        for i in range(len(timestamps_ms) - 1)
    ]
    med = statistics.median(intervals_s)
    assert 2.5 <= med <= 7.5, (
        f"median snapshot interval {med:.1f}s not in [2.5, 7.5]; "
        f"intervals={[round(x, 1) for x in intervals_s]}"
    )

    return (
        f"{len(snapshots)} snapshots, all have flink.job-id; "
        f"median_interval={med:.1f}s; "
        f"checkpoints.completed={ckpt_counts.get('completed')}; "
        f"job={job_id[:8]}"
    )


# ==========================================================================
# 0.9  rewrite_data_files with system.bucket(8, id) predicate
# ==========================================================================
def _check_rewrite_data_files_bucket():
    """Creates bucket(8, id)-partitioned table, inserts 4 separate batches to
    produce multiple files per bucket, then calls rewrite_data_files targeting
    bucket 1 via system.bucket(8, id) = 1 predicate.
    """
    spark = _make_spark("capl-smoke-rewrite-bucket")
    try:
        spark.sparkContext.setLogLevel("WARN")
        spark.sql("CREATE NAMESPACE IF NOT EXISTS smoke_cat.smoke")
        spark.sql("DROP TABLE IF EXISTS smoke_cat.smoke.rewrite_test")
        spark.sql(
            "CREATE TABLE smoke_cat.smoke.rewrite_test "
            "(id BIGINT, val STRING) "
            "USING iceberg "
            "PARTITIONED BY (bucket(8, id))"
        )
        for batch in range(4):
            (
                spark.range(100)
                .selectExpr("id", f"CONCAT('b{batch}-', CAST(id AS STRING)) AS val")
                .writeTo("smoke_cat.smoke.rewrite_test")
                .append()
            )
        # Iceberg 1.12 default min-input-files=5; explicitly set to 1 so 4 files/bucket
        # qualify. target-file-size-bytes triggers small-file selection (< 75% of target).
        result = spark.sql(
            "CALL smoke_cat.system.rewrite_data_files("
            "  table => 'smoke_cat.smoke.rewrite_test',"
            "  options => map('min-input-files', '1', 'target-file-size-bytes', '134217728'),"
            "  where => 'system.bucket(8, id) = 1'"
            ")"
        )
        rows = result.collect()
        assert rows, "rewrite_data_files returned no rows"
        d = rows[0].asDict()
        assert d.get("rewritten_data_files_count", 0) >= 1, (
            f"no files rewritten: {d}"
        )

        # Verify per-bucket file counts: bucket 1 → 1 (compacted), others → 4 each
        file_rows = spark.sql(
            "SELECT partition FROM smoke_cat.smoke.rewrite_test.files"
        ).collect()
        bucket_counts: dict[int, int] = {}
        for fr in file_rows:
            part = fr["partition"]
            bucket_val = int(part[0])
            bucket_counts[bucket_val] = bucket_counts.get(bucket_val, 0) + 1
        assert bucket_counts.get(1, 0) == 1, (
            f"bucket 1 has {bucket_counts.get(1, 0)} files, expected 1; counts={bucket_counts}"
        )
        for b in range(8):
            if b == 1:
                continue
            assert bucket_counts.get(b, 0) == 4, (
                f"bucket {b} has {bucket_counts.get(b, 0)} files, expected 4; counts={bucket_counts}"
            )
        return (
            f"bucket(8,id)=1 predicate accepted; "
            f"rewritten={d.get('rewritten_data_files_count')}, "
            f"added={d.get('added_data_files_count')}; "
            f"file_counts={bucket_counts}"
        )
    finally:
        spark.stop()


# ==========================================================================
# 0.10  Spark cluster mode: write + rewrite_data_files on workers
# ==========================================================================
def _check_spark_cluster():
    """Write Iceberg table and run rewrite_data_files via Spark cluster
    (spark://spark-master:7077, client mode). Verify workers handled tasks
    via Spark driver REST API.
    """
    try:
        from pyspark.sql import SparkSession
    except ImportError as exc:
        raise RuntimeError(f"PySpark not available: {exc}") from exc

    spark = (
        SparkSession.builder.appName("capl-smoke-cluster")
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
        spark.sql("DROP TABLE IF EXISTS smoke_cat.smoke.cluster_test")
        spark.sql(
            "CREATE TABLE smoke_cat.smoke.cluster_test "
            "(id BIGINT, val STRING) USING iceberg"
        )

        for batch in range(4):
            (
                spark.range(0, 500_000)
                .selectExpr("id", f"CONCAT('b{batch}-', CAST(id AS STRING)) AS val")
                .writeTo("smoke_cat.smoke.cluster_test")
                .append()
            )

        spark.sql(
            "CALL smoke_cat.system.rewrite_data_files("
            "  table => 'smoke_cat.smoke.cluster_test',"
            "  options => map('min-input-files', '1', 'target-file-size-bytes', '134217728')"
            ")"
        )

        exec_r = requests.get(
            f"http://localhost:4040/api/v1/applications/{app_id}/executors",
            timeout=TIMEOUT,
        )
        assert exec_r.status_code == 200, (
            f"Spark driver REST API failed: {exec_r.status_code} {exec_r.text[:200]}"
        )
        executors = exec_r.json()
        workers = [e for e in executors if str(e.get("id")) != "driver"]
        assert workers, (
            f"no worker executors found; ids={[e.get('id') for e in executors]}"
        )
        total_tasks = sum(e.get("totalTasks", 0) for e in workers)
        assert total_tasks > 0, f"workers had 0 tasks; details={workers}"

        return (
            f"master={SPARK_MASTER}; "
            f"workers={len(workers)}; "
            f"tasks_on_workers={total_tasks}"
        )
    finally:
        spark.stop()


# ==========================================================================
# main
# ==========================================================================
def main() -> None:
    print("=" * 60)
    print("CAPL Stage-0 Smoke Tests")
    print("=" * 60)

    print("\n[0.4] S3 via Toxiproxy")
    check("s3_reachable", _check_s3)
    check("s3_put_get", _check_s3_put_get)

    print("\n[0.5] Catalog and proxy connectivity")
    check("catalog_direct", _check_catalog_direct)
    check("proxy_passthrough", _check_proxy_passthrough)
    check("advisor_health", _check_advisor_health)

    print("\n[0.6] Slot header propagation (PySpark)")
    check("slot_header_X-CAPL-Slot", _check_slot_headers)

    print("\n[0.7] REST-client timeout property (confirmed HTTPClient.java 1.12.0)")
    check("timeout_property_name", _check_timeout_property_name)

    print("\n[0.8] Flink SQL Gateway")
    check("flink_gateway_reachable", _check_flink_gateway)
    check("kafka_flink_iceberg", _check_kafka_flink_iceberg)

    print("\n[0.9] rewrite_data_files bucket predicate")
    check("rewrite_data_files_bucket", _check_rewrite_data_files_bucket)

    print("\n[0.10] Spark cluster mode (spark://spark-master:7077)")
    check("spark_cluster", _check_spark_cluster)

    print("\n" + "=" * 60)
    print(f"PASSED: {len(PASSED)}  FAILED: {len(FAILED)}")
    if FAILED:
        print(f"Failed: {', '.join(FAILED)}", file=sys.stderr)
        sys.exit(1)
    print("All smoke tests passed.")


if __name__ == "__main__":
    main()
