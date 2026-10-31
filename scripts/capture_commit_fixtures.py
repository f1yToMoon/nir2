"""Capture real Iceberg REST commit bodies through the CAPL proxy as unit-test fixtures (stage 2).

Runs inside the runner container (stand up, full profile for Flink):
    docker compose ... exec -T runner python3 -m scripts.capture_commit_fixtures

Every operation runs on its own table; the commit bodies are taken from the proxy debug log
(PUT /_capl/debug/keep-bodies) as the bytes the proxy received. The expected classification of a
fixture is the operation the script executed (ground truth), not the parser output.
Output: tests/fixtures/commits/<name>.json
"""
from __future__ import annotations

import json
import os
import subprocess
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import requests

PROXY_URL = os.environ.get("CAPL_PROXY_URI", "http://capl:8180")
ADV_URL = os.environ.get("CAPL_ADVISOR_URI", "http://capl:8190")
WAREHOUSE = os.environ.get("WAREHOUSE", "s3://warehouse/")
S3_URL = os.environ.get("S3_ENDPOINT", "http://toxiproxy:18333")
SPARK_MASTER = os.environ.get("SPARK_MASTER", "spark://spark-master:7077")
FLINK_GW = os.environ.get("FLINK_SQL_GATEWAY", "http://flink-sql-gateway:8083")
OUT = Path(os.environ.get("FIXTURE_DIR", "/workspace/tests/fixtures/commits"))
NS = "fx"
SLOT = "fixtures"
CAT = "fxc"
RUN = f"fx-{int(time.time())}"


def git_sha() -> str:
    if os.environ.get("GIT_SHA"):
        return os.environ["GIT_SHA"]
    try:
        return subprocess.run(["git", "-C", "/workspace", "rev-parse", "HEAD"], capture_output=True,
                              text=True, check=True).stdout.strip()
    except Exception:
        return "unknown"


def proxy_commits(since_ms: int, table: str | None) -> list[dict[str, Any]]:
    entries = requests.get(f"{ADV_URL}/_capl/debug/last-commits", timeout=10).json()
    out = []
    for e in entries:
        if e["ts"] < since_ms or "body" not in e:
            continue
        if table is not None and not e["path"].endswith(f"/tables/{table}"):
            continue
        out.append(e)
    return out


def save(name: str, entry: dict[str, Any], expected: dict[str, Any], source: str, operation: str,
         fmt: int | None, note: str = "") -> None:
    assert 200 <= entry["status"] < 300, f"{name}: status {entry['status']}"
    doc = {
        "name": name, "source": source, "operation": operation, "format_version": fmt,
        "path": entry["path"], "kind": entry["kind"],
        "headers": {"X-CAPL-Slot": entry["slot"], "X-CAPL-Writer": entry["writer"]},
        "status": entry["status"], "expected": expected, "note": note,
        "captured_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "git_sha": git_sha(),
        "iceberg_client": "1.12.0", "body": entry["body"],
    }
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / f"{name}.json").write_text(json.dumps(doc, indent=1, ensure_ascii=False) + "\n")
    print(f"saved {name}: {entry['path']}")


# ---------------------------------------------------------------- spark

def spark_session() -> Any:
    from pyspark.sql import SparkSession

    pre = f"spark.sql.catalog.{CAT}"
    b = (SparkSession.builder.appName("capture-commit-fixtures").master(SPARK_MASTER)
         .config("spark.sql.extensions",
                 "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.cores.max", "2").config("spark.sql.shuffle.partitions", "2")
         .config("spark.ui.enabled", "false")
         .config(pre, "org.apache.iceberg.spark.SparkCatalog").config(f"{pre}.type", "rest")
         .config(f"{pre}.uri", PROXY_URL).config(f"{pre}.warehouse", WAREHOUSE)
         .config(f"{pre}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
         .config(f"{pre}.s3.endpoint", S3_URL).config(f"{pre}.s3.path-style-access", "true")
         .config(f"{pre}.cache-enabled", "false")
         .config(f"{pre}.header.X-CAPL-Slot", SLOT).config(f"{pre}.header.X-CAPL-Writer", "test")
         .config(f"{pre}.header.X-CAPL-Run", RUN))
    return b.getOrCreate()


def make_table(spark: Any, name: str, fmt: int, mode: str = "copy-on-write", rows: int = 40,
               files: int = 4, id_type: str = "BIGINT") -> str:
    full = f"{CAT}.{NS}.{name}"
    spark.sql(f"DROP TABLE IF EXISTS {full} PURGE")
    spark.sql(f"""CREATE TABLE {full} (id {id_type}, bkt INT, val STRING) USING iceberg
                  PARTITIONED BY (bkt)
                  TBLPROPERTIES ('format-version'='{fmt}', 'write.delete.mode'='{mode}',
                                 'write.update.mode'='{mode}', 'write.merge.mode'='{mode}',
                                 'write.distribution-mode'='none')""")
    per = rows // files
    for f in range(files):
        spark.sql(f"INSERT INTO {full} SELECT CAST(id AS {id_type}), CAST(id % 2 AS INT), 'v' "
                  f"FROM range({f * per}, {(f + 1) * per})")
    return full


def run_op(name: str, table: str, action: Callable[[], None], expected: dict[str, Any], source: str,
           operation: str, fmt: int | None, pick: str = "last", note: str = "") -> None:
    t0 = int(time.time() * 1000)
    action()
    commits = [e for e in proxy_commits(t0, table) if e["status"] == 200]
    assert commits, f"{name}: no successful commit seen by the proxy"
    save(name, commits[-1] if pick == "last" else commits[0], expected, source, operation, fmt, note)


def capture_spark() -> None:
    spark = spark_session()
    jvm = spark.sparkContext._jvm
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CAT}.{NS}")
    sql = spark.sql

    def exp(op_class: str, iceberg_op: str | None, has_deletes: bool = False, true: str | None = None,
            amb: str | None = None) -> dict[str, Any]:
        # op_class/ambiguity: what the body shows; true_class: class of the executed operation
        # (conflict_matrix.yaml op_classes), independent of the parser
        return {"op_class": op_class, "iceberg_op": iceberg_op, "has_deletes": has_deletes,
                "true_class": true or op_class, "ambiguity": amb,
                "is_flink": False}

    for fmt in (2, 3):
        t = f"append_v{fmt}"
        full = make_table(spark, t, fmt)
        run_op(f"spark_append_v{fmt}", t, lambda f=full: sql(f"INSERT INTO {f} VALUES (1000, 0, 'a')"),
               exp("append", "append", amb="data_files_added_only"), "spark", "INSERT INTO", fmt)

    t = "overwrite_dyn"
    full = make_table(spark, t, 2)

    def _overwrite(f: str = full) -> None:
        base = spark.sql(f"SELECT snapshot_id FROM {f}.snapshots ORDER BY committed_at DESC LIMIT 1"
                         ).collect()[0][0]
        (spark.range(0, 5).selectExpr("id", "0 AS bkt", "'ow' AS val").writeTo(f)
         .option("isolation-level", "serializable").option("validate-from-snapshot-id", str(base))
         .overwritePartitions())
    run_op("spark_overwrite_partitions_opts", t, _overwrite,
           exp("overwrite", "overwrite", true="overwrite_opts", amb="overwrite_options"), "spark",
           "DataFrame overwritePartitions() with isolation-level + validate-from-snapshot-id", 2)

    t = "insert_overwrite_static"
    full = make_table(spark, t, 2)
    run_op("spark_insert_overwrite_static", t,
           lambda f=full: sql(f"INSERT OVERWRITE {f} PARTITION (bkt = 0) SELECT 7, 'io'"),
           exp("cow_dml", "overwrite", true="insert_overwrite", amb="overwrite_by_filter"), "spark",
           "INSERT OVERWRITE ... PARTITION (static)", 2,
           note="OverwriteFiles by filter: same commit class and summary keys as CoW DML "
                "(SparkWrite.java:374-404); not distinguishable from the body, the slot→op binding "
                "gives the real type")

    for stmt, label in (("DELETE FROM {f} WHERE id = 3", "delete"),
                        ("UPDATE {f} SET val = 'u' WHERE id = 4", "update"),
                        ("MERGE INTO {f} t USING (SELECT 5L AS id, 1 AS bkt) s ON t.id = s.id "
                         "AND t.bkt = s.bkt WHEN MATCHED THEN UPDATE SET val = 'm'", "merge")):
        t = f"cow_{label}"
        full = make_table(spark, t, 2)
        run_op(f"spark_cow_{label}", t, lambda f=full, st=stmt: sql(st.format(f=f)),
               exp("cow_dml", "overwrite", amb="overwrite_by_filter"), "spark",
               f"CoW {label.upper()} (rows of a file)", 2)

    t = "metadata_delete"
    full = make_table(spark, t, 2)
    run_op("spark_metadata_delete", t, lambda f=full: sql(f"DELETE FROM {f} WHERE bkt = 1"),
           exp("cow_dml", "delete", true="metadata_delete", amb="data_files_removed_only"), "spark",
           "DELETE of whole partition (metadata delete)", 2)

    # MERGE that only inserts (no target row matched): the commit class reports "append"
    # (BaseOverwriteFiles.java:54-56, BaseRowDelta.java:51-53) although the operation validates
    for mode, true in (("copy-on-write", "cow_dml"), ("merge-on-read", "mor_dml")):
        t = f"merge_insert_only_{true}"
        full = make_table(spark, t, 2, mode=mode)
        run_op(f"spark_{true}_merge_insert_only", t,
               lambda f=full: sql(f"MERGE INTO {f} t USING (SELECT 999999L AS id, 0 AS bkt) s "
                                  "ON t.id = s.id AND t.bkt = s.bkt WHEN MATCHED THEN UPDATE SET val = 'm' "
                                  "WHEN NOT MATCHED THEN INSERT (id, bkt, val) VALUES (s.id, s.bkt, 'new')"),
               exp("append", "append", true=true, amb="data_files_added_only"), "spark",
               f"MERGE with inserts only ({mode})", 2)

    for fmt in (2, 3):
        for stmt, label, op in (("DELETE FROM {f} WHERE id = 3", "delete", "delete"),
                                ("UPDATE {f} SET val = 'u' WHERE id = 4", "update", "overwrite"),
                                ("MERGE INTO {f} t USING (SELECT 5L AS id, 1 AS bkt) s ON t.id = s.id "
                                 "AND t.bkt = s.bkt WHEN MATCHED THEN UPDATE SET val = 'm'",
                                 "merge", "overwrite")):
            t = f"mor_{label}_v{fmt}"
            full = make_table(spark, t, fmt, mode="merge-on-read")
            run_op(f"spark_mor_{label}_v{fmt}", t, lambda f=full, st=stmt: sql(st.format(f=f)),
                   exp("mor_dml", op, True), "spark",
                   f"MoR {label.upper()} ({'DV' if fmt == 3 else 'position deletes'})", fmt)

    t = "rewrite_data"
    full = make_table(spark, t, 2, files=6)
    run_op("spark_rewrite_data_files", t,
           lambda f=full: sql(f"CALL {CAT}.system.rewrite_data_files(table => '{NS}.{t}', "
                              "options => map('min-input-files', '2'))"),
           exp("rewrite_data", "replace"), "spark", "CALL rewrite_data_files", 2)

    t = "rewrite_deletes"
    full = make_table(spark, t, 2, mode="merge-on-read")
    for i in range(3):
        sql(f"DELETE FROM {full} WHERE id = {i * 2}")
    run_op("spark_rewrite_position_delete_files_v2", t,
           lambda f=full: sql(f"CALL {CAT}.system.rewrite_position_delete_files(table => '{NS}.{t}', "
                              "options => map('rewrite-all', 'true'))"),
           exp("rewrite_deletes", "replace", True), "spark", "CALL rewrite_position_delete_files", 2)

    t = "expire"
    full = make_table(spark, t, 2)
    run_op("spark_expire_snapshots", t,
           lambda f=full: sql(f"CALL {CAT}.system.expire_snapshots(table => '{NS}.{t}', "
                              "older_than => current_timestamp(), retain_last => 1)"),
           exp("expire", None), "spark", "CALL expire_snapshots", 2)

    t = "ctas"
    sql(f"DROP TABLE IF EXISTS {CAT}.{NS}.{t} PURGE")
    run_op("spark_ctas", t,
           lambda: sql(f"CREATE TABLE {CAT}.{NS}.{t} USING iceberg AS SELECT 1L AS id, 'x' AS val"),
           exp("append", "append", amb="data_files_added_only"), "spark",
           "CREATE TABLE AS SELECT (create transaction)", 2)

    t = "alter"
    full = make_table(spark, t, 2)
    run_op("spark_alter_add_column", t, lambda f=full: sql(f"ALTER TABLE {f} ADD COLUMN extra INT"),
           exp("metadata", None), "spark", "ALTER TABLE ADD COLUMN", 2)
    run_op("spark_set_properties", t,
           lambda f=full: sql(f"ALTER TABLE {f} SET TBLPROPERTIES ('capl.fixture' = '1')"),
           exp("metadata", None), "spark", "ALTER TABLE SET TBLPROPERTIES", 2)

    # equality-delete upsert through the Java API, as Flink commits it (RowDelta without validation)
    t = "upsert_java"
    full = make_table(spark, t, 2, id_type="INT")    # py4j passes Python ints as Integer
    run_op("java_rowdelta_equality_upsert", t, lambda: _upsert_java(spark, t),
           exp("stream_upsert", "overwrite", True), "java",
           "RowDelta(addRows + equality deletes), Iceberg Java API", 2)

    # multi-table transaction: two tables, each with an appended data file (CommitTransactionRequest)
    ta, tb = "txn_a", "txn_b"
    make_table(spark, ta, 2)
    make_table(spark, tb, 2)
    t0 = int(time.time() * 1000)
    _txn_commit(jvm, ta, tb)
    txn = [e for e in proxy_commits(t0, None) if e["kind"] == "transaction" and e["status"] == 204]
    assert txn, "no transaction commit seen"
    save("java_multi_table_transaction", txn[-1],
         {"kind": "transaction", "tables": [f"{NS}.{ta}", f"{NS}.{tb}"], "op_class": "append",
          "true_class": "append", "ambiguity": "data_files_added_only",
          "iceberg_op": "append", "has_deletes": False, "is_flink": False},
         "java", "RESTCatalog.commitTransaction(TableCommit x2), FastAppend in each", 2)
    spark.stop()


def _java_catalog(jvm: Any, name: str) -> Any:
    props = jvm.java.util.HashMap()
    for k, v in {"uri": PROXY_URL, "warehouse": WAREHOUSE,
                 "io-impl": "org.apache.iceberg.aws.s3.S3FileIO", "s3.endpoint": S3_URL,
                 "s3.path-style-access": "true", "header.X-CAPL-Slot": SLOT,
                 "header.X-CAPL-Writer": "test"}.items():
        props.put(k, v)
    cat = jvm.org.apache.iceberg.rest.RESTCatalog()
    cat.initialize(name, props)
    return cat


def _upsert_java(spark: Any, table: str) -> None:
    jvm = spark.sparkContext._jvm
    gw = spark.sparkContext._gateway
    cat = _java_catalog(jvm, "fx_java")
    tbl = cat.loadTable(jvm.org.apache.iceberg.catalog.TableIdentifier.parse(f"{NS}.{table}"))
    ff = jvm.org.apache.iceberg.FileFormat
    schema, spec = tbl.schema(), tbl.spec()
    ids = gw.new_array(jvm.int, 1)
    ids[0] = schema.findField("id").fieldId()
    wf = (getattr(jvm.org.apache.iceberg.data, "GenericFileWriterFactory$Builder")(tbl)
          .dataFileFormat(ff.PARQUET).deleteFileFormat(ff.PARQUET)
          .equalityFieldIds(ids).equalityDeleteRowSchema(schema).build())
    off = jvm.org.apache.iceberg.io.OutputFileFactory.builderFor(tbl, 0, 1).format(ff.PARQUET).build()
    rec = jvm.org.apache.iceberg.data.GenericRecord.create(schema)
    rec.setField("id", 1)
    rec.setField("bkt", 1)
    rec.setField("val", "upsert")
    pk = jvm.org.apache.iceberg.PartitionKey(spec, schema)
    pk.partition(rec)
    dw = wf.newDataWriter(off.newOutputFile(spec, pk), spec, pk)
    ew = wf.newEqualityDeleteWriter(off.newOutputFile(spec, pk), spec, pk)
    ew.write(rec)
    dw.write(rec)
    dw.close()
    ew.close()
    tbl.newRowDelta().addRows(dw.toDataFile()).addDeletes(ew.toDeleteFile()).commit()


def _txn_commit(jvm: Any, ta: str, tb: str) -> None:
    cat = _java_catalog(jvm, "fx_txn")
    ti = jvm.org.apache.iceberg.catalog.TableIdentifier
    commits = jvm.java.util.ArrayList()
    for name in (ta, tb):
        ident = ti.parse(f"{NS}.{name}")
        tbl = cat.loadTable(ident)
        files = list(tbl.currentSnapshot().addedDataFiles(tbl.io()))
        txn = tbl.newTransaction()
        txn.newFastAppend().appendFile(files[0]).commit()     # staged in the transaction only
        commits.add(jvm.org.apache.iceberg.catalog.TableCommit.create(
            ident, txn.startMetadata(), txn.currentMetadata()))
    cat.commitTransaction(commits)


# ---------------------------------------------------------------- flink

def capture_flink() -> None:
    r = requests.post(f"{FLINK_GW}/v1/sessions", json={"properties": {
        "execution.runtime-mode": "streaming", "execution.checkpointing.interval": "2s",
        "state.checkpoints.dir": "file:///flink-checkpoints/fixtures",
        "pipeline.name": "capl-fixtures"}}, timeout=10)
    r.raise_for_status()
    sid = r.json()["sessionHandle"]

    def q(stmt: str, timeout_s: int = 180) -> None:
        resp = requests.post(f"{FLINK_GW}/v1/sessions/{sid}/statements", json={"statement": stmt},
                             timeout=30)
        resp.raise_for_status()
        op = resp.json()["operationHandle"]
        for _ in range(timeout_s):
            st = requests.get(f"{FLINK_GW}/v1/sessions/{sid}/operations/{op}/status",
                              timeout=10).json().get("status")
            if st == "FINISHED":
                return
            if st in ("ERROR", "CLOSED", "CANCELED"):
                res = requests.get(f"{FLINK_GW}/v1/sessions/{sid}/operations/{op}/result/0",
                                   timeout=10).text
                raise RuntimeError(f"{st}: {stmt[:100]} {res[:800]}")
            time.sleep(1)
        raise TimeoutError(stmt[:100])

    def wait_job_commit(table: str, t0: int, timeout_s: int = 120) -> dict[str, Any]:
        for _ in range(timeout_s):
            ok = [e for e in proxy_commits(t0, table) if e["status"] == 200]
            if ok:
                return ok[-1]
            time.sleep(1)
        raise TimeoutError(f"no Flink commit for {table}")

    try:
        q(f"""CREATE CATALOG fxf WITH ('type'='iceberg', 'catalog-type'='rest', 'uri'='{PROXY_URL}',
              'warehouse'='{WAREHOUSE}', 'io-impl'='org.apache.iceberg.aws.s3.S3FileIO',
              's3.endpoint'='{S3_URL}', 's3.path-style-access'='true',
              'header.X-CAPL-Writer'='stream', 'header.X-CAPL-Slot'='{SLOT}-flink')""")
        q(f"CREATE DATABASE IF NOT EXISTS fxf.{NS}")
        for t in ("flink_append", "flink_upsert"):
            q(f"DROP TABLE IF EXISTS fxf.{NS}.{t}")
        q(f"CREATE TABLE fxf.{NS}.flink_append (id BIGINT, bkt INT, val STRING) "
          f"PARTITIONED BY (bkt) WITH ('format-version'='2')")
        q(f"CREATE TABLE fxf.{NS}.flink_upsert (id BIGINT, bkt INT, val STRING, "
          f"PRIMARY KEY (id, bkt) NOT ENFORCED) PARTITIONED BY (bkt) "
          f"WITH ('format-version'='2', 'write.upsert.enabled'='true')")
        t0 = int(time.time() * 1000)
        q(f"INSERT INTO fxf.{NS}.flink_append VALUES (1, 0, 'a'), (2, 1, 'b')")
        e = wait_job_commit("flink_append", t0)
        save("flink_append", e, {"op_class": "append", "iceberg_op": "append", "has_deletes": False,
                                 "true_class": "append", "ambiguity": None,
                                 "is_flink": True},
             "flink", "Flink SQL INSERT INTO (IcebergFilesCommitter)", 2)
        t0 = int(time.time() * 1000)
        q(f"INSERT INTO fxf.{NS}.flink_upsert VALUES (1, 0, 'a'), (1, 0, 'b'), (2, 1, 'c')")
        e = wait_job_commit("flink_upsert", t0)
        save("flink_upsert_v2", e, {"op_class": "stream_upsert", "iceberg_op": "overwrite",
                                    "true_class": "stream_upsert", "ambiguity": None,
                                    "has_deletes": True, "is_flink": True},
             "flink", "Flink SQL INSERT INTO with write.upsert.enabled (equality deletes)", 2)
    finally:
        requests.delete(f"{FLINK_GW}/v1/sessions/{sid}", timeout=10)


def main() -> None:
    requests.put(f"{ADV_URL}/_capl/debug/keep-bodies", params={"on": "true"}, timeout=10).raise_for_status()
    try:
        which = os.environ.get("FIXTURES", "spark,flink").split(",")
        if "spark" in which:
            capture_spark()
        if "flink" in which:
            capture_flink()
    finally:
        requests.put(f"{ADV_URL}/_capl/debug/keep-bodies", params={"on": "false"}, timeout=10)
    print(f"fixtures: {len(list(OUT.glob('*.json')))} in {OUT}")


if __name__ == "__main__":
    main()
