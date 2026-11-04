"""Stage 2 integration checks of the CAPL proxy on the stand (tz/06 stage 2, REVIEWER.md §4).

Run in the runner container with the stand up (make up):
    docker compose ... exec -T runner pytest -q tests/integration/test_proxy_stage2.py

- test_journal_and_footprints: Spark commits (≥ 1000 appends + DML / compaction / metadata deletes)
  through the proxy; every snapshot appears in commit_attempts and commit_events; footprints in
  commit_partitions equal PyIceberg inspect.entries (catalog read directly, not via the proxy) on a
  sample of ≥ 100 snapshots; sampled SHA-256 integrity checks report no mismatch.
- test_responses_identical_to_catalog: same requests via the proxy and directly → same status,
  headers (except Date) and body bytes, including 404 and 409.
- test_overhead_bypass_vs_proxy: NFR-1, commit request latency, ≥ 1000 commits per path.
- test_fail_open_under_load: advisor stopped / failing / slow (flag) while commits run.
Numbers go to results/stage-2/<RESULTS_NAME>/.
"""
from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import random
import statistics
import threading
import time
from collections import Counter
from pathlib import Path
from typing import Any

import pytest
import requests

PROXY_URL = os.environ.get("CAPL_PROXY_URI", "http://capl:8180")
DIRECT_URL = os.environ.get("CATALOG_DIRECT_URI", "http://catalog:8181")
ADV_URL = os.environ.get("CAPL_ADVISOR_URI", "http://capl:8190")
METRICS_URL = os.environ.get("CAPL_METRICS_URI", "http://capl:9101/metrics")
WAREHOUSE = os.environ.get("WAREHOUSE", "s3://warehouse/")
S3_URL = os.environ.get("S3_ENDPOINT", "http://toxiproxy:18333")
SPARK_MASTER = os.environ.get("SPARK_MASTER", "spark://spark-master:7077")
PG_DSN = os.environ.get("TELEMETRY_PG_DSN", "postgresql://capl:capl@postgres:5432/telemetry")
N_APPENDS = int(os.environ.get("STAGE2_APPENDS", "1000"))
N_BENCH = int(os.environ.get("STAGE2_BENCH_COMMITS", "1000"))
BENCH_TABLE_PROPS = {"commit.manifest-merge.enabled":
                     os.environ.get("STAGE2_BENCH_MANIFEST_MERGE", "true")}
N_FOOTPRINT_SAMPLE = int(os.environ.get("STAGE2_FOOTPRINT_SAMPLE", "150"))
BENCH_EXPIRE = os.environ.get("STAGE2_BENCH_EXPIRE", "true") == "true"
# Size of one snapshot entry in the table metadata (compact JSON), bytes: the scenario writers are
# Spark and Flink, whose snapshot summary carries engine/app properties (fixtures spark_append_v2
# 871 B, flink_append 882 B, spark_mor_merge_v2 960 B) that PyIceberg's does not (~544 B on the
# stand). The bench appends with snapshot_properties padded to this size (S2-R3-02); 0 = PyIceberg
# as is (r3, r4, r7).
BENCH_SNAPSHOT_BYTES = int(os.environ.get("STAGE2_BENCH_SNAPSHOT_BYTES", "960"))
# windows whose median snapshot count is within the top band of the history get pooled for the
# NFR-1 check (S2-R3-01): band = snapshots >= BENCH_BAND * expire_at (or * max without expire)
BENCH_BAND = float(os.environ.get("STAGE2_BENCH_BAND", "0.9"))
BENCH_MIN_BAND_PAIRS = 300
# summary keys of a Spark 4.0 append (tests/fixtures/commits/spark_append_v2.json) absent from PyIceberg's
SPARK_SUMMARY = {"spark.app.id": "app-20261101154946-0005", "app-id": "app-20261101154946-0005",
                 "engine-name": "spark", "engine-version": "4.0.1", "app-name": "capl-stage2-bench",
                 "iceberg-version": "Apache Iceberg 1.12.0 (commit 212aa894e919ff9fdf89160da4aadcbe6858ac01)",
                 "manifests-created": "1", "manifests-kept": "0", "manifests-replaced": "0"}
WORKLOADS = Path(__file__).parents[2] / "config" / "workloads"
OUT = Path(os.environ.get("STAGE2_OUT",
                          f"/workspace/results/stage-2/{os.environ.get('RESULTS_NAME', 'run')}"))
NS = "stage2"
RUN = f"stage2-{int(time.time())}"
SEED = 2026


def _bench_expire_plan() -> dict[str, Any]:
    """Table history the scenarios run with (ADR-008), in commit counts: S1 has the highest commit
    rate (sum of writers / commit_period_s of its streams); scheduled expire every interval_s keeps
    the snapshots of the last older_than_s, so the table oscillates between rate * older_than_s
    and rate * (older_than_s + interval_s) snapshots."""
    import yaml

    s1 = yaml.safe_load(next(WORKLOADS.glob("S1_*.yaml")).read_text())
    exp = {**yaml.safe_load((WORKLOADS / "_base.yaml").read_text())["maintenance"]["expire_snapshots"],
           **s1.get("maintenance", {}).get("expire_snapshots", {})}     # S1 may override (ADR-008)
    rate = sum(st.get("writers", 1) / st["commit_period_s"] for st in s1["streams"])
    return {"s1_commits_per_s": rate, "interval_s": exp["interval_s"], "older_than_s": exp["older_than_s"],
            "retain": round(rate * exp["older_than_s"]),
            "expire_at": round(rate * (exp["older_than_s"] + exp["interval_s"]))}


def _save(name: str, doc: Any) -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / name).write_text(json.dumps(doc, indent=1, default=str) + "\n")


def _metric(name: str, labels: dict[str, str] | None = None) -> float:
    total = 0.0
    for line in requests.get(METRICS_URL, timeout=10).text.splitlines():
        if not line.startswith(name):
            continue
        head, _, val = line.rpartition(" ")
        if head.split("{")[0] != name:
            continue
        if labels and not all(f'{k}="{v}"' in head for k, v in labels.items()):
            continue
        total += float(val)
    return total


def _pq(sql: str, *args: Any) -> list[tuple[Any, ...]]:
    import psycopg

    with psycopg.connect(PG_DSN) as conn:
        return conn.execute(sql, args).fetchall()


def _pyiceberg(uri: str, slot: str | None = None) -> Any:
    from pyiceberg.catalog.rest import RestCatalog

    props = {"uri": uri, "warehouse": WAREHOUSE, "s3.endpoint": S3_URL,
             "s3.access-key-id": os.environ["AWS_ACCESS_KEY_ID"],
             "s3.secret-access-key": os.environ["AWS_SECRET_ACCESS_KEY"],
             "s3.region": os.environ.get("AWS_REGION", "us-east-1")}
    if slot:
        props.update({"header.X-CAPL-Slot": slot, "header.X-CAPL-Run": RUN,
                      "header.X-CAPL-Writer": "test"})
    return RestCatalog(f"cat-{slot or 'direct'}", **props)


def _flush() -> dict[str, Any]:
    r = requests.post(f"{ADV_URL}/v1/debug/flush", timeout=600)
    r.raise_for_status()
    return r.json()  # type: ignore[no-any-return]


@pytest.fixture(scope="module")
def spark() -> Any:
    from pyspark.sql import SparkSession

    pre = "spark.sql.catalog.s2"
    b = (SparkSession.builder.appName("stage2-proxy").master(SPARK_MASTER)
         .config("spark.sql.extensions",
                 "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
         .config("spark.sql.session.timeZone", "UTC").config("spark.cores.max", "4")
         .config("spark.driver.memory", "1536m").config("spark.executor.memory", "2g")
         .config("spark.sql.shuffle.partitions", "4").config("spark.ui.enabled", "false")
         .config(pre, "org.apache.iceberg.spark.SparkCatalog").config(f"{pre}.type", "rest")
         .config(f"{pre}.uri", PROXY_URL).config(f"{pre}.warehouse", WAREHOUSE)
         .config(f"{pre}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
         .config(f"{pre}.s3.endpoint", S3_URL).config(f"{pre}.s3.path-style-access", "true")
         .config(f"{pre}.cache-enabled", "false")
         .config(f"{pre}.header.X-CAPL-Slot", "s2-spark").config(f"{pre}.header.X-CAPL-Writer", "test")
         .config(f"{pre}.header.X-CAPL-Run", RUN))
    session = b.getOrCreate()
    session.sql(f"CREATE NAMESPACE IF NOT EXISTS s2.{NS}")
    yield session
    session.stop()


@pytest.fixture(scope="module", autouse=True)
def _provenance() -> Any:
    """Commit the run was made on (make proxy-verify passes GIT_SHA; '-dirty' = uncommitted)."""
    _save("git_sha.json", {"git_sha": os.environ.get("GIT_SHA", "unknown"), "run": RUN,
                           "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())})
    yield


@pytest.fixture(autouse=True)
def _reset_proxy() -> Any:
    requests.put(f"{ADV_URL}/v1/debug/advisor", json={"fault": "none"}, timeout=10)
    requests.put(f"{ADV_URL}/v1/debug/mode", json={"mode": "full"}, timeout=10)
    yield
    requests.put(f"{ADV_URL}/v1/debug/advisor", json={"fault": "none"}, timeout=10)
    requests.post(f"{ADV_URL}/v1/debug/run-stop", timeout=10)
    requests.put(f"{ADV_URL}/_capl/debug/integrity", params={"rate": 0.01}, timeout=10)


# ---------------------------------------------------------------- journal and footprints

def _part_str(p: dict[str, Any]) -> str:
    """Independent of capl.enricher: PyIceberg partition record → 'dt=…/key_bucket=…'."""
    out = []
    for k, v in p.items():
        if isinstance(v, dt.date):
            v = v.isoformat()
        out.append(f"{k}={'null' if v is None else v}")
    return "/".join(out)


def _pyiceberg_footprint(tbl: Any, sid: int) -> dict[str, tuple[int, int, int, int]]:
    """partition → (added data, deleted data, added delete, removed delete files) of `sid`."""
    ents = tbl.inspect.entries(snapshot_id=sid).to_pylist()
    acc: dict[str, list[int]] = {}
    for e in ents:
        if e["snapshot_id"] != sid or e["status"] not in (1, 2):
            continue
        df = e["data_file"]
        key = _part_str(df["partition"])
        a = acc.setdefault(key, [0, 0, 0, 0])
        if df["content"] == 0:
            a[0 if e["status"] == 1 else 1] += 1
        else:
            a[2 if e["status"] == 1 else 3] += 1
    return {k: (v[0], v[1], v[2], v[3]) for k, v in acc.items()}


def test_journal_and_footprints(spark: Any) -> None:
    rng = random.Random(SEED)
    table = f"journal_{int(time.time())}"
    full = f"s2.{NS}.{table}"
    spark.sql(f"""CREATE TABLE {full} (event_id BIGINT, key BIGINT, dt DATE, val STRING)
                  USING iceberg PARTITIONED BY (dt, bucket(8, key))
                  TBLPROPERTIES ('format-version'='2', 'write.delete.mode'='copy-on-write',
                                 'write.distribution-mode'='none')""")
    requests.put(f"{ADV_URL}/_capl/debug/integrity", params={"rate": 1.0}, timeout=10)
    ok_before = _metric("capl_body_integrity_total", {"result": "ok"})
    bad_before = _metric("capl_body_integrity_total", {"result": "mismatch"})
    days = [dt.date(2026, 11, 1) + dt.timedelta(days=i) for i in range(10)]
    eid = 0
    t0 = time.time()
    for _ in range(N_APPENDS):
        rows = []
        for _ in range(rng.randint(1, 4)):
            eid += 1
            rows.append(f"({eid}, {rng.randint(0, 10**6)}, DATE '{rng.choice(days)}', 'v')")
        spark.sql(f"INSERT INTO {full} VALUES {', '.join(rows)}")
    t_appends = time.time() - t0
    # operations that delete files: CoW DELETE of a few rows, metadata delete, compaction
    other = Counter()
    for _ in range(20):
        spark.sql(f"DELETE FROM {full} WHERE event_id = {rng.randint(1, eid)}")
        other["cow_delete"] += 1
    for d in days[:3]:
        spark.sql(f"DELETE FROM {full} WHERE dt = DATE '{d}'")
        other["metadata_delete"] += 1
    for d in days[3:8]:
        spark.sql(f"CALL s2.system.rewrite_data_files(table => '{NS}.{table}', "
                  f"where => \"dt = '{d}'\", options => map('min-input-files', '2'))")
        other["rewrite_data"] += 1
    mor = {fv: _mor_table(spark, fv, rng) for fv in (2, 3)}
    flushed = _flush()

    direct = _pyiceberg(DIRECT_URL)
    tbl = direct.load_table(f"{NS}.{table}")
    snaps = {s.snapshot_id: s for s in tbl.snapshots()}
    attempts = _pq("SELECT new_snapshot_id, http_status, op_class, hold_ms, slot, run_id "
                   "FROM stand.commit_attempts WHERE table_name = %s", f"{NS}.{table}")
    ok_ids = {a[0] for a in attempts if a[1] == 200 and a[0] is not None}
    events = {r[0]: r for r in _pq("SELECT snapshot_id, op_class, n_partitions, added_data_files, "
                                   "deleted_data_files FROM stand.commit_events WHERE table_name = %s",
                                   f"{NS}.{table}")}
    missing_attempts = set(snaps) - ok_ids
    missing_events = set(snaps) - set(events)

    # footprints: all non-append snapshots + a seeded sample of appends
    non_append = [s for s, v in snaps.items() if v.summary.operation.value != "append"]
    appends = sorted(set(snaps) - set(non_append))
    picked = random.Random(SEED).sample(appends, min(N_FOOTPRINT_SAMPLE, len(appends)))
    sample = sorted(set(non_append) | set(picked))
    mismatches = []
    n_parts = 0
    for sid in sample:
        exp = _pyiceberg_footprint(tbl, sid)
        got = {r[0]: (r[1], r[2], r[3], r[4]) for r in _pq(
            "SELECT partition, added_data_files, deleted_data_files, added_delete_files, "
            "removed_delete_files FROM stand.commit_partitions WHERE table_name = %s AND snapshot_id = %s",
            f"{NS}.{table}", sid)}
        n_parts += len(exp)
        if exp != got:
            mismatches.append({"snapshot_id": sid, "pyiceberg": exp, "capl": got})
    compared = Counter()
    for sid in sample:
        for a, d, df, rd in _pyiceberg_footprint(tbl, sid).values():
            compared.update(added_data=a, deleted_data=d, added_delete=df, removed_delete=rd)

    # MoR v2: per partition against PyIceberg; v3 (DV): PyIceberg 0.12.0 inspect.entries fails on
    # DV tables (pyiceberg/table/inspect.py:271), so totals are compared with the snapshot summary
    mor_v2 = direct.load_table(mor[2])
    for snap in mor_v2.snapshots():
        sid = snap.snapshot_id
        exp = _pyiceberg_footprint(mor_v2, sid)
        got = {r[0]: (r[1], r[2], r[3], r[4]) for r in _pq(
            "SELECT partition, added_data_files, deleted_data_files, added_delete_files, "
            "removed_delete_files FROM stand.commit_partitions WHERE table_name = %s AND snapshot_id = %s",
            mor[2], sid)}
        n_parts += len(exp)
        for a, d, df, rd in exp.values():
            compared.update(mor_v2_added_data=a, mor_v2_deleted_data=d, mor_v2_added_delete=df,
                            mor_v2_removed_delete=rd)
        if exp != got:
            mismatches.append({"table": mor[2], "snapshot_id": sid, "pyiceberg": exp, "capl": got})
    mor_v3 = direct.load_table(mor[3])
    v3_mismatches = []
    for snap in mor_v3.snapshots():
        summ = snap.summary.additional_properties if snap.summary else {}
        want = (int(summ.get("added-data-files", 0)), int(summ.get("deleted-data-files", 0)),
                int(summ.get("added-delete-files", 0)), int(summ.get("removed-delete-files", 0)))
        row = _pq("SELECT added_data_files, deleted_data_files, added_delete_files, removed_delete_files "
                  "FROM stand.commit_events WHERE table_name = %s AND snapshot_id = %s",
                  mor[3], snap.snapshot_id)
        got3 = tuple(row[0]) if row else None
        compared.update(mor_v3_added_delete=want[2], mor_v3_removed_delete=want[3], mor_v3_snapshots=1)
        if got3 != want:
            v3_mismatches.append({"snapshot_id": snap.snapshot_id, "summary": want, "capl": got3})
    ok_after = _metric("capl_body_integrity_total", {"result": "ok"})
    bad_after = _metric("capl_body_integrity_total", {"result": "mismatch"})
    report = {
        "mor_tables": mor, "compared_nonzero": compared, "mor_v3_total_mismatches": v3_mismatches,
        "table": f"{NS}.{table}", "run": RUN, "appends": N_APPENDS, "other_ops": other,
        "append_phase_s": round(t_appends, 1), "snapshots": len(snaps),
        "commit_attempts": len(attempts), "attempts_status": Counter(a[1] for a in attempts),
        "attempts_ok_with_snapshot": len(ok_ids), "missing_in_attempts": sorted(missing_attempts),
        "commit_events": len(events), "missing_in_events": sorted(missing_events),
        "op_class_events": Counter(e[1] for e in events.values()),
        "footprint_sample_snapshots": len(sample), "footprint_sample_non_append": len(non_append),
        "footprint_partitions_compared": n_parts, "footprint_mismatches": mismatches[:20],
        "n_footprint_mismatches": len(mismatches), "flush": flushed,
        "integrity_ok": ok_after - ok_before, "integrity_mismatch": bad_after - bad_before,
    }
    _save("journal_footprints.json", report)
    assert len(snaps) >= N_APPENDS + other["cow_delete"] + other["metadata_delete"]
    assert not missing_attempts, f"{len(missing_attempts)} snapshots not in commit_attempts"
    assert not missing_events, f"{len(missing_events)} snapshots not in commit_events"
    assert len(sample) >= 100
    assert not mismatches, f"{len(mismatches)} footprint mismatches, e.g. {mismatches[:2]}"
    assert not v3_mismatches, v3_mismatches[:3]
    # each compared quantity is non-zero somewhere (a 0 = 0 comparison proves nothing, S2-R1-04)
    for k in ("added_data", "deleted_data", "mor_v2_added_delete", "mor_v2_deleted_data",
              "mor_v2_removed_delete", "mor_v3_added_delete", "mor_v3_removed_delete"):
        assert compared[k] > 0, (k, compared)
    assert report["integrity_ok"] >= len(attempts) and report["integrity_mismatch"] == 0


def _mor_table(spark: Any, fv: int, rng: random.Random) -> str:
    """MoR table with DELETE / UPDATE / MERGE and (v2) rewrite_position_delete_files."""
    table = f"mor_v{fv}_{int(time.time())}"
    full = f"s2.{NS}.{table}"
    spark.sql(f"""CREATE TABLE {full} (event_id BIGINT, key BIGINT, dt DATE, val STRING)
                  USING iceberg PARTITIONED BY (dt, bucket(4, key))
                  TBLPROPERTIES ('format-version'='{fv}', 'write.delete.mode'='merge-on-read',
                                 'write.update.mode'='merge-on-read', 'write.merge.mode'='merge-on-read',
                                 'write.distribution-mode'='none')""")
    for i in range(6):
        spark.sql(f"INSERT INTO {full} SELECT id, id % 97, DATE '2026-11-01' + CAST(id % 3 AS INT), 'v' "
                  f"FROM range({i * 200}, {(i + 1) * 200})")
    for _ in range(4):
        spark.sql(f"DELETE FROM {full} WHERE event_id = {rng.randint(0, 1199)}")
        spark.sql(f"UPDATE {full} SET val = 'u' WHERE event_id = {rng.randint(0, 1199)}")
    spark.sql(f"MERGE INTO {full} t USING (SELECT {rng.randint(0, 1199)}L AS event_id) s "
              "ON t.event_id = s.event_id WHEN MATCHED THEN UPDATE SET val = 'm'")
    if fv == 2:
        spark.sql(f"CALL s2.system.rewrite_position_delete_files(table => '{NS}.{table}', "
                  "options => map('rewrite-all', 'true'))")
    spark.sql(f"CALL s2.system.rewrite_data_files(table => '{NS}.{table}', "
              "options => map('min-input-files', '2', 'delete-file-threshold', '1'))")
    return f"{NS}.{table}"


# ---------------------------------------------------------------- responses unchanged

def test_responses_identical_to_catalog(spark: Any) -> None:
    table = f"resp_{int(time.time())}"
    spark.sql(f"CREATE TABLE s2.{NS}.{table} (id BIGINT) USING iceberg")
    spark.sql(f"INSERT INTO s2.{NS}.{table} VALUES (1)")
    meta = requests.get(f"{DIRECT_URL}/v1/namespaces/{NS}/tables/{table}", timeout=10).json()
    cur = meta["metadata"]["current-snapshot-id"]
    stale = {"requirements": [{"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": cur + 1}],
             "updates": [{"action": "set-properties", "updates": {"k": "v"}}]}
    reqs = [
        ("GET", "/v1/config", None), ("GET", f"/v1/namespaces/{NS}", None),
        ("GET", f"/v1/namespaces/{NS}/tables/{table}", None),
        ("GET", f"/v1/namespaces/{NS}/tables/no_such_table", None),
        ("HEAD", f"/v1/namespaces/{NS}/tables/{table}", None),
        ("POST", f"/v1/namespaces/{NS}/tables/{table}", json.dumps(stale).encode()),
        ("POST", f"/v1/namespaces/{NS}/tables/no_such_table", json.dumps(stale).encode()),
        ("POST", f"/v1/namespaces/{NS}/tables/{table}", b"{not json"),
    ]
    rows = []
    for method, path, body in reqs:
        out = []
        for base in (PROXY_URL, DIRECT_URL):
            # same Host for both: the catalog reflects the received Host in error pages, and the
            # proxy forwards Host unchanged
            hdrs_in = {"Host": "capl:8180", **({"Content-Type": "application/json"} if body else {})}
            r = requests.request(method, base + path, data=body, timeout=30, headers=hdrs_in)
            hdrs = _raw_headers(r, drop=("date",))
            out.append((r.status_code, hdrs, hashlib.sha256(r.content).hexdigest(), len(r.content)))
        rows.append({"method": method, "path": path, "proxy": out[0][::2], "direct": out[1][::2],
                     "same_status": out[0][0] == out[1][0], "same_headers": out[0][1] == out[1][1],
                     "same_body": out[0][2] == out[1][2]})
    _save("responses_identical.json", rows)
    statuses = {r["direct"][0] for r in rows}
    assert {200, 404, 409} <= statuses, statuses           # errors are passed through as well
    bad = [r for r in rows if not (r["same_status"] and r["same_headers"] and r["same_body"])]
    assert not bad, bad


def _raw_headers(r: Any, drop: tuple[str, ...] = ()) -> list[tuple[str, str]]:
    """All response header fields as received (repeated fields kept), names lower-cased."""
    return sorted((k.lower(), v) for k, v in r.raw.headers.items() if k.lower() not in drop)


def test_successful_commit_responses_identical(spark: Any) -> None:
    """S2-R1-01: responses to successful commits (200 table commit, 204 transaction) through the
    proxy and directly. Two tables prepared identically; the same commit goes to one through the
    proxy and to the other directly. Bodies of 200 differ only by table identity (uuid, location),
    so for 200 header fields are compared except the values of Content-Length and ETag (a hash of
    the body); their presence must agree."""
    rows = []
    for i in range(3):
        names = {}
        for via in ("proxy", "direct"):
            t = f"okc_{via}_{i}_{int(time.time())}"
            spark.sql(f"CREATE TABLE s2.{NS}.{t} (id BIGINT) USING iceberg")
            spark.sql(f"INSERT INTO s2.{NS}.{t} VALUES (1)")
            names[via] = t
        res = {}
        for via, base in (("proxy", PROXY_URL), ("direct", DIRECT_URL)):
            t = names[via]
            meta = requests.get(f"{DIRECT_URL}/v1/namespaces/{NS}/tables/{t}", timeout=10).json()
            cur = meta["metadata"]["current-snapshot-id"]
            change = {"requirements": [{"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": cur}],
                      "updates": [{"action": "set-properties", "updates": {"k": str(i)}}]}
            hdr = {"Host": "capl:8180", "Content-Type": "application/json"}
            r1 = requests.post(f"{base}/v1/namespaces/{NS}/tables/{t}", data=json.dumps(change),
                               headers=hdr, timeout=30)
            txn = {"table-changes": [{"identifier": {"namespace": [NS], "name": t},
                                      "requirements": [], "updates": [
                                          {"action": "set-properties", "updates": {"t": str(i)}}]}]}
            r2 = requests.post(f"{base}/v1/transactions/commit", data=json.dumps(txn), headers=hdr,
                               timeout=30)
            res[via] = (r1, r2)
        (p1, p2), (d1, d2) = res["proxy"], res["direct"]
        cl = ("date", "content-length", "etag")
        row = {
            "commit_status": [p1.status_code, d1.status_code],
            "commit_headers_proxy": _raw_headers(p1, cl), "commit_headers_direct": _raw_headers(d1, cl),
            "commit_has_content_length": ["content-length" in p1.headers,
                                          "content-length" in d1.headers],
            "commit_has_etag": ["etag" in p1.headers, "etag" in d1.headers],
            "commit_proxy_length_ok":
                int(p1.headers.get("content-length", len(p1.content))) == len(p1.content),
            "txn_status": [p2.status_code, d2.status_code],
            "txn_headers_proxy": _raw_headers(p2, ("date",)),
            "txn_headers_direct": _raw_headers(d2, ("date",)),
            "txn_body_len": [len(p2.content), len(d2.content)],
        }
        rows.append(row)
    _save("responses_successful_commits.json", rows)
    for row in rows:
        assert row["commit_status"] == [200, 200] and row["txn_status"] == [204, 204], row
        assert row["commit_headers_proxy"] == row["commit_headers_direct"], row
        assert row["commit_has_content_length"][0] == row["commit_has_content_length"][1], row
        assert row["commit_has_etag"][0] == row["commit_has_etag"][1], row
        assert row["commit_proxy_length_ok"], row
        assert row["txn_headers_proxy"] == row["txn_headers_direct"], row
        assert row["txn_body_len"] == [0, 0], row


# ---------------------------------------------------------------- NFR-1 overhead

def _q(xs: list[float], q: float) -> float:
    s = sorted(xs)
    return s[min(len(s) - 1, int(q * len(s)))]


def _metadata(tname: str) -> tuple[int, list[dict[str, Any]]]:
    """Table metadata read from the catalog directly (not measured): uncompressed bytes of the
    loadTable response (the same JSON a commit response carries) and the snapshot entries."""
    r = requests.get(f"{DIRECT_URL}/v1/namespaces/{NS}/tables/{tname}", timeout=60)
    r.raise_for_status()
    return len(r.content), r.json()["metadata"].get("snapshots", [])


def _snapshot_bytes(snap: dict[str, Any]) -> int:
    return len(json.dumps(snap, separators=(",", ":")))


def _paired_median_ci(diffs: list[float], rng: random.Random, n_boot: int) -> list[float]:
    """Bootstrap 95 % CI of the median of paired differences (pairs resampled with replacement)."""
    meds = sorted(statistics.median(rng.choices(diffs, k=len(diffs))) for _ in range(n_boot))
    return [meds[int(0.025 * n_boot)], meds[min(n_boot - 1, int(0.975 * n_boot))]]


def test_overhead_bypass_vs_proxy() -> None:
    """Latency of the commit request (RestCatalog.commit_table: one POST) for appends of the
    same data, alternating proxy/bypass, N_BENCH commits each; overhead = proxy - bypass.

    Bench tables merge manifests (`commit.manifest-merge.enabled=true`, min count 100), as the
    stand's writers do: Iceberg Java default, TableProperties.java:119,122 (Spark, Flink); PyIceberg
    0.12.0 defaults to false (pyiceberg/table/__init__.py:208). STAGE2_BENCH_MANIFEST_MERGE=false
    reproduces the growing manifest list (stage-2 r3).

    Snapshots are of the size Spark/Flink writes (BENCH_SNAPSHOT_BYTES, S2-R3-02): the overhead
    grows with the table metadata, so the history boundary is measured in both snapshots and bytes.

    The table history follows the scheduled expire of the scenarios (ADR-008, `_bench_expire_plan`):
    it grows to expire_at, then saws between retain and expire_at. NFR-1 is checked on the top band
    of that history (windows with a median snapshot count >= BENCH_BAND * expire_at, pooled): the
    upper bound of the bootstrap 95 % CI of the paired median <= 5 ms (S2-R3-01). The run has to
    pass at least one full saw cycle per path (two expires), otherwise the check fails.
    STAGE2_BENCH_EXPIRE=false lets the history grow without bound (r3, r4; boundary search)."""
    import pyarrow as pa
    from pyiceberg.schema import Schema
    from pyiceberg.types import LongType, NestedField

    schema = Schema(NestedField(1, "id", LongType(), required=False))
    cats = {"proxy": _pyiceberg(PROXY_URL, "bench"), "bypass": _pyiceberg(DIRECT_URL)}
    tables, lat = {}, {"proxy": [], "bypass": []}
    timing_on = {"proxy": False, "bypass": False}   # per path; a function attribute on `timed`
    # would be looked up through the loop variable and read the last path's flag
    for name, cat in cats.items():
        cat.create_namespace_if_not_exists(NS)
        tname = f"{NS}.bench_{name}_{int(time.time())}"
        tables[name] = cat.create_table(tname, schema, properties=BENCH_TABLE_PROPS)
        orig = cat.commit_table

        def timed(*a: Any, _orig: Any = orig, _name: str = name, **k: Any) -> Any:
            t0 = time.perf_counter()
            try:
                return _orig(*a, **k)
            finally:
                if timing_on[_name]:
                    lat[_name].append((time.perf_counter() - t0) * 1000)

        cat.commit_table = timed
    data = pa.table({"id": pa.array([1, 2, 3], pa.int64())})
    # snapshot summary of the size Spark/Flink writes: engine keys + padding, calibrated on the
    # first (unmeasured) commit of each table
    props: dict[str, dict[str, str]] = {"proxy": {}, "bypass": {}}
    for name in tables:
        tables[name].append(data)
        if BENCH_SNAPSHOT_BYTES:
            props[name] = dict(SPARK_SUMMARY)
            tables[name].append(data, snapshot_properties=props[name])
            size = _snapshot_bytes(_metadata(tables[name].name()[-1])[1][-1])
            pad = BENCH_SNAPSHOT_BYTES - size - len('"capl.bench.pad":"",')
            if pad > 0:
                props[name]["capl.bench.pad"] = "x" * pad
    rng = random.Random(SEED)
    plan = _bench_expire_plan() if BENCH_EXPIRE else None
    n_snaps = {name: len(_metadata(t.name()[-1])[1]) for name, t in tables.items()}
    snaps_at: dict[str, list[int]] = {"proxy": [], "bypass": []}
    n_expires = {"proxy": 0, "bypass": 0}
    win = 100
    meta_at: list[dict[str, int]] = []                    # proxy table: (snapshots, metadata bytes)
    over_before = (_metric("capl_proxy_overhead_seconds_sum"), _metric("capl_proxy_overhead_seconds_count"))
    for i in range(N_BENCH + 50):
        order = ["proxy", "bypass"]
        rng.shuffle(order)
        for name in order:
            timing_on[name] = i >= 50                     # 50 warm-up commits per path
            tables[name].append(data, snapshot_properties=props[name])
            n_snaps[name] += 1
            if i >= 50:
                snaps_at[name].append(n_snaps[name])
            if plan and n_snaps[name] >= plan["expire_at"]:
                # scheduled expire of the scenarios (ADR-008); its commit is not measured
                timing_on[name] = False
                ids = sorted((s.timestamp_ms, s.snapshot_id) for s in tables[name].snapshots())
                drop = [sid for _, sid in ids[:len(ids) - plan["retain"]]]
                tables[name].maintenance.expire_snapshots().by_ids(drop).commit()
                n_snaps[name] = len(tables[name].snapshots())
                n_expires[name] += 1
        if i >= 50 and (i - 50) % win in (0, win - 1):  # metadata size at window start and end
            nbytes, snaps = _metadata(tables["proxy"].name()[-1])
            meta_at.append({"commit": i - 50, "snapshots": len(snaps), "metadata_bytes": nbytes})
    over_after = (_metric("capl_proxy_overhead_seconds_sum"), _metric("capl_proxy_overhead_seconds_count"))
    p, b = lat["proxy"], lat["bypass"]
    rep: dict[str, Any] = {
        "n_per_path": len(p), "unit": "ms",
        "proxy": {"p50": _q(p, .5), "p95": _q(p, .95), "p99": _q(p, .99), "mean": statistics.fmean(p)},
        "bypass": {"p50": _q(b, .5), "p95": _q(b, .95), "p99": _q(b, .99), "mean": statistics.fmean(b)},
        "proxy_internal_overhead_mean_ms": 1000 * (over_after[0] - over_before[0])
        / max(1.0, over_after[1] - over_before[1]),
    }
    rep["overhead_p50"] = rep["proxy"]["p50"] - rep["bypass"]["p50"]
    rep["overhead_p95"] = rep["proxy"]["p95"] - rep["bypass"]["p95"]
    # whole series, for reference only: its value depends on the series length (S2-R2-01)
    boot = random.Random(SEED)
    d50, d95 = [], []
    for _ in range(2000):
        ps = [boot.choice(p) for _ in p]
        bs = [boot.choice(b) for _ in b]
        d50.append(_q(ps, .5) - _q(bs, .5))
        d95.append(_q(ps, .95) - _q(bs, .95))
    rep["overhead_p50_ci95"] = [_q(d50, .025), _q(d50, .975)]
    rep["overhead_p95_ci95"] = [_q(d95, .025), _q(d95, .975)]
    # windows of 100 commits; the table size of a window is the median snapshot count of its
    # commits (sawtooth with the scheduled expire); paired medians (proxy_i - bypass_i of
    # interleaved commits) are robust to window-level drift of the catalog (r4: -32.4 ms)
    windows = []
    for k, w0 in enumerate(range(0, len(p) - win + 1, win)):
        pw, bw = p[w0:w0 + win], b[w0:w0 + win]
        diffs = [x - y for x, y in zip(pw, bw, strict=True)]
        ends = [m["metadata_bytes"] for m in meta_at if w0 <= m["commit"] < w0 + win]
        windows.append({"w0": w0, "snapshots_mid": int(statistics.median(snaps_at["proxy"][w0:w0 + win])),
                        "snapshots_max": max(snaps_at["proxy"][w0:w0 + win]),
                        "metadata_bytes_max": max(ends) if ends else None,
                        "proxy_p50": _q(pw, .5), "bypass_p50": _q(bw, .5),
                        "diff_p50": _q(pw, .5) - _q(bw, .5),
                        "paired_median_diff": statistics.median(diffs),
                        "paired_median_ci95": _paired_median_ci(diffs, random.Random(SEED + k), 1000)})
    xs = [w["snapshots_mid"] for w in windows]
    ys = [w["paired_median_diff"] for w in windows]
    mx, my = statistics.fmean(xs), statistics.fmean(ys)
    sxx = sum((x - mx) ** 2 for x in xs) or 1.0
    slope = sum((x - mx) * (y - my) for x, y in zip(xs, ys, strict=True)) / sxx
    max_snaps = max(snaps_at["proxy"] + snaps_at["bypass"])
    rep["trend"] = {"statistic": "paired_median_diff", "windows": windows,
                    "slope_ms_per_1000_snapshots": 1000 * slope, "intercept_ms": my - slope * mx,
                    "fitted_at_max_snapshots_ms": my + slope * (max_snaps - mx),   # reference only
                    "r": slope * (sxx ** 0.5) / (sum((y - my) ** 2 for y in ys) ** 0.5 or 1.0)}
    over = [w for w in windows if w["paired_median_diff"] > 5.0]
    rep["first_window_over_5ms"] = over[0] if over else None
    # metadata bytes per snapshot (linear: every snapshot adds one entry to snapshots and snapshot-log)
    mx2 = statistics.fmean(m["snapshots"] for m in meta_at)
    my2 = statistics.fmean(m["metadata_bytes"] for m in meta_at)
    sxx2 = sum((m["snapshots"] - mx2) ** 2 for m in meta_at) or 1.0
    per_snap = sum((m["snapshots"] - mx2) * (m["metadata_bytes"] - my2) for m in meta_at) / sxx2
    rep["metadata_bytes"] = {"per_snapshot": per_snap, "intercept": my2 - per_snap * mx2, "samples": meta_at}
    nbytes, snaps = _metadata(tables["proxy"].name()[-1])
    rep["commit_response_bytes_at_end"] = nbytes
    rep["snapshot_entry_bytes"] = {"target": BENCH_SNAPSHOT_BYTES,
                                   "mean": statistics.fmean(_snapshot_bytes(s) for s in snaps),
                                   "summary_padding": len(props["proxy"].get("capl.bench.pad", ""))}
    # NFR-1 on the top band of the history (S2-R3-01)
    top = plan["expire_at"] if plan else max_snaps
    band = [w for w in windows if w["snapshots_mid"] >= BENCH_BAND * top]
    band_diffs = [p[i] - b[i] for w in band for i in range(w["w0"], w["w0"] + win)]
    band_p = [p[i] for w in band for i in range(w["w0"], w["w0"] + win)]
    band_b = [b[i] for w in band for i in range(w["w0"], w["w0"] + win)]
    rep["top_band"] = {
        "snapshots_from": BENCH_BAND * top, "snapshots_to": top, "windows": [w["w0"] for w in band],
        "n_pairs": len(band_diffs),
        "paired_median_diff": statistics.median(band_diffs) if band_diffs else None,
        "paired_median_ci95": (_paired_median_ci(band_diffs, random.Random(SEED), 4000)
                               if band_diffs else None),
        "p95_diff": _q(band_p, .95) - _q(band_b, .95) if band_diffs else None,
        "max_window_paired_median": max(w["paired_median_diff"] for w in band) if band else None}
    rep["bench_table_properties"] = BENCH_TABLE_PROPS
    rep["bench_expire"] = {**(plan or {"enabled": False}), "n_expires": n_expires, "max_snapshots": max_snaps}
    tp = tables["proxy"].refresh()
    rep["manifest_list_entries_at_end"] = len(tp.current_snapshot().manifests(tp.io))
    _save("overhead.json", rep)
    _save("overhead_raw.json", lat)
    assert len(p) == len(b) == N_BENCH
    if BENCH_SNAPSHOT_BYTES:      # the bench writes snapshots of the size it claims (±5 %)
        sb = rep["snapshot_entry_bytes"]
        assert abs(sb["mean"] - BENCH_SNAPSHOT_BYTES) <= 0.05 * BENCH_SNAPSHOT_BYTES, sb
    if plan:                      # a full saw cycle per path: up to expire_at, down, up again
        assert min(n_expires.values()) >= 2 and max_snaps >= plan["expire_at"], rep["bench_expire"]
    tb = rep["top_band"]
    assert tb["n_pairs"] >= BENCH_MIN_BAND_PAIRS, tb
    assert tb["paired_median_ci95"][1] <= 5.0, tb
    assert tb["p95_diff"] <= 15.0, tb
    # whole series (weaker than the band; kept)
    assert rep["overhead_p50"] <= 5.0, rep
    assert rep["overhead_p95"] <= 15.0, rep


# ---------------------------------------------------------------- fail-open

def test_fail_open_under_load() -> None:
    """Commits from 4 writers run continuously; holds are armed on their slots; the advisor
    component is switched to down / error / slow. Expect: every commit succeeds, nothing is held
    while the advisor is faulty, holds apply again after recovery."""
    import pyarrow as pa
    from pyiceberg.schema import Schema
    from pyiceberg.types import LongType, NestedField

    schema = Schema(NestedField(1, "id", LongType(), required=False))
    data = pa.table({"id": pa.array([1], pa.int64())})
    slots = [f"fo-{i}" for i in range(4)]
    stop = threading.Event()
    phase = {"name": "none"}
    log: list[dict[str, Any]] = []
    errors: list[str] = []

    def writer(slot: str) -> None:
        cat = _pyiceberg(PROXY_URL, slot)
        cat.create_namespace_if_not_exists(NS)
        tbl = cat.create_table(f"{NS}.fo_{slot.replace('-', '_')}_{int(time.time())}", schema)
        while not stop.is_set():
            ph = phase["name"]
            t0 = time.perf_counter()
            try:
                tbl.append(data)
                log.append({"slot": slot, "phase": ph, "ms": (time.perf_counter() - t0) * 1000})
            except Exception as exc:          # recorded, asserted below
                errors.append(f"{slot} {ph}: {exc!r}"[:300])

    threads = [threading.Thread(target=writer, args=(s,), daemon=True) for s in slots]
    for t in threads:
        t.start()
    time.sleep(3)
    fo_before = {r: _metric("capl_fail_open_total", {"stage": "advisor", "reason": r})
                 for r in ("AdvisorUnavailable", "RuntimeError", "TimeoutError")}
    held_before = len([e for e in requests.get(f"{ADV_URL}/_capl/debug/last-commits", timeout=10).json()
                       if e.get("held")])
    for fault in ("down", "error", "slow"):
        requests.put(f"{ADV_URL}/v1/debug/advisor", json={"fault": fault}, timeout=10).raise_for_status()
        phase["name"] = fault
        t_end = time.time() + 8
        while time.time() < t_end:            # keep re-arming 5 s holds on all slots
            for s in slots:
                requests.post(f"{ADV_URL}/v1/debug/hold", json={"slot": s, "max_ms": 5000}, timeout=10)
            time.sleep(0.5)
    requests.post(f"{ADV_URL}/v1/debug/run-stop", timeout=10)
    requests.put(f"{ADV_URL}/v1/debug/advisor", json={"fault": "none"}, timeout=10)
    phase["name"] = "recovered"
    for s in slots:
        requests.post(f"{ADV_URL}/v1/debug/hold", json={"slot": s, "max_ms": 300}, timeout=10)
    time.sleep(3)
    stop.set()
    for t in threads:
        t.join(timeout=30)
    fo_after = {r: _metric("capl_fail_open_total", {"stage": "advisor", "reason": r}) for r in fo_before}
    by_phase: dict[str, list[float]] = {}
    for e in log:
        by_phase.setdefault(e["phase"], []).append(e["ms"])
    recent = requests.get(f"{ADV_URL}/_capl/debug/last-commits", timeout=10).json()
    rep = {
        "writers": len(slots), "errors": errors,
        "commits_by_phase": {k: len(v) for k, v in by_phase.items()},
        "max_append_ms_by_phase": {k: round(max(v), 1) for k, v in by_phase.items()},
        "fail_open_increments": {k: fo_after[k] - fo_before[k] for k in fo_before},
        "held_commits_total_in_debug_log": len([e for e in recent if e.get("held")]) - held_before,
    }
    _save("fail_open.json", rep)
    assert not errors, errors[:5]
    for fault in ("down", "error", "slow"):
        assert len(by_phase.get(fault, [])) >= 10, rep
        # a 5 s hold would show up as ≥ 5000 ms appends; fail-open adds at most the 20 ms timeout
        assert max(by_phase[fault]) < 3000, rep
    assert all(v >= 10 for v in rep["fail_open_increments"].values()), rep
    assert rep["held_commits_total_in_debug_log"] >= 1        # holds work again after recovery
