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
N_FOOTPRINT_SAMPLE = int(os.environ.get("STAGE2_FOOTPRINT_SAMPLE", "150"))
OUT = Path(os.environ.get("STAGE2_OUT",
                          f"/workspace/results/stage-2/{os.environ.get('RESULTS_NAME', 'run')}"))
NS = "stage2"
RUN = f"stage2-{int(time.time())}"
SEED = 2026


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


def _pyiceberg_footprint(tbl: Any, sid: int) -> dict[str, tuple[int, int, int]]:
    """partition → (added data, deleted data, added delete files) of snapshot `sid`."""
    ents = tbl.inspect.entries(snapshot_id=sid).to_pylist()
    acc: dict[str, list[int]] = {}
    for e in ents:
        if e["snapshot_id"] != sid or e["status"] not in (1, 2):
            continue
        df = e["data_file"]
        key = _part_str(df["partition"])
        a = acc.setdefault(key, [0, 0, 0])
        if df["content"] == 0:
            a[0 if e["status"] == 1 else 1] += 1
        elif e["status"] == 1:
            a[2] += 1
    return {k: (v[0], v[1], v[2]) for k, v in acc.items()}


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
        got = {r[0]: (r[1], r[2], r[3]) for r in _pq(
            "SELECT partition, added_data_files, deleted_data_files, added_delete_files "
            "FROM stand.commit_partitions WHERE table_name = %s AND snapshot_id = %s",
            f"{NS}.{table}", sid)}
        n_parts += len(exp)
        if exp != got:
            mismatches.append({"snapshot_id": sid, "pyiceberg": exp, "capl": got})
    ok_after = _metric("capl_body_integrity_total", {"result": "ok"})
    bad_after = _metric("capl_body_integrity_total", {"result": "mismatch"})
    report = {
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
    assert report["integrity_ok"] >= len(attempts) and report["integrity_mismatch"] == 0


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
            hdrs = sorted((k.lower(), v) for k, v in r.headers.items() if k.lower() != "date")
            out.append((r.status_code, hdrs, hashlib.sha256(r.content).hexdigest(), len(r.content)))
        rows.append({"method": method, "path": path, "proxy": out[0][::2], "direct": out[1][::2],
                     "same_status": out[0][0] == out[1][0], "same_headers": out[0][1] == out[1][1],
                     "same_body": out[0][2] == out[1][2]})
    _save("responses_identical.json", rows)
    statuses = {r["direct"][0] for r in rows}
    assert {200, 404, 409} <= statuses, statuses           # errors are passed through as well
    bad = [r for r in rows if not (r["same_status"] and r["same_headers"] and r["same_body"])]
    assert not bad, bad


# ---------------------------------------------------------------- NFR-1 overhead

def _q(xs: list[float], q: float) -> float:
    s = sorted(xs)
    return s[min(len(s) - 1, int(q * len(s)))]


def test_overhead_bypass_vs_proxy() -> None:
    """Latency of the commit request (RestCatalog.commit_table: one POST) for fast appends of the
    same data, alternating proxy/bypass, N_BENCH commits each; overhead = proxy - bypass."""
    import pyarrow as pa
    from pyiceberg.schema import Schema
    from pyiceberg.types import LongType, NestedField

    schema = Schema(NestedField(1, "id", LongType(), required=False))
    cats = {"proxy": _pyiceberg(PROXY_URL, "bench"), "bypass": _pyiceberg(DIRECT_URL)}
    tables, lat = {}, {"proxy": [], "bypass": []}
    for name, cat in cats.items():
        cat.create_namespace_if_not_exists(NS)
        tname = f"{NS}.bench_{name}_{int(time.time())}"
        tables[name] = cat.create_table(tname, schema)
        orig = cat.commit_table

        def timed(*a: Any, _orig: Any = orig, _name: str = name, **k: Any) -> Any:
            t0 = time.perf_counter()
            try:
                return _orig(*a, **k)
            finally:
                if timed.on:  # type: ignore[attr-defined]
                    lat[_name].append((time.perf_counter() - t0) * 1000)

        timed.on = False  # type: ignore[attr-defined]
        cat.commit_table = timed
    data = pa.table({"id": pa.array([1, 2, 3], pa.int64())})
    rng = random.Random(SEED)
    over_before = (_metric("capl_proxy_overhead_seconds_sum"), _metric("capl_proxy_overhead_seconds_count"))
    for i in range(N_BENCH + 50):
        order = ["proxy", "bypass"]
        rng.shuffle(order)
        for name in order:
            cats[name].commit_table.on = i >= 50          # 50 warm-up commits per path
            tables[name].append(data)
    over_after = (_metric("capl_proxy_overhead_seconds_sum"), _metric("capl_proxy_overhead_seconds_count"))
    p, b = lat["proxy"], lat["bypass"]
    rep = {
        "n_per_path": len(p), "unit": "ms",
        "proxy": {"p50": _q(p, .5), "p95": _q(p, .95), "p99": _q(p, .99), "mean": statistics.fmean(p)},
        "bypass": {"p50": _q(b, .5), "p95": _q(b, .95), "p99": _q(b, .99), "mean": statistics.fmean(b)},
        "proxy_internal_overhead_mean_ms": 1000 * (over_after[0] - over_before[0])
        / max(1.0, over_after[1] - over_before[1]),
    }
    rep["overhead_p50"] = rep["proxy"]["p50"] - rep["bypass"]["p50"]
    rep["overhead_p95"] = rep["proxy"]["p95"] - rep["bypass"]["p95"]
    # bootstrap 95 % CI of the p50/p95 differences
    boot = random.Random(SEED)
    d50, d95 = [], []
    for _ in range(2000):
        ps = [boot.choice(p) for _ in p]
        bs = [boot.choice(b) for _ in b]
        d50.append(_q(ps, .5) - _q(bs, .5))
        d95.append(_q(ps, .95) - _q(bs, .95))
    rep["overhead_p50_ci95"] = [_q(d50, .025), _q(d50, .975)]
    rep["overhead_p95_ci95"] = [_q(d95, .025), _q(d95, .975)]
    rep["nfr1"] = {"p50_le_5ms": rep["overhead_p50"] <= 5, "p95_le_15ms": rep["overhead_p95"] <= 15}
    _save("overhead.json", rep)
    _save("overhead_raw.json", lat)
    assert len(p) == len(b) == N_BENCH
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
