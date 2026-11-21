"""Verify every exported observation; run after all seven runs and make export.

PYTHONPATH=src python verify_evidence.py
Writes a new verification.json; never modifies evidence or previous verification.
"""
import gzip
import hashlib
import json
import subprocess
from pathlib import Path
from datetime import datetime

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from runner.history import history_bounds
from runner.health import validate_health_rows
from runner.metadata import verify_metadata_inventory

root = Path(__file__).resolve().parent
repo = root.parents[2]
output = root / "verification.json"
assert not output.exists(), "preserve the existing verification"
plan = json.loads((root / "plan-amendment-20261121.json").read_text())
assert hashlib.sha256(Path(plan['previous_amendment']).read_bytes()).hexdigest() == plan['previous_amendment_sha256']
manifest = json.loads((root / "evidence/manifest.json").read_text())
assert set(manifest) == {r["run_id"] for r in plan["runs"]}
out = {}
for run in plan["runs"]:
    rid = run["run_id"]
    item = manifest[rid]
    target = root / "evidence" / rid
    assert item["git_sha"] == run["git_sha"]
    n_hashes = 0
    for name, expected in item["files"].items():
        blob = (target / name).read_bytes()
        assert len(blob) < 95_000_000, (rid, name, len(blob))
        assert hashlib.sha256(blob).hexdigest() == expected["sha256"], (rid, name)
        if "uncompressed_sha256" in expected:
            raw = gzip.decompress(blob)
            assert hashlib.sha256(raw).hexdigest() == expected["uncompressed_sha256"]
        n_hashes += 1

    def load(name):
        return json.loads(gzip.decompress((target / f"{name}.json.gz").read_bytes()))

    result = load("run-result")
    assert result["validation"]["ok"] is True
    assert result["git_sha"] == run["git_sha"]
    counts = {}
    for name in ("runs", "ops", "commit_attempts", "commit_events", "commit_partitions",
                 "table_health", "table_meta", "orphan_scan", "run_summary"):
        rows = load(name)
        assert len(rows) == item["files"][f"{name}.json.gz"]["rows"]
        parquet = repo / "data/traces" / rid / f"{name}.parquet"
        assert pq.read_metadata(parquet).num_rows == len(rows), str(parquet)
        counts[name] = len(rows)
    ledger_path = repo / "data/runs" / rid / "ledger.pkl"
    assert hashlib.sha256(ledger_path.read_bytes()).hexdigest() == item["source_pickle_sha256"]
    csv_count, delays = 0, []
    for name in item["files"]:
        if not name.endswith(".csv.gz"):
            continue
        for frame in pd.read_csv(target / name, chunksize=500_000, float_precision="round_trip"):
            generated = frame["generated_at"].to_numpy(dtype=float)
            committed = frame["committed_at"].to_numpy(dtype=float)
            assert np.isfinite(generated).all() and np.isfinite(committed).all()
            assert (generated > 0).all() and (committed >= generated).all()
            delays.append(committed - generated)
            csv_count += len(frame)
    fresh = result["stream_freshness"]
    assert fresh["complete"] is True
    assert csv_count == item["stream_events"] == fresh["n_expected"] == fresh["n_samples"]
    assert csv_count > 0
    latency = np.concatenate(delays)
    p95 = float(np.percentile(latency, 95))
    p99 = float(np.percentile(latency, 99))
    maximum = float(latency.max())
    assert p95 == fresh["p95_s"], (rid, p95, fresh["p95_s"])
    assert 0 <= p95 <= p99 <= maximum
    meta = sorted(load("table_meta"), key=lambda m: m["at"])
    assert meta[0]["snapshots"] == 1
    requests = [r for r in load("commit_attempts") if r["table_name"] == "bench.events"
                and r["http_status"] is not None and 200 <= r["http_status"] < 300]
    bounds = history_bounds(int(meta[0]["current_snapshot_id"]), requests)
    final = int(meta[-1]["snapshots"])
    assert bounds["lower"]["final_snapshots"] == bounds["upper"]["final_snapshots"] == final
    assert bounds["upper"]["peak_snapshots"] <= 1452
    assert max(m["metadata_bytes"] for m in meta) <= 1_510_000
    sample_rows = [{**m, **{key: datetime.fromisoformat(m[key]) for key in
                   ('at', 'sample_started_at', 'metadata_observed_at')}} for m in meta]
    commit_rows = [{**r, 'forwarded_at': datetime.fromisoformat(r['forwarded_at'])} for r in requests]
    coverage = validate_health_rows(sample_rows, commit_rows, datetime.fromisoformat(result['t0']),
                                    datetime.fromisoformat(result['t_end']), 30)
    assert coverage['ok'], (rid, coverage)
    inventory = json.loads(gzip.decompress((target / 'journals/metadata-inventory.json.gz').read_bytes()))
    byte_bound = verify_metadata_inventory(inventory, target / 'metadata')
    assert byte_bound['complete'] and byte_bound['max_metadata_bytes'] <= 1_510_000
    assert not any(o["outcome"] in ("error", "commit_unknown", "history_unresolved")
                   for o in load("ops"))
    out[rid] = {"git_sha": run["git_sha"], "verified_hashes": n_hashes, "json_parquet_rows": counts,
                "stream_rows": csv_count, "p95_from_full_csv_s": p95,
                "p99_from_full_csv_s": p99, "max_from_full_csv_s": maximum,
                "p95_recorded_s": fresh["p95_s"], "history": bounds,
                "observed_final_snapshots": final, "health_coverage": coverage,
                "metadata_versions": len(inventory["files"]),
                "metadata_bytes_upper_bound": byte_bound["max_metadata_bytes"]}
output.write_text(json.dumps(out, default=str, indent=2))
print(json.dumps({rid: {k: v for k, v in row.items() if k != "json_parquet_rows"}
                  for rid, row in out.items()}, default=str, indent=2))
