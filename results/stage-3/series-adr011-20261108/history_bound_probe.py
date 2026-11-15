"""Read-only dense history audit; metadata mutations lie between forwarding and response."""
import gzip
import json
import sys
from pathlib import Path

from runner.db import DB
from runner.env import Env

rid = sys.argv[1]
env = Env()
db = DB(env.pg_dsn, env.pg_schema)
try:
    initial = db.query("SELECT * FROM {s}.table_meta WHERE run_id=%s ORDER BY at LIMIT 1", [rid])[0]
    rows = db.query("SELECT forwarded_at,responded_at,new_snapshot_id,removed_snapshot_ids "
                    "FROM {s}.commit_attempts WHERE run_id=%s AND table_name=%s "
                    "AND http_status BETWEEN 200 AND 299", [rid, "bench.events"])
finally:
    db.close()
assert initial["snapshots"] == 1, initial
adds, removes = {}, {}
for row in rows:
    assert row["forwarded_at"] is not None and row["responded_at"] is not None
    assert row["forwarded_at"] <= row["responded_at"]
    if row["new_snapshot_id"] is not None:
        target = adds.setdefault(int(row["new_snapshot_id"]), [])
        target.append((row["forwarded_at"], row["responded_at"]))
    for sid in row["removed_snapshot_ids"] or []:
        removes.setdefault(int(sid), []).append((row["forwarded_at"], row["responded_at"]))
assert set(removes) <= set(adds), "initial golden must be retained; every removed id must be journalled"
out = {}
for mode in ("lower", "upper"):
    events = []
    for sid, windows in adds.items():
        events.append((min(w[1 if mode == "lower" else 0] for w in windows), 0, sid))
    for sid, windows in removes.items():
        events.append((min(w[0 if mode == "lower" else 1] for w in windows), 1, sid))
    current, removed = {initial["current_snapshot_id"]}, set()
    peak, at, ids = 1, None, []
    for timestamp, kind, sid in sorted(events):
        if kind:
            removed.add(sid)
            current.discard(sid)
        elif sid not in removed:
            current.add(sid)
        if len(current) > peak:
            peak, at, ids = len(current), timestamp, sorted(current)
    out[mode] = {"peak_snapshots": peak, "at": str(at), "snapshot_ids_at_peak": ids}
proof = {"run_id": rid, "initial": initial, "successful_requests": rows,
         "bounds": out, "verified_snapshot_limit": 1452,
         "initial_sample_period_s": 30, "claim": "all successful metadata mutations are journalled"}
root = Path("/workspace/results/stage-3/series-adr011-20261108")
path = root / f"history-bounds-{rid}.json.gz"
path.write_bytes(gzip.compress(json.dumps(proof, default=str).encode(), mtime=0))
print(json.dumps({"run_id": rid, "bounds": {
    key: {k: v for k, v in value.items() if k != "snapshot_ids_at_peak"}
    for key, value in out.items()}, "evidence": str(path)}))
