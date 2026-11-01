"""Localization for S2-R1-02: client wall-clock times of each proxied commit against the proxy's
journal timestamps (same host clock):
    pre      = commit_attempts.received_at - client start (client send, connection, uvicorn)
    proxy_in = forwarded_at - received_at
    catalog  = responded_at - forwarded_at (catalog until response headers)
    post     = client end - responded_at (response transfer and the client parsing it)

    docker compose ... exec -T -w /workspace -e GIT_SHA=... -e N=100 \
        runner python3 -m scripts.overhead_localization_timestamps <out.json>
"""
from __future__ import annotations

import json
import os
import random
import statistics
import sys
import time
from typing import Any

import psycopg
import pyarrow as pa
import requests

from tests.integration.test_proxy_stage2 import (
    ADV_URL,
    DIRECT_URL,
    NS,
    PG_DSN,
    PROXY_URL,
    SEED,
    _pyiceberg,
)

N = int(os.environ.get("N", "100"))
BLOCKS = os.environ.get("BLOCKS", "on,off,on,off").split(",")
JOURNAL = ("select new_snapshot_id, extract(epoch from received_at), extract(epoch from forwarded_at),"
           " extract(epoch from responded_at) from stand.commit_attempts where new_snapshot_id = any(%s)")


def main(out: str | None) -> None:
    cats = {"proxy": _pyiceberg(PROXY_URL, "bench"), "bypass": _pyiceberg(DIRECT_URL)}
    names = sorted(t[-1] for t in cats["bypass"].list_tables(NS))
    tables = {path: cats[path].load_table(f"{NS}.{[n for n in names if n.startswith(f'bench_{path}_')][-1]}")
              for path in cats}
    last: dict[str, tuple[float, float]] = {}

    def timed(path: str) -> Any:
        orig = cats[path].commit_table

        def f(*a: Any, **k: Any) -> Any:
            t0 = time.time()
            try:
                return orig(*a, **k)
            finally:
                last[path] = (t0, time.time())
        return f

    for path in cats:
        cats[path].commit_table = timed(path)
    data = pa.table({"id": pa.array([1], pa.int64())})
    rng = random.Random(SEED + 2)
    rows = []
    for mode in BLOCKS:
        requests.put(f"{ADV_URL}/_capl/debug/enricher", params={"on": mode != "off"}, timeout=10)
        requests.post(f"{ADV_URL}/v1/debug/flush", timeout=600)
        recs = []
        for _ in range(N):
            order = ["proxy", "bypass"]
            rng.shuffle(order)
            rec = {}
            for path in order:
                tables[path].append(data)
                rec[path] = (*last[path], tables[path].current_snapshot().snapshot_id)
            recs.append(rec)
        requests.post(f"{ADV_URL}/v1/debug/flush", timeout=600)
        with psycopg.connect(PG_DSN) as conn:
            journal = {sid: (float(a), float(f), float(r)) for sid, a, f, r in
                       conn.execute(JOURNAL, ([rec["proxy"][2] for rec in recs],)).fetchall()}
        seg: dict[str, list[float]] = {
            k: [] for k in ("pre", "proxy_in", "catalog", "post", "total", "bypass")}
        for rec in recs:
            t0, t1, sid = rec["proxy"]
            if sid not in journal:
                continue
            rcv, fwd, rsp = journal[sid]
            seg["pre"].append((rcv - t0) * 1000)
            seg["proxy_in"].append((fwd - rcv) * 1000)
            seg["catalog"].append((rsp - fwd) * 1000)
            seg["post"].append((t1 - rsp) * 1000)
            seg["total"].append((t1 - t0) * 1000)
            seg["bypass"].append((rec["bypass"][1] - rec["bypass"][0]) * 1000)
        row: dict[str, Any] = {"mode": mode, "n": len(seg["pre"]),
                               "snapshots": len(tables["proxy"].snapshots())}
        row.update({f"{k}_p50": round(statistics.median(v), 2) for k, v in seg.items()})
        row.update({f"{k}_mean": round(statistics.fmean(v), 2) for k, v in seg.items()})
        rows.append(row)
        print(row, flush=True)
    requests.put(f"{ADV_URL}/_capl/debug/enricher", params={"on": True}, timeout=10)
    if out:
        with open(out, "w") as fh:
            json.dump({"git_sha": os.environ.get("GIT_SHA", "unknown"), "blocks": rows}, fh, indent=1)


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else None)
