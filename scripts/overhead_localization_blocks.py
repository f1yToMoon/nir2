"""Localization for S2-R1-02: where does the extra commit latency with enrichment on come from?

Runs in the runner container on the bench tables of `make proxy-verify`:
    docker compose ... exec -T -w /workspace -e GIT_SHA=... -e N=120 -e BLOCKS=on,off,noread \
        runner python3 -m scripts.overhead_localization_blocks <out.json>

Modes: on (enrichment with manifest reading), noread (jobs go through the process pool and the
bookkeeping, no manifest is read), off (no enrichment). Per block: mean client latency of the
commit request via proxy and bypass, and what the proxy measured itself — catalog time
(capl_commit_upstream_seconds), response streaming and own time (means over the block).
"""
from __future__ import annotations

import json
import os
import random
import statistics
import sys
import time
from typing import Any

import pyarrow as pa
import requests

from tests.integration.test_proxy_stage2 import (
    ADV_URL,
    DIRECT_URL,
    METRICS_URL,
    NS,
    PROXY_URL,
    SEED,
    _pyiceberg,
)

N = int(os.environ.get("N", "150"))
BLOCKS = os.environ.get("BLOCKS", "on,off,on,off").split(",")
UPSTREAM = ('capl_commit_upstream_seconds_sum{op_class="append"}',
            'capl_commit_upstream_seconds_count{op_class="append"}')
STREAM = ('capl_proxy_phase_seconds_sum{phase="response_stream"}',
          'capl_proxy_phase_seconds_count{phase="response_stream"}')
OWN = ("capl_proxy_overhead_seconds_sum", "capl_proxy_overhead_seconds_count")
KEYS = UPSTREAM + STREAM + OWN


def scrape() -> dict[str, float]:
    out = {}
    for line in requests.get(METRICS_URL, timeout=10).text.splitlines():
        key, _, value = line.rpartition(" ")
        if key in KEYS:
            out[key] = float(value)
    return out


def main(out: str | None) -> None:
    cats = {"proxy": _pyiceberg(PROXY_URL, "bench"), "bypass": _pyiceberg(DIRECT_URL)}
    names = sorted(t[-1] for t in cats["bypass"].list_tables(NS))
    tables = {path: cats[path].load_table(f"{NS}.{[n for n in names if n.startswith(f'bench_{path}_')][-1]}")
              for path in cats}
    lat: dict[str, list[float]] = {"proxy": [], "bypass": []}

    def timed(path: str) -> Any:
        orig = cats[path].commit_table

        def f(*a: Any, **k: Any) -> Any:
            t0 = time.perf_counter()
            try:
                return orig(*a, **k)
            finally:
                lat[path].append((time.perf_counter() - t0) * 1000)
        return f

    for path in cats:
        cats[path].commit_table = timed(path)
    data = pa.table({"id": pa.array([1], pa.int64())})
    rng = random.Random(SEED + 1)
    rows = []
    for mode in BLOCKS:
        requests.put(f"{ADV_URL}/_capl/debug/enricher",
                     params={"on": mode != "off", "read": mode != "noread"}, timeout=10)
        requests.post(f"{ADV_URL}/v1/debug/flush", timeout=600)
        before = scrape()
        for path in lat:
            lat[path].clear()
        for _ in range(N):
            order = ["proxy", "bypass"]
            rng.shuffle(order)
            for path in order:
                tables[path].append(data)
        after = scrape()
        d = {k: after[k] - before.get(k, 0.0) for k in KEYS}
        p, b = lat["proxy"], lat["bypass"]
        r = {"enrich": mode, "snapshots": len(tables["proxy"].snapshots()),
             "proxy_mean": statistics.fmean(p), "bypass_mean": statistics.fmean(b),
             "upstream_mean": d[UPSTREAM[0]] / d[UPSTREAM[1]] * 1000,
             "response_stream_mean": d[STREAM[0]] / d[STREAM[1]] * 1000,
             "proxy_own_mean": d[OWN[0]] / d[OWN[1]] * 1000,
             "paired_median_diff": statistics.median(x - y for x, y in zip(p, b, strict=True))}
        r["proxy_minus_bypass_mean"] = r["proxy_mean"] - r["bypass_mean"]
        r["upstream_minus_bypass_mean"] = r["upstream_mean"] - r["bypass_mean"]
        r["proxy_minus_upstream_mean"] = r["proxy_mean"] - r["upstream_mean"]
        rows.append({k: (round(v, 2) if isinstance(v, float) else v) for k, v in r.items()})
        print(rows[-1], flush=True)
    requests.put(f"{ADV_URL}/_capl/debug/enricher", params={"on": True}, timeout=10)
    if out:
        with open(out, "w") as fh:
            json.dump({"git_sha": os.environ.get("GIT_SHA", "unknown"), "n_per_block_per_path": N,
                       "blocks": rows}, fh, indent=1)


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else None)
