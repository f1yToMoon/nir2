"""Ablation for S2-R1-02: does enrichment of proxied commits cause the overhead growth with table size?

Runs in the runner container after `make proxy-verify` (uses its bench tables with ~2000 snapshots):
    docker compose ... exec -T -e GIT_SHA=... runner python3 -m scripts.overhead_enricher_ablation <out.json>

Blocks of N commits per path (proxy/bypass interleaved, order shuffled by seed), enrichment
alternately on and off (ABAB...), difference of medians per block.
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

from tests.integration.test_proxy_stage2 import ADV_URL, DIRECT_URL, NS, PROXY_URL, SEED, _pyiceberg

N = int(os.environ.get("ABLATION_COMMITS", "150"))
BLOCKS = os.environ.get("ABLATION_BLOCKS", "on,off,on,off,on,off").split(",")


def main(out: str) -> None:
    cats = {"proxy": _pyiceberg(PROXY_URL, "bench"), "bypass": _pyiceberg(DIRECT_URL)}
    names = sorted(t[-1] for t in cats["bypass"].list_tables(NS))
    tables = {path: cats[path].load_table(f"{NS}.{[n for n in names if n.startswith(f'bench_{path}_')][-1]}")
              for path in ("proxy", "bypass")}
    lat: dict[str, list[float]] = {}

    def timed(path: str) -> Any:
        orig = cats[path].commit_table

        def f(*a: Any, **k: Any) -> Any:
            t0 = time.perf_counter()
            try:
                return orig(*a, **k)
            finally:
                lat.setdefault(path, []).append((time.perf_counter() - t0) * 1000)
        return f

    for path in cats:
        cats[path].commit_table = timed(path)
    data = pa.table({"id": pa.array([1], pa.int64())})
    rng = random.Random(SEED)
    rows = []
    for mode in BLOCKS:
        requests.put(f"{ADV_URL}/_capl/debug/enricher", params={"on": mode == "on"}, timeout=10)
        requests.post(f"{ADV_URL}/v1/debug/flush", timeout=600)      # start each block with an empty queue
        lat.clear()
        for _ in range(N):
            order = ["proxy", "bypass"]
            rng.shuffle(order)
            for path in order:
                tables[path].append(data)
        p, b = lat["proxy"], lat["bypass"]
        rows.append({"enrich": mode, "snapshots_proxy_table": len(tables["proxy"].snapshots()),
                     "proxy_p50": statistics.median(p), "bypass_p50": statistics.median(b),
                     "diff_p50": statistics.median(p) - statistics.median(b),
                     "paired_median_diff": statistics.median(x - y for x, y in zip(p, b, strict=True))})
        print(rows[-1], flush=True)
    requests.put(f"{ADV_URL}/_capl/debug/enricher", params={"on": True}, timeout=10)
    summary = {m: statistics.fmean(r["diff_p50"] for r in rows if r["enrich"] == m) for m in ("on", "off")}
    doc = {"git_sha": os.environ.get("GIT_SHA", "unknown"), "commits_per_block_per_path": N,
           "blocks": rows, "mean_diff_p50_by_mode": summary}
    with open(out, "w") as fh:
        json.dump(doc, fh, indent=1)
    print(summary)


if __name__ == "__main__":
    main(sys.argv[1])
