"""Resumable series of runs from config/experiments/matrix.yaml (tz/06 step 3.8, tz/05 §5).

    python3 -m runner.batch --matrix config/experiments/matrix.yaml --series E3_main [--dry-run]

Each run is a separate process (`runner.main run`): one Spark application and one run id per run
(the run id is a static header of the slot catalogs). Finished runs are skipped only after
validation of the saved result; an invalid finished result stops resumption without overwriting it.
"""
from __future__ import annotations

import argparse
import itertools
import subprocess
import sys
from pathlib import Path
from typing import Any

import yaml

from runner.resume import RESULTS_DIR, require_valid_result


def expand(matrix: dict[str, Any], series: str) -> list[dict[str, Any]]:
    d = matrix.get("defaults", {})
    s = matrix["series"][series]
    seeds = s.get("seeds", d.get("seeds", [11]))
    out: list[dict[str, Any]] = []
    for scen, r, seed in itertools.product(s["scenarios"], s["runs"], seeds):
        levels = r.get("concurrency", s.get("concurrency", ["mid"]))
        for lv in levels:
            short = scen.split("_")[0]
            variant = r.get("variant", s.get("variant"))
            tag = f"-{r['tag']}" if r.get("tag") else ""
            v = f"-{variant}" if variant else ""
            routing = r.get("routing", d.get("routing", "proxy"))
            out.append({"run_id": f"{series}-{short}-{r['policy']}-{lv}-s{seed}{v}{tag}"
                                  + ("-bypass" if routing == "bypass" else ""),
                        "scenario": scen, "policy": r["policy"], "level": lv, "seed": seed,
                        "variant": variant, "routing": routing, "overrides": s.get("overrides")})
    return out


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--matrix", required=True)
    ap.add_argument("--series", required=True)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args(argv)
    runs = expand(yaml.safe_load(Path(args.matrix).read_text()), args.series)
    from runner.db import DB
    from runner.env import Env
    env = Env()
    db = DB(env.pg_dsn, env.pg_schema)
    try:
        done = {r["run_id"] for r in db.query("SELECT run_id FROM {s}.runs WHERE status = 'finished'")}
    finally:
        db.close()
    rc = 0
    for r in runs:
        if r["run_id"] in done:
            require_valid_result(RESULTS_DIR / f"{r['run_id']}.json")
            print("skip finished", r["run_id"], flush=True)
            continue
        cmd = [sys.executable, "-m", "runner.main", "run", "--scenario", r["scenario"], "--policy",
               r["policy"], "--concurrency", r["level"], "--seed", str(r["seed"]), "--run-id",
               r["run_id"], "--series", args.series]
        if r["variant"]:
            cmd += ["--variant", r["variant"]]
        if r["overrides"]:
            import json
            cmd += ["--overrides", json.dumps(r["overrides"])]
        print(" ".join(cmd), flush=True)
        if not args.dry_run:
            env_vars = None
            if r["routing"] == "bypass":
                import os
                env_vars = {**os.environ, "CATALOG_ROUTING": "bypass"}
            rc |= subprocess.call(cmd, env=env_vars)
    return rc


if __name__ == "__main__":
    sys.exit(main())
