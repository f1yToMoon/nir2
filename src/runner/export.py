"""Export of run traces to Parquet (tz/06 step 3.8): one directory per run.

    python3 -m runner.export --run <run_id> --out data/traces   (empty --run: all finished runs)
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

TABLES = {
    "runs": "SELECT * FROM {s}.runs WHERE run_id = %s",
    "ops": "SELECT * FROM {s}.ops WHERE run_id = %s",
    "commit_attempts": "SELECT * FROM {s}.commit_attempts WHERE run_id = %s",
    "commit_events": "SELECT * FROM {s}.commit_events WHERE run_id = %s",
    "commit_partitions": "SELECT p.* FROM {s}.commit_partitions p JOIN {s}.commit_events e "
                         "USING (table_name, snapshot_id) WHERE e.run_id = %s",
    "table_health": "SELECT * FROM {s}.table_health WHERE run_id = %s",
    "table_meta": "SELECT * FROM {s}.table_meta WHERE run_id = %s",
    "orphan_scan": "SELECT * FROM {s}.orphan_scan WHERE run_id = %s",
    "run_summary": "SELECT * FROM {s}.run_summary WHERE run_id = %s",
}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run", default="")
    ap.add_argument("--out", default="data/traces")
    args = ap.parse_args(argv)
    from runner.db import DB
    from runner.env import Env
    env = Env()
    db = DB(env.pg_dsn, env.pg_schema)
    runs = [args.run] if args.run else [r["run_id"] for r in db.query(
        "SELECT run_id FROM {s}.runs WHERE status = 'finished' ORDER BY started_at")]
    for rid in runs:
        d = Path(args.out) / rid
        d.mkdir(parents=True, exist_ok=True)
        for name, q in TABLES.items():
            df = pd.DataFrame(db.query(q, [rid]))
            for c in df.columns:
                if df[c].dtype == object:
                    df[c] = df[c].map(lambda v: v if v is None or isinstance(v, (str, int, float))
                                      else str(v))
            df.to_parquet(d / f"{name}.parquet", index=False)
        print("exported", rid, flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
