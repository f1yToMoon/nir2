"""Save stage-3 evidence for review without access to the live PostgreSQL instance.

Run inside the runner: python3 scripts/stage3_artifacts.py --runs <finished run ids>.
The JSON exports preserve nested values; Parquet exports remain available via make export.
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from runner.db import DB
from runner.env import REPO, Env
from runner.export import TABLES


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--runs", nargs="+", required=True)
    args = parser.parse_args()
    env = Env()
    db = DB(env.pg_dsn, env.pg_schema)
    root = REPO / "results" / "stage-3" / "evidence"
    root.mkdir(parents=True, exist_ok=True)
    manifest = {}
    try:
        for rid in args.runs:
            run = db.query(TABLES["runs"], [rid])
            if len(run) != 1 or run[0]["status"] != "finished":
                raise ValueError(f"{rid}: expected one finished run")
            target = root / rid
            target.mkdir(exist_ok=True)
            files = {}
            for name, query in TABLES.items():
                rows = db.query(query, [rid])
                path = target / f"{name}.json.gz"
                content = json.dumps(rows, default=str, ensure_ascii=False).encode()
                path.write_bytes(gzip.compress(content, mtime=0))
                files[path.name] = {"rows": len(rows),
                                    "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
            intents = REPO / "data" / "runs" / rid / "intents.json"
            path = target / "intents.json.gz"
            path.write_bytes(gzip.compress(intents.read_bytes(), mtime=0))
            files[path.name] = {"rows": len(json.loads(intents.read_text())),
                                "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
            manifest[rid] = {"git_sha": run[0]["git_sha"], "files": files}
            print(f"saved {rid}: {len(files)} files", flush=True)
    finally:
        db.close()
    (root / "manifest.json").write_text(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
