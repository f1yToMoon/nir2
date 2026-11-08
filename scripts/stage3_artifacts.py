"""Save stage-3 evidence for review without access to the live PostgreSQL instance.

Run inside the runner: python3 scripts/stage3_artifacts.py --runs <finished run ids>.
The JSON exports preserve nested values; Parquet exports remain available via make export.
"""
from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import io
import json
import math
import pickle
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from runner.db import DB
from runner.env import REPO, Env
from runner.export import TABLES

EVENT_ROWS_PER_FILE = 500_000


def digest(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def save_bytes(target: Path, name: str, raw: bytes, rows: int,
               files: dict[str, Any]) -> None:
    path = target / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(gzip.compress(raw, mtime=0))
    files[name] = {"rows": rows, "sha256": digest(path),
                   "uncompressed_sha256": hashlib.sha256(raw).hexdigest()}


def save_json(target: Path, name: str, value: Any, rows: int,
              files: dict[str, Any]) -> None:
    save_bytes(target, name, json.dumps(value, default=str, ensure_ascii=False).encode(), rows, files)


def save_ledger(source: Path, target: Path, files: dict[str, Any]) -> dict[str, Any]:
    # Only this project's locally generated ledger is loaded; remote review uses JSON/CSV.
    with source.open("rb") as fh:
        saved = pickle.load(fh)
    led = saved["ledger"]
    state = {k: v for k, v in saved.items() if k != "ledger"}
    state["ledger"] = {k: v for k, v in vars(led).items()
                       if k not in ("stream_events", "_inserted_index")}
    state["ledger"]["deleted_ids"] = sorted(led.deleted_ids)
    save_json(target, "ledger.json.gz", state, 1, files)
    sources = []
    for index, (origin, events) in enumerate(led.stream_events):
        n = len(events.ids)
        assert all(len(a) == n for a in
                   (events.keys, events.part_idx, events.generated_at, events.committed_at))
        info = {"origin": origin, "rows": n, "labels": events.labels, "files": [],
                "missing_generated_at": 0, "missing_committed_at": 0}
        # Bound each public artifact below GitHub's per-file limit, without sampling rows.
        for part, begin in enumerate(range(0, n, EVENT_ROWS_PER_FILE)):
            name = f"stream-{index:02d}-{part:04d}.csv.gz"
            end = min(n, begin + EVENT_ROWS_PER_FILE)
            with (
                (target / name).open("wb") as raw,
                gzip.GzipFile(filename="", fileobj=raw, mode="wb", compresslevel=6, mtime=0) as gz,
                io.TextIOWrapper(gz, encoding="utf-8", newline="") as text,
            ):
                writer = csv.writer(text)
                writer.writerow(["event_id", "key", "partition", "generated_at", "committed_at"])
                for i in range(begin, end):
                    writer.writerow([events.ids[i], events.keys[i], events.labels[events.part_idx[i]],
                                     events.generated_at[i], events.committed_at[i]])
                    info["missing_generated_at"] += not math.isfinite(events.generated_at[i])
                    info["missing_committed_at"] += not math.isfinite(events.committed_at[i])
            files[name] = {"rows": end - begin, "sha256": digest(target / name)}
            info["files"].append(name)
        sources.append(info)
    save_json(target, "stream-sources.json.gz", sources, len(sources), files)
    return {"source_pickle_sha256": digest(source), "stream_events": sum(s["rows"] for s in sources)}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--runs", nargs="+", required=True)
    parser.add_argument("--out", type=Path, default=REPO / "results" / "stage-3" / "evidence")
    args = parser.parse_args()
    root = args.out
    root.mkdir(parents=True, exist_ok=True)
    manifest_path = root / "manifest.json"
    manifest = json.loads(manifest_path.read_text()) if manifest_path.exists() else {}
    if not isinstance(manifest, dict):
        raise ValueError("invalid existing manifest; preserve it and choose a new --out")
    env = Env()
    db = DB(env.pg_dsn, env.pg_schema)
    try:
        for rid in args.runs:
            if not rid or Path(rid).name != rid or rid in (".", ".."):
                raise ValueError(f"invalid run directory name: {rid!r}")
            run = db.query(TABLES["runs"], [rid])
            if len(run) != 1 or run[0]["status"] != "finished":
                raise ValueError(f"{rid}: expected one finished run")
            target = root / rid
            if rid in manifest:
                raise FileExistsError(f"{rid}: evidence already recorded; choose a new --out")
            target.mkdir(exist_ok=False)
            files = {}
            for name, query in TABLES.items():
                rows = db.query(query, [rid])
                save_json(target, f"{name}.json.gz", rows, len(rows), files)
            source = REPO / "data" / "runs" / rid
            intents = source / "intents.json"
            save_bytes(target, "intents.json.gz", intents.read_bytes(),
                       len(json.loads(intents.read_text())), files)
            for journal in sorted(source.glob("*.json")):
                if journal.name != "intents.json":
                    save_bytes(target, f"journals/{journal.name}.gz", journal.read_bytes(), 1, files)
            result = REPO / "results" / "stage-3" / "runs" / f"{rid}.json"
            save_bytes(target, "run-result.json.gz", result.read_bytes(), 1, files)
            golden = run[0]["params"]["golden"]["data"]
            if golden not in ("_data.json", "_data_lite.json"):
                raise ValueError(f"unexpected golden data filename: {golden!r}")
            save_bytes(target, "golden-data.json.gz", (REPO / "data/golden" / golden).read_bytes(), 1, files)
            ledger = save_ledger(source / "ledger.pkl", target, files)
            manifest[rid] = {"format_version": 2, "git_sha": run[0]["git_sha"],
                              **ledger, "files": files}
            temporary = root / ".manifest.json.tmp"
            temporary.write_text(json.dumps(manifest, indent=2))
            temporary.replace(manifest_path)
            print(f"saved {rid}: {len(files)} files", flush=True)
    finally:
        db.close()


if __name__ == "__main__":
    main()
