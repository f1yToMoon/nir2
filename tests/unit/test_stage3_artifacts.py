"""Portable evidence preserves all events/bytes and cannot replace previous measurements."""
import csv
import gzip
import hashlib
import importlib.util
import io
import json
import pickle
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from runner.invariants import Ledger
from runner.streaming import EventLog

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("stage3_artifacts", ROOT / "scripts/stage3_artifacts.py")
assert spec and spec.loader
artifacts = importlib.util.module_from_spec(spec)
spec.loader.exec_module(artifacts)


def test_portable_export_preserves_events_journals_and_existing_evidence(
    monkeypatch: Any, tmp_path: Path,
) -> None:
    rid = "measured"
    source = tmp_path / "data/runs" / rid
    source.mkdir(parents=True)
    intents = b'[{"op_id":"one","footprint":["p"]}]\n'
    journal = b'{"op_id":"one","deleted_rows":[[1,5,"p"]]}\n'
    (source / "intents.json").write_bytes(intents)
    (source / "one.json").write_bytes(journal)
    results = tmp_path / "results/stage-3/runs"
    results.mkdir(parents=True)
    (results / f"{rid}.json").write_text('{"validation":{"ok":true}}')
    golden = tmp_path / "data/golden"
    golden.mkdir()
    (golden / "_data.json").write_text('{"rows":10,"seed":1}')
    events = EventLog()
    for i in range(3):
        events.add(1_000_000_000_000 + i, 5 + i, "p", 1.25 + i)
        events.committed_at[i] = 3.5 + i
    ledger = Ledger(100, stream_events=[("stream", events)], deleted_ids={1})
    ledger.merge_updated[3] = 2
    with (source / "ledger.pkl").open("wb") as fh:
        pickle.dump({"ledger": ledger, "table": "bench.events", "current": 101, "ops": []}, fh)
    closed = []

    class DB:
        def __init__(self, *_args: Any) -> None:
            pass

        def query(self, query: str, _args: Any) -> list[dict[str, Any]]:
            if query == artifacts.TABLES["runs"]:
                return [{"status": "finished", "git_sha": "source-sha",
                         "params": {"golden": {"data": "_data.json"}}}]
            return []

        def close(self) -> None:
            closed.append(True)

    out = tmp_path / "export"
    out.mkdir()
    previous = {"historic": {"git_sha": "old", "files": {}}}
    manifest_path = out / "manifest.json"
    manifest_path.write_text(json.dumps(previous))
    monkeypatch.setattr(artifacts, "REPO", tmp_path)
    monkeypatch.setattr(artifacts, "DB", DB)
    monkeypatch.setattr(artifacts, "Env", lambda: SimpleNamespace(
        pg_dsn="PRIVATE_ENV_NOT_FOR_EXPORT", pg_schema="test"))
    monkeypatch.setattr(artifacts, "EVENT_ROWS_PER_FILE", 2)
    monkeypatch.setattr("sys.argv", ["export", "--runs", rid, "--out", str(out)])
    artifacts.main()
    manifest = json.loads(manifest_path.read_text())
    assert manifest["historic"] == previous["historic"]
    entry = manifest[rid]
    assert entry["stream_events"] == 3
    target = out / rid
    assert gzip.decompress((target / "intents.json.gz").read_bytes()) == intents
    assert gzip.decompress((target / "journals/one.json.gz").read_bytes()) == journal
    state = json.loads(gzip.decompress((target / "ledger.json.gz").read_bytes()))
    assert state["ledger"]["deleted_ids"] == [1]
    assert state["ledger"]["merge_updated"] == {"3": 2}
    rows = []
    for path in sorted(target.glob("stream-*.csv.gz")):
        rows.extend(csv.DictReader(io.StringIO(gzip.decompress(path.read_bytes()).decode())))
    assert [int(r["event_id"]) for r in rows] == list(events.ids)
    assert [float(r["generated_at"]) for r in rows] == list(events.generated_at)
    assert [float(r["committed_at"]) for r in rows] == list(events.committed_at)
    assert len(list(target.glob("stream-*.csv.gz"))) == 2
    for name, info in entry["files"].items():
        raw = (target / name).read_bytes()
        assert hashlib.sha256(raw).hexdigest() == info["sha256"]
        assert b"PRIVATE_ENV_NOT_FOR_EXPORT" not in gzip.decompress(raw)
    before = manifest_path.read_bytes()
    with pytest.raises(FileExistsError):
        artifacts.main()
    assert manifest_path.read_bytes() == before
    assert closed == [True, True]
