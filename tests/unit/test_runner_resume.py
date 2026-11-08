"""Finished status must not turn an invalid preserved result into success."""
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

INVALID = [None, b"{", b'{"validation":{"ok":false}}', b'{"validation":{"ok":1}}']


class FinishedDB:
    def __init__(self, run_id: str) -> None:
        self.run_id = run_id
        self.closed = False
        self.deleted = False
        self.status = "finished"

    def migrate(self) -> None:
        pass

    def query(self, *_args: Any) -> list[dict[str, str]]:
        return [{"status": self.status, "run_id": self.run_id}]

    def delete_run(self, _run_id: str) -> None:
        self.deleted = True
        pytest.fail("resume must not delete existing measurements")

    def close(self) -> None:
        self.closed = True


def prepare(tmp_path: Path, run_id: str, payload: bytes | None) -> Path:
    path = tmp_path / f"{run_id}.json"
    if payload is not None:
        path.write_bytes(payload)
    return path


@pytest.mark.parametrize("payload", INVALID)
def test_main_refuses_finished_invalid_json(monkeypatch: Any, tmp_path: Path,
                                           payload: bytes | None) -> None:
    from runner import main
    run_id = "finished-invalid"
    path = prepare(tmp_path, run_id, payload)
    db = FinishedDB(run_id)
    monkeypatch.setattr(main, "DB", lambda *_args: db)
    monkeypatch.setattr(main, "RESULTS_DIR", tmp_path)
    monkeypatch.setattr(main, "build_session", lambda *_a, **_k: pytest.fail("Spark must not start"))
    run = main.Run.__new__(main.Run)
    run.env = SimpleNamespace(pg_dsn="unused", pg_schema="test")
    run.sc, run.args, run.run_id = SimpleNamespace(), SimpleNamespace(force=False), run_id
    run._golden = lambda: ({}, {})
    with pytest.raises((ValueError, SystemExit), match="Cannot resume"):
        run.execute()
    assert not db.deleted and db.closed
    assert path.read_bytes() == payload if payload is not None else not path.exists()


def test_main_skips_valid_result_and_closes_db(monkeypatch: Any, tmp_path: Path) -> None:
    from runner import main
    run_id = "finished-valid"
    path = prepare(tmp_path, run_id, b'{"validation":{"ok":true}}')
    db = FinishedDB(run_id)
    monkeypatch.setattr(main, "DB", lambda *_args: db)
    monkeypatch.setattr(main, "RESULTS_DIR", tmp_path)
    run = main.Run.__new__(main.Run)
    run.env = SimpleNamespace(pg_dsn="unused", pg_schema="test")
    run.sc, run.args, run.run_id = SimpleNamespace(), SimpleNamespace(force=False), run_id
    run._golden = lambda: ({}, {})
    assert run.execute() == 0
    assert not db.deleted and db.closed
    assert path.read_bytes() == b'{"validation":{"ok":true}}'


@pytest.mark.parametrize("status", ["failed", "running"])
def test_main_preserves_existing_unfinished_run(monkeypatch: Any, tmp_path: Path, status: str) -> None:
    from runner import main
    run_id = "existing"
    path = prepare(tmp_path, run_id, b"partial measurements")
    db = FinishedDB(run_id)
    db.status = status
    monkeypatch.setattr(main, "DB", lambda *_args: db)
    monkeypatch.setattr(main, "RESULTS_DIR", tmp_path)
    run = main.Run.__new__(main.Run)
    run.env = SimpleNamespace(pg_dsn="unused", pg_schema="test")
    run.sc, run.args, run.run_id = SimpleNamespace(), SimpleNamespace(force=False), run_id
    run._golden = lambda: ({}, {})
    with pytest.raises((ValueError, SystemExit), match="Cannot resume"):
        run.execute()
    assert not db.deleted and db.closed
    assert path.read_bytes() == b"partial measurements"


@pytest.mark.parametrize("payload", [*INVALID, b'{"validation":{"ok":true}}'])
def test_batch_finished_result_is_validated(monkeypatch: Any, tmp_path: Path,
                                           payload: bytes | None) -> None:
    from runner import batch
    from runner import db as db_module
    run_id = "verify-S0-P0-mid-s1"
    path = prepare(tmp_path, run_id, payload)
    db = FinishedDB(run_id)
    monkeypatch.setattr(db_module, "DB", lambda *_args: db)
    monkeypatch.setattr(batch, "RESULTS_DIR", tmp_path, raising=False)
    monkeypatch.setattr(batch.subprocess, "call", lambda *_a, **_k: pytest.fail("must not rerun"))
    matrix = tmp_path / "matrix.yaml"
    matrix.write_text(json.dumps({"series": {"verify": {
        "scenarios": ["S0_baseline"], "runs": [{"policy": "P0"}], "seeds": [1]}}}))
    args = ["--matrix", str(matrix), "--series", "verify"]
    if payload == b'{"validation":{"ok":true}}':
        assert batch.main(args) == 0
    else:
        with pytest.raises((ValueError, SystemExit), match="Cannot resume"):
            batch.main(args)
    assert not db.deleted and db.closed
    assert path.read_bytes() == payload if payload is not None else not path.exists()
