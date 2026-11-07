"""Outcome labels of the runner: base_expired only with data evidence (S2-R4-05, author decision
2026-11-04, REVIEWER.md P5); otherwise history_unresolved."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from runner.ops import executor as ex_mod
from runner.ops.executor import OpExecutor, TableState

UTC = timezone.utc  # noqa: UP017
T0 = datetime(2026, 11, 4, 12, 0, tzinfo=UTC)
HIST = ("org.apache.iceberg.exceptions.ValidationException: Cannot determine history between "
        "starting snapshot 111 and the last known ancestor 333")
BASE = "java.lang.IllegalArgumentException: Snapshot 111 is not an ancestor of 444"


class FakeDB:
    def __init__(self, children: dict[int, list[int]], expires: list[dict[str, Any]]) -> None:
        self.children, self.expires = children, expires

    def children_of(self, table: str, sid: int) -> list[int]:
        return self.children.get(sid, [])

    def validation_path(self, table: str, ancestor: int, base: int) -> list[int]:
        # Minimal journal chain: ancestor -> each supplied child -> base.
        return [ancestor, *self.children.get(base, [])] if self.children.get(base) else []

    def expire_commits(self, table: str, t0: Any, t1: Any) -> list[dict[str, Any]]:
        return [e for e in self.expires if t0 <= e["responded_at"] <= t1]


class Ctx:
    def __init__(self, db: FakeDB) -> None:
        self.db = db
        self.env = None
        self.table = "bench.events"


def _ex(db: FakeDB, snapshots: list[int], monkeypatch: pytest.MonkeyPatch) -> OpExecutor:
    monkeypatch.setattr(ex_mod, "load_table_state", lambda env, t: TableState(
        snapshots[-1], {s: {"parent": None, "ts_ms": 0} for s in snapshots}, "s3://m.json"))
    monkeypatch.setattr(ex_mod.time, "sleep", lambda s: None)
    e = OpExecutor.__new__(OpExecutor)
    e.ctx = Ctx(db)  # type: ignore[assignment]
    return e


def test_history_confirmed(monkeypatch: pytest.MonkeyPatch) -> None:
    db = FakeDB({111: [222]}, [{"op_id": "bg-1", "responded_at": T0 + timedelta(seconds=5),
                                "removed_snapshot_ids": [100, 222]}])
    e = _ex(db, [111, 333, 444], monkeypatch)      # 222 (after the base) is gone
    out, d = e._outcome(HIST, T0, T0 + timedelta(seconds=10))
    assert out == "base_expired" and d["expire_evidence"]["confirmed"]


def test_history_without_expire_commit_is_unresolved(monkeypatch: pytest.MonkeyPatch) -> None:
    # wrong validate-from-snapshot-id: same message, no expire removed the snapshot after it
    db = FakeDB({111: [222]}, [])
    e = _ex(db, [111, 333, 444], monkeypatch)
    out, _ = e._outcome(HIST, T0, T0 + timedelta(seconds=10))
    assert out == "history_unresolved"


def test_history_snapshot_still_present_is_unresolved(monkeypatch: pytest.MonkeyPatch) -> None:
    db = FakeDB({111: [222]}, [{"op_id": "bg-1", "responded_at": T0 + timedelta(seconds=5),
                                "removed_snapshot_ids": [222]}])
    e = _ex(db, [111, 222, 444], monkeypatch)      # 222 present in metadata
    out, _ = e._outcome(HIST, T0, T0 + timedelta(seconds=10))
    assert out == "history_unresolved"


def test_expire_outside_operation_window_is_unresolved(monkeypatch: pytest.MonkeyPatch) -> None:
    db = FakeDB({111: [222]}, [{"op_id": "bg-1", "responded_at": T0 - timedelta(seconds=5),
                                "removed_snapshot_ids": [222]}])
    e = _ex(db, [111, 444], monkeypatch)
    out, _ = e._outcome(HIST, T0, T0 + timedelta(seconds=10))
    assert out == "history_unresolved"


def test_rowdelta_base_confirmed(monkeypatch: pytest.MonkeyPatch) -> None:
    db = FakeDB({}, [{"op_id": "bg-2", "responded_at": T0 + timedelta(seconds=1),
                      "removed_snapshot_ids": [111]}])
    e = _ex(db, [444], monkeypatch)
    out, _ = e._outcome(BASE, T0, T0 + timedelta(seconds=10))
    assert out == "base_expired"


def test_other_labels(monkeypatch: pytest.MonkeyPatch) -> None:
    e = _ex(FakeDB({}, []), [1], monkeypatch)
    assert e._outcome("org.apache.iceberg.exceptions.ValidationException: Found conflicting files",
                      T0, T0)[0] == "data_conflict"
    assert e._outcome("org.apache.iceberg.exceptions.CommitFailedException: x", T0, T0)[0] \
        == "retry_exhausted"
    assert e._outcome("org.apache.iceberg.exceptions.CommitStateUnknownException: x", T0, T0)[0] \
        == "commit_unknown"
    assert e._outcome("java.lang.OutOfMemoryError", T0, T0)[0] == "error"


def test_expire_after_failure_is_unresolved(monkeypatch: pytest.MonkeyPatch) -> None:
    db = FakeDB({111: [222]}, [{"op_id": "too-late", "responded_at": T0 + timedelta(seconds=10.5),
                                "removed_snapshot_ids": [222]}])
    e = _ex(db, [111, 444], monkeypatch)
    assert e._outcome(HIST, T0, T0 + timedelta(seconds=10))[0] == "history_unresolved"


def test_unrelated_branch_child_is_unresolved(monkeypatch: pytest.MonkeyPatch) -> None:
    db = FakeDB({}, [{"op_id": "other-branch", "responded_at": T0 + timedelta(seconds=5),
                      "removed_snapshot_ids": [222]}])
    e = _ex(db, [111, 333, 444], monkeypatch)
    assert e._outcome(HIST, T0, T0 + timedelta(seconds=10))[0] == "history_unresolved"
