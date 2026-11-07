"""Freshness uses exact source event clocks and only files added by the source snapshot."""
from __future__ import annotations

from datetime import UTC
from unittest.mock import patch

import pytest

from runner.env import Env
from runner.freshness import StreamFreshness, added_data_files
from runner.streaming import EventLog


def test_added_files_exclude_carried_entries_and_delete_files() -> None:
    records = {
        "list": [{"added_snapshot_id": 7, "manifest_path": "new"},
                 {"added_snapshot_id": 6, "manifest_path": "carried"}],
        "new": [{"status": status, "snapshot_id": sid,
                 "data_file": {"content": content, "file_path": path}}
                for status, sid, content, path in [(1, None, 0, "added"),
                                                  (0, 7, 0, "existing"),
                                                  (2, 7, 0, "deleted"),
                                                  (1, 6, 0, "old"),
                                                  (1, 7, 1, "delete-file")]],
    }
    with patch("runner.freshness._records", side_effect=lambda opener, path: records[path]):
        assert list(added_data_files(None, "list", 7)) == ["added"]  # type: ignore[arg-type]


def test_exact_source_clock_percentile_and_no_compaction_recount() -> None:
    log = EventLog()
    log.add(1, 1, "p", 100)
    log.add(2, 2, "p", 101)
    observer = StreamFreshness(Env(), "run", "bench.events")
    observer.register(log)
    assert observer.record([1, 2], 110)
    assert observer.record([1, 2], 150)  # later appearance never shifts first visibility
    result = observer.result()
    assert result["complete"]
    assert result["n_samples"] == 2
    assert result["p95_s"] == pytest.approx(9.95)


def test_missing_event_suppresses_percentile() -> None:
    log = EventLog()
    log.add(1, 1, "p", 100)
    log.add(2, 2, "p", 101)
    observer = StreamFreshness(Env(), "run", "bench.events")
    observer.register(log)
    observer.record([1], 110)
    result = observer.result()
    assert not result["complete"]
    assert result["missing"] == 1
    assert result["p95_s"] is None
    assert not observer.record([99], 110)  # append commit can race agent log registration
    with pytest.raises(ValueError, match="negative freshness"):
        observer.record([2], 99)


def test_event_log_pickle_preserves_clocks_and_restores_index() -> None:
    import pickle
    log = EventLog()
    log.add(123, 9, "p", 42)
    restored = pickle.loads(pickle.dumps(log))
    assert restored.generated_at[0] == 42
    assert restored.positions == {123: 0}
    restored.add(456, 10, "p", 43)
    assert list(restored.ids) == [123, 456]


def test_observer_snapshot_retry_after_agent_registration() -> None:
    from datetime import datetime
    from types import SimpleNamespace
    from unittest.mock import Mock

    observer = StreamFreshness(Env(), "run", "bench.events")
    log = EventLog()
    observer.register(log)
    db = Mock()
    db.query.return_value = [{"new_snapshot_id": 7,
                              "responded_at": datetime.fromtimestamp(110, UTC)}]
    state = SimpleNamespace(metadata={"snapshots": [
        {"snapshot-id": 7, "manifest-list": "list", "summary": {"operation": "append"}}]})
    with (patch("runner.freshness.load_table_state", return_value=state),
          patch("runner.freshness.added_data_files", return_value=["file"]),
          patch("runner.freshness.event_ids", return_value=[1])):
        observer.sample(db, None)  # type: ignore[arg-type]
        assert not observer.result()["complete"]
        log.add(1, 1, "p", 100)
        observer.sample(db, None)  # type: ignore[arg-type]
        observer.sample(db, None)  # type: ignore[arg-type]
    assert observer.result()["complete"]
    assert observer.result()["n_snapshots"] == 1
    assert observer.result()["p95_s"] == 10
    query = db.query.call_args.args[0]
    assert "writer_class = 'stream'" in query
    assert "iceberg_op = 'append'" in query
