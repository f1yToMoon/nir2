"""Regressions for DELETE effects and reinsertion in the independent row ledger."""
from collections import Counter
from types import SimpleNamespace
from typing import Any

from runner.invariants import Invariants, Ledger


def test_ledger_resets_reinserted_version() -> None:
    led = Ledger(1)
    led.apply_effects({"effect_updated_ids": [7]})
    led.apply_effects({"deleted_rows": [(7, 1, "p")]})
    assert led.deleted_ids == {7}
    assert not led.merge_updated
    led.apply_effects({"effect_inserted_rows": [(7, 1, "p")]})
    led.apply_effects({"effect_updated_ids": [7]})
    assert led.deleted_ids == set()
    assert led.merge_updated == Counter({7: 1})
    assert led.merge_inserted == []


def test_retained_deleted_golden_row_fails_count_invariant(monkeypatch: Any) -> None:
    class FakeDB:
        def query(self, *_args: Any) -> list[dict[str, int]]:
            return [{"added": 0, "deleted": 0, "n": 0}]

    inv = Invariants(None, FakeDB(), "r", "bench.events", 1, 1, object())
    monkeypatch.setattr(inv, "_lineage", lambda _led: ([], None))
    monkeypatch.setattr("runner.ops.executor.load_table_state", lambda *_args: SimpleNamespace(
        current_snapshot_id=1, metadata={"snapshots": [{"snapshot-id": 1,
                                                       "summary": {"total-records": "2"}}]}))
    queries = []

    def sql(q: str) -> list[dict[str, Any]]:
        queries.append(q)
        if "GROUP BY 1, 2" in q:
            assert "ANTI JOIN" not in q
            return [{"p": "p", "o": "golden", "n": 2}]
        if "GROUP BY 1" in q:
            assert "t.event_id = d.dkey" in q
            return [{"p": "p", "n": 1}]
        return [{"n": 2}]

    monkeypatch.setattr(inv, "_sql", sql)
    result = inv.ledger_row_counts(Ledger(1, deleted_ids={7}), [])
    assert not result["ok"]
    assert result["n_mismatches"] == 1
    assert result["journal_physical_records"] == result["snapshot_total_records"]


def test_merge_insert_delete_reinsert_is_counted_once() -> None:
    event_id = 9_000_000_000_123
    led = Ledger(1)
    led.apply_effects({"effect_inserted_rows": [(event_id, 42, "p")]})
    led.apply_effects({"effect_updated_ids": [event_id]})
    led.apply_effects({"deleted_rows": [(event_id, 42, "p")]})
    assert led.deleted_ids == {event_id}
    assert not led.merge_updated
    # Another event of this key is never excluded by a key-wide tombstone.
    led.apply_effects({"effect_inserted_rows": [(event_id + 1, 42, "p")]})
    assert event_id + 1 not in led.deleted_ids
    led.apply_effects({"effect_inserted_rows": [(event_id, 42, "p")]})
    assert led.deleted_ids == set()
    assert sorted(led.merge_inserted) == [(event_id, 42, "p"), (event_id + 1, 42, "p")]
