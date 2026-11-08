from datetime import UTC, datetime, timedelta

import pytest

from runner.history import history_bounds

T = datetime(2026, 1, 1, tzinfo=UTC)


def row(lo, hi, sid=None, removed=None):
    return {"forwarded_at": T + timedelta(seconds=lo), "responded_at": T + timedelta(seconds=hi),
            "new_snapshot_id": sid, "removed_snapshot_ids": removed}


def test_peak_missed_by_thirty_second_samples_exceeds_real_limit():
    rows = [row(1, 2, i) for i in range(1, 1453)] + [row(3, 4, removed=list(range(1, 1452)))]
    b = history_bounds(0, rows)
    assert b["lower"]["peak_snapshots"] == b["upper"]["peak_snapshots"] == 1453
    assert b["upper"]["final_snapshots"] == 2


def test_duplicate_success_and_expire_do_not_double_count():
    r = row(1, 2, 1)
    b = history_bounds(0, [r, r, row(3, 4, removed=[1]), row(5, 6, removed=[1])])
    assert b["upper"]["peak_snapshots"] == 2
    assert b["upper"]["final_snapshots"] == 1


def test_uncertain_overlapping_mutations_are_not_assumed_ordered():
    b = history_bounds(0, [row(1, 2, 1), row(3, 5, 2), row(3, 5, removed=[1])])
    assert b["lower"]["peak_snapshots"] == 2
    assert b["upper"]["peak_snapshots"] == 3


@pytest.mark.parametrize("bad", [row(3, 2, 1), {**row(1, 2, 1), "forwarded_at": None},
                                 row(1, 2, removed=[999]), row(1, 2, removed=[0])])
def test_incomplete_or_invalid_journal_cannot_prove_a_bound(bad):
    with pytest.raises(ValueError):
        history_bounds(0, [bad])
