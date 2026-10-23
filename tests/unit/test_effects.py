"""Unit tests for runner.ops.effects and runner.ops.errors (stage-1 trial validity)."""
from __future__ import annotations

import pytest

from runner.ops.effects import (
    commit_effect,
    in_hold_window,
    summary_effect,
    trial_outcome,
)
from runner.ops.errors import classify

# Summaries copied from the stand (Iceberg 1.12.0, Spark 4.0.1)
MOR_DELETE_PARTIAL = {"operation": "delete", "added-position-delete-files": "1",
                      "added-delete-files": "1", "added-position-deletes": "10"}
METADATA_DELETE = {"operation": "delete", "deleted-data-files": "1", "deleted-records": "50"}
EQ_UPSERT = {"operation": "overwrite", "added-data-files": "1", "added-records": "10",
             "added-equality-delete-files": "1", "added-equality-deletes": "10"}


def test_mor_delete_partial_is_valid() -> None:
    assert summary_effect("mor_delete", MOR_DELETE_PARTIAL) is None


def test_metadata_delete_is_not_mor_delete() -> None:
    reason = summary_effect("mor_delete", METADATA_DELETE)
    assert reason is not None
    assert "position deletes" in reason
    assert "metadata delete" in reason


def test_v3_dv_counts_as_position_delete() -> None:
    assert summary_effect("mor_delete", {"operation": "delete", "added-dvs": "1"}) is None


def test_append_is_not_stream_upsert() -> None:
    plain_append = {"operation": "append", "added-records": "10"}
    assert summary_effect("append", plain_append) is None
    assert summary_effect("stream_upsert", plain_append) is not None
    assert summary_effect("stream_upsert", EQ_UPSERT) is None


def test_rewrite_needs_replace() -> None:
    assert summary_effect("rewrite_data", {"operation": "replace", "deleted-data-files": "4",
                                           "added-data-files": "1"}) is None
    assert summary_effect("rewrite_data", {"operation": "replace"}) is not None


def test_unknown_kind_raises() -> None:
    with pytest.raises(ValueError):
        summary_effect("nope", {})


def test_expire_needs_removed_ids() -> None:
    assert commit_effect("expire", [{"action": "remove-snapshots", "snapshot-ids": [1, 2]}]) is None
    assert commit_effect("expire", [{"action": "remove-snapshots", "snapshot-ids": []}]) is not None
    assert commit_effect("expire", []) is not None


def test_commit_effect_reads_add_snapshot() -> None:
    upd = [{"action": "add-snapshot", "snapshot": {"summary": MOR_DELETE_PARTIAL}},
           {"action": "set-snapshot-ref"}]
    assert commit_effect("mor_delete", upd) is None
    assert commit_effect("mor_delete", [{"action": "set-snapshot-ref"}]) is not None


def test_trial_outcome() -> None:
    assert trial_outcome("success", [200]) == "success"
    assert trial_outcome("success", [409, 200]) == "K1"
    assert trial_outcome("K2", [409]) == "K2"


def test_hold_window() -> None:
    pending = {"held": True, "ts": 100, "ts_fwd": 200}
    ok = [{"status": 200, "ts": 120, "ts_resp": 150}]
    assert in_hold_window(pending, ok)
    assert not in_hold_window(pending, [{"status": 200, "ts": 120, "ts_resp": 250}])
    assert not in_hold_window(pending, [{"status": 409, "ts": 120, "ts_resp": 150}])
    assert not in_hold_window(pending, [])
    assert not in_hold_window({**pending, "hold_timeout": True}, ok)
    assert not in_hold_window({**pending, "held": False}, ok)


@pytest.mark.parametrize(("text", "expected"), [
    ("org.apache.iceberg.exceptions.ValidationException: Found conflicting files that can "
     "contain records matching ref(name=\"bkt\") == 0", "K2"),
    ("org.apache.iceberg.exceptions.CommitFailedException: Requirement failed: branch main "
     "has changed", "K1"),
    ("java.lang.IllegalArgumentException: Snapshot 1 is not an ancestor of 2", "unknown"),
    # Spark rewrite action wrapper: the message names both, the cause decides
    ("java.lang.RuntimeException: Cannot commit rewrite because of a ValidationException or "
     "CommitFailedException.\n\tat org.apache.iceberg.spark.actions.X\nCaused by: "
     "org.apache.iceberg.exceptions.CommitFailedException: Requirement failed", "K1"),
    ("java.lang.RuntimeException: Cannot commit rewrite because of a ValidationException or "
     "CommitFailedException.\nCaused by: org.apache.iceberg.exceptions.ValidationException: "
     "Cannot commit, missing data files", "K2"),
    ("Cannot commit rewrite because of a ValidationException or CommitFailedException", "unknown"),
])
def test_classify(text: str, expected: str) -> None:
    assert classify(text) == expected
