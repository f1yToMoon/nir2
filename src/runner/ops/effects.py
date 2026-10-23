"""Checks that an Iceberg commit really did what the operation was meant to do.

Input is the snapshot summary from the commit body (`add-snapshot` update as seen by the
proxy) or the list of `remove-snapshots` ids, so the check works for commits that were
never applied (pending commit that failed validation) and does not depend on Spark's
metadata cache.

Operation kinds and the evidence each one must leave (SnapshotSummary keys, Iceberg 1.12.0):

  append          operation=append, added-records > 0
  stream_upsert   operation=overwrite, added-records > 0, added-equality-deletes > 0
  overwrite       operation=overwrite, added-records > 0, deleted-data-files > 0
  cow_dml         operation=overwrite, deleted-data-files > 0 (file rewritten)
  mor_delete      operation=delete, position deletes or DVs added, no data files removed
                  (a DELETE that removed whole files is a metadata delete, not MoR —
                  BaseRowDelta.operation(): only deletes added → "delete")
  mor_update      operation=overwrite, added-records > 0, position deletes or DVs added
  rewrite_data    operation=replace, deleted-data-files > 0, added-data-files > 0
  rewrite_deletes operation=replace, position-delete files removed
  expire          ≥ 1 snapshot id removed
"""
from __future__ import annotations

from collections.abc import Mapping
from typing import Any

# op name used by tests/scenarios → effect kind
OP_KIND: dict[str, str] = {
    "append":               "append",
    "stream_upsert":        "stream_upsert",
    "overwrite":            "overwrite",
    "overwrite_opts":       "overwrite",
    "insert_overwrite":     "overwrite",
    "cow_dml":              "cow_dml",
    "merge_upsert":         "cow_dml",
    "merge_upsert_unscoped": "cow_dml",
    "delete_by_keys":       "cow_dml",
    "mor_dml":              "mor_delete",
    "mor_delete":           "mor_delete",
    "mor_update":           "mor_update",
    "mor_merge":            "mor_update",
    "update_status":        "mor_update",
    "rewrite_data":         "rewrite_data",
    "compaction_where":     "rewrite_data",
    "rewrite_deletes":      "rewrite_deletes",
    "expire":               "expire",
}


def _n(summary: Mapping[str, Any], key: str) -> int:
    try:
        return int(summary.get(key, 0) or 0)
    except (TypeError, ValueError):
        return 0


def summary_effect(kind: str, summary: Mapping[str, Any]) -> str | None:
    """Return None if the snapshot summary shows the intended effect, else the reason."""
    op = summary.get("operation", "")
    added = _n(summary, "added-records")
    del_files = _n(summary, "deleted-data-files")
    add_files = _n(summary, "added-data-files")
    pos = _n(summary, "added-position-deletes") + _n(summary, "added-dvs")
    eq = _n(summary, "added-equality-deletes")
    rm_pos_files = _n(summary, "removed-position-delete-files") + _n(summary, "removed-dvs")

    checks: dict[str, list[tuple[bool, str]]] = {
        "append": [(op == "append", f"operation={op}"), (added > 0, "added-records=0")],
        "stream_upsert": [(op == "overwrite", f"operation={op}"),
                          (added > 0, "added-records=0"),
                          (eq > 0, "added-equality-deletes=0")],
        "overwrite": [(op == "overwrite", f"operation={op}"),
                      (added > 0, "added-records=0"),
                      (del_files > 0, "deleted-data-files=0")],
        "cow_dml": [(op == "overwrite", f"operation={op}"),
                    (del_files > 0, "deleted-data-files=0")],
        "mor_delete": [(op == "delete", f"operation={op}"),
                       (pos > 0, "no position deletes / DVs added"),
                       (del_files == 0, f"metadata delete: deleted-data-files={del_files}")],
        "mor_update": [(op == "overwrite", f"operation={op}"),
                       (added > 0, "added-records=0"),
                       (pos > 0, "no position deletes / DVs added")],
        "rewrite_data": [(op == "replace", f"operation={op}"),
                         (del_files > 0, "deleted-data-files=0"),
                         (add_files > 0, "added-data-files=0")],
        "rewrite_deletes": [(op == "replace", f"operation={op}"),
                            (rm_pos_files > 0, "no position-delete files removed")],
    }
    if kind not in checks:
        raise ValueError(f"no summary check for kind {kind!r}")
    failed = [reason for ok, reason in checks[kind] if not ok]
    return "; ".join(failed) if failed else None


def commit_effect(kind: str, updates: list[dict[str, Any]]) -> str | None:
    """Check one commit body (proxy digest `updates`) against the operation kind."""
    if kind == "expire":
        removed = [i for u in updates if u.get("action") == "remove-snapshots"
                   for i in u.get("snapshot-ids", [])]
        return None if removed else "no remove-snapshots update"
    snaps = [u["snapshot"] for u in updates
             if u.get("action") == "add-snapshot" and u.get("snapshot")]
    if len(snaps) != 1:
        return f"expected 1 add-snapshot, got {len(snaps)}"
    return summary_effect(kind, snaps[0].get("summary", {}))


def added_snapshot(updates: list[dict[str, Any]]) -> dict[str, Any] | None:
    for u in updates:
        if u.get("action") == "add-snapshot" and u.get("snapshot"):
            snap: dict[str, Any] = u["snapshot"]
            return snap
    return None


def removed_snapshot_ids(updates: list[dict[str, Any]]) -> list[int]:
    return [int(i) for u in updates if u.get("action") == "remove-snapshots"
            for i in u.get("snapshot-ids", [])]


def trial_outcome(op_result: str, attempt_statuses: list[int | None]) -> str:
    """Final outcome of the pending operation.

    op_result is "success" or the classify() result of its exception. A successful
    operation whose commit got HTTP 409 at least once was retried by the Iceberg
    client → K1; without 409 → success.
    """
    if op_result == "success":
        return "K1" if 409 in attempt_statuses else "success"
    return op_result


def in_hold_window(pending: Mapping[str, Any], committed: list[Mapping[str, Any]]) -> bool:
    """All committed commits were answered by the catalog while pending was held.

    pending: proxy entry of the pending operation's first commit attempt
    (ts = arrival at proxy, ts_fwd = released and forwarded).
    """
    if not pending.get("held") or pending.get("hold_timeout"):
        return False
    t0, t1 = pending.get("ts"), pending.get("ts_fwd")
    if t0 is None or t1 is None or not committed:
        return False
    return all(
        c.get("status") == 200 and c.get("ts_resp") is not None
        and t0 <= c["ts"] and c["ts_resp"] <= t1
        for c in committed
    )
