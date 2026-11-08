"""Bounds on every catalogue history state, including peaks between health samples."""
from __future__ import annotations

from datetime import datetime
from typing import Any


def history_bounds(initial: int, requests: list[dict[str, Any]]) -> dict[str, Any]:
    adds: dict[int, list[tuple[datetime, datetime]]] = {}
    removes: dict[int, list[tuple[datetime, datetime]]] = {}
    for row in requests:
        times = []
        for key in ("forwarded_at", "responded_at"):
            value = row[key]
            if isinstance(value, str):
                value = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if not isinstance(value, datetime) or value.tzinfo is None:
                raise ValueError(f"missing/invalid catalogue time: {key}")
            times.append(value)
        lo, hi = times
        if lo > hi:
            raise ValueError("catalogue response precedes forwarding")
        if row["new_snapshot_id"] is not None:
            adds.setdefault(int(row["new_snapshot_id"]), []).append((lo, hi))
        for sid in row["removed_snapshot_ids"] or []:
            removes.setdefault(int(sid), []).append((lo, hi))
    if initial in adds or initial in removes or not set(removes) <= set(adds):
        raise ValueError("incomplete journal or removal of the retained golden snapshot")
    out: dict[str, Any] = {"unique_additions": len(adds), "unique_removals": len(removes)}
    for mode in ("lower", "upper"):
        events = []
        for sid, windows in adds.items():
            events.append((min(w[1 if mode == "lower" else 0] for w in windows), "add", sid))
        for sid, windows in removes.items():
            events.append((min(w[0 if mode == "lower" else 1] for w in windows), "remove", sid))
        # Simultaneous uncertain transitions: removals first for the lower bound,
        # additions first for the upper bound. A tombstone avoids re-adding an expired ID.
        events.sort(key=lambda e: (e[0], (e[1] == "add") if mode == "lower" else (e[1] == "remove")))
        live, expired = {initial}, set()
        peak, at = 1, None
        for timestamp, kind, sid in events:
            if kind == "remove":
                expired.add(sid)
                live.discard(sid)
            elif sid not in expired:
                live.add(sid)
            if len(live) > peak:
                peak, at = len(live), timestamp
        out[mode] = {"peak_snapshots": peak, "at": at, "final_snapshots": len(live)}
    return out
