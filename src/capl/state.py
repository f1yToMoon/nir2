"""In-memory state of the CAPL service: commit ring buffers, intensities, periodic writers.

Mutated only from the event loop (tz/02 §4). Intensities follow tz/03 §3.1: a weighted mix of
per-window rates λ^(w) = (number of events in [t-w, t)) / w; periodic writers are detected by
the coefficient of variation of commit intervals.
"""
from __future__ import annotations

import itertools
import statistics
from collections import Counter, deque
from dataclasses import dataclass, field
from typing import Any

from capl.config import StateConfig


@dataclass(frozen=True)
class CommitRecord:
    t: float                           # committed_at, Unix seconds
    snapshot_id: int
    op_class: str
    writer_id: str | None
    partitions: tuple[str, ...]


@dataclass(frozen=True)
class PeriodicWriter:
    writer_id: str
    period_s: float
    phase_s: float                     # time of the last commit modulo period
    cv: float
    n_commits: int
    partitions: dict[str, float]       # empirical distribution over the last commits

    def expected_commits(self, t1: float, t2: float) -> int:
        """⌊(t2-φ)/T⌋ - ⌊(t1-φ)/T⌋ (tz/03 §3.1)."""
        return int((t2 - self.phase_s) // self.period_s - (t1 - self.phase_s) // self.period_s)


@dataclass
class _WriterHistory:
    times: deque[float]
    partitions: deque[tuple[str, ...]]


@dataclass
class TableState:
    commits: deque[CommitRecord]
    writers: dict[str, _WriterHistory] = field(default_factory=dict)


class StateStore:
    def __init__(self, cfg: StateConfig) -> None:
        if len(cfg.intensity_windows_s) != len(cfg.intensity_weights):
            raise ValueError("intensity_windows_s and intensity_weights differ in length")
        self.cfg = cfg
        self.tables: dict[str, TableState] = {}
        self.slot_ops: dict[str, str] = {}      # slot → op_id (FR-PROXY-4)

    # ---- slots ----
    def assign_slot(self, slot: str, op_id: str) -> None:
        self.slot_ops[slot] = op_id

    def release_slot(self, slot: str) -> None:
        self.slot_ops.pop(slot, None)

    def op_for_slot(self, slot: str | None) -> str | None:
        return self.slot_ops.get(slot) if slot else None

    def reset(self) -> None:
        self.tables.clear()
        self.slot_ops.clear()

    # ---- events ----
    def add_commit_event(self, event: dict[str, Any]) -> None:
        rec = CommitRecord(
            t=float(event["committed_at"]), snapshot_id=int(event["snapshot_id"]),
            op_class=str(event["op_class"]), writer_id=event.get("writer_id"),
            partitions=tuple(p["partition"] for p in event.get("partitions", [])),
        )
        ts = self.tables.get(event["table"])
        if ts is None:
            ts = TableState(deque(maxlen=self.cfg.ring_buffer_per_table))
            self.tables[event["table"]] = ts
        ts.commits.append(rec)
        if rec.writer_id:
            n = self.cfg.periodic_writer.partition_history
            wh = ts.writers.setdefault(rec.writer_id, _WriterHistory(deque(maxlen=n), deque(maxlen=n)))
            wh.times.append(rec.t)
            wh.partitions.append(rec.partitions)

    # ---- intensities ----
    def window_rate(self, table: str, window_s: float, now: float, op_class: str | None = None,
                    partition: str | None = None) -> float:
        """Commits per second in [now - window_s, now) (events at `now` are excluded)."""
        ts = self.tables.get(table)
        if ts is None:
            return 0.0
        n = 0
        for r in reversed(ts.commits):
            if r.t >= now:
                continue
            if r.t < now - window_s:
                break
            if op_class is not None and r.op_class != op_class:
                continue
            if partition is not None and partition not in r.partitions:
                continue
            n += 1
        return n / window_s

    def intensity(self, table: str, now: float, op_class: str | None = None,
                  partition: str | None = None) -> float:
        return sum(w * self.window_rate(table, win, now, op_class, partition)
                   for win, w in zip(self.cfg.intensity_windows_s, self.cfg.intensity_weights,
                                     strict=True))

    # ---- periodic writers ----
    def periodic_writers(self, table: str) -> dict[str, PeriodicWriter]:
        ts = self.tables.get(table)
        if ts is None:
            return {}
        pw = self.cfg.periodic_writer
        out = {}
        for wid, wh in ts.writers.items():
            times = sorted(wh.times)
            if len(times) < pw.min_commits_for_period:
                continue
            gaps = [b - a for a, b in itertools.pairwise(times)]
            mean = statistics.fmean(gaps)
            if mean <= 0:
                continue
            cv = statistics.pstdev(gaps) / mean
            if cv > pw.max_period_cv:
                continue
            period = statistics.median(gaps)
            cnt: Counter[str] = Counter(p for ps in wh.partitions for p in ps)
            total = sum(cnt.values()) or 1
            out[wid] = PeriodicWriter(wid, period, times[-1] % period, cv, len(times),
                                      {p: c / total for p, c in cnt.items()})
        return out

    def snapshot(self, now: float) -> dict[str, Any]:
        return {
            "slots": dict(self.slot_ops),
            "tables": {
                name: {
                    "commits_buffered": len(ts.commits),
                    "intensity_per_s": self.intensity(name, now),
                    "by_op_class": {c: self.intensity(name, now, op_class=c)
                                    for c in sorted({r.op_class for r in ts.commits})},
                    "periodic_writers": {w: {"period_s": p.period_s, "cv": round(p.cv, 3),
                                             "n": p.n_commits}
                                         for w, p in self.periodic_writers(name).items()},
                }
                for name, ts in self.tables.items()
            },
        }
