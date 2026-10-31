"""Hold gates: delay forwarding of a commit until released, or until a hard deadline.

The proxy never changes the commit; a hold only postpones forwarding and is bounded by
hold.max_ms (AGENTS.md, constraint 1). Release reasons: explicit release, commit of a named
competitor slot, deadline, stop of the run (release_all).
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field


@dataclass
class _Gate:
    event: asyncio.Event
    max_s: float
    reason: str
    release_on_commit_of: str | None
    released_by: str | None = None
    created_at: float = field(default_factory=time.monotonic)


@dataclass(frozen=True)
class HoldResult:
    held: bool
    hold_ms: int
    timed_out: bool
    max_ms: int
    reason: str | None
    released_by: str | None


NO_HOLD = HoldResult(held=False, hold_ms=0, timed_out=False, max_ms=0, reason=None, released_by=None)


class HoldGates:
    """Registry slot → gate. All methods run in the event loop (no locks needed)."""

    def __init__(self, max_ms_limit: int) -> None:
        if max_ms_limit <= 0:
            raise ValueError("hold.max_ms must be positive")
        self.max_ms_limit = max_ms_limit
        self._gates: dict[str, _Gate] = {}

    def register(self, slot: str, max_ms: int, reason: str = "debug",
                 release_on_commit_of: str | None = None) -> int:
        """Arm a gate for the next commit of `slot`; returns the effective max hold in ms."""
        if max_ms <= 0:
            raise ValueError("max_ms must be positive")
        old = self._gates.get(slot)
        if old is not None:
            old.released_by = "replaced"
            old.event.set()
        eff = min(max_ms, self.max_ms_limit)
        self._gates[slot] = _Gate(asyncio.Event(), eff / 1000.0, reason, release_on_commit_of)
        return eff

    def release(self, slot: str, by: str = "release") -> bool:
        gate = self._gates.pop(slot, None)
        if gate is None:
            return False
        gate.released_by = by
        gate.event.set()
        return True

    def release_all(self, by: str = "run_stop") -> int:
        slots = list(self._gates)
        for s in slots:
            self.release(s, by)
        return len(slots)

    def on_commit_succeeded(self, slot: str) -> list[str]:
        """Release gates waiting for a successful commit of `slot`; returns released slots."""
        waiting = [s for s, g in self._gates.items() if g.release_on_commit_of == slot and slot]
        for s in waiting:
            self.release(s, f"commit:{slot}")
        return waiting

    def is_armed(self, slot: str) -> bool:
        return slot in self._gates

    def armed(self) -> dict[str, dict[str, object]]:
        return {s: {"max_ms": round(g.max_s * 1000), "reason": g.reason,
                    "release_on_commit_of": g.release_on_commit_of}
                for s, g in self._gates.items()}

    async def wait(self, slot: str, max_ms: int | None = None) -> HoldResult:
        """Block the current commit of `slot` while its gate is armed (bounded by the deadline)."""
        gate = self._gates.get(slot)
        if gate is None:
            return NO_HOLD
        max_s = gate.max_s if max_ms is None else min(gate.max_s, max_ms / 1000.0)
        t0 = time.monotonic()
        timed_out = False
        try:
            await asyncio.wait_for(gate.event.wait(), timeout=max_s)
        except TimeoutError:
            timed_out = True
            gate.released_by = "deadline"
            if self._gates.get(slot) is gate:
                del self._gates[slot]
        return HoldResult(held=True, hold_ms=round((time.monotonic() - t0) * 1000),
                          timed_out=timed_out, max_ms=round(max_s * 1000), reason=gate.reason,
                          released_by=gate.released_by)
