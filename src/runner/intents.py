"""Deterministic plan of controlled operations (intents) of a run (FR-RUN-3, NFR-5).

Everything random is derived from the run seed: arrival times, footprints of selectors that do
not depend on the table state (recency_zipf, keys_all_days, single_day, table), per-operation
seeds for the batch contents. Two runs with the same (scenario, variant, level, seed) produce the
same list of intents; `fingerprint()` is what the determinism check compares.

Selectors small_files_topn and delete_files_topn choose partitions by the table state at the
moment of execution (AutoComp-like); the intent fixes the selector and its parameters, the
resolved partitions are recorded in `ops.footprint_requested` and compared between runs
separately (they depend on the timing of the streams).
"""
from __future__ import annotations

import hashlib
import math
from dataclasses import asdict, dataclass, field
from datetime import date, timedelta
from typing import Any

import numpy as np

from runner.scenario import ControlledOp, Scenario

STATE_SELECTORS = {"small_files_topn", "delete_files_topn"}


# ── Iceberg bucket transform for long (spec: Murmur3 32-bit x86, seed 0, 8-byte LE) ──────────

def _rotl32(x: int, r: int) -> int:
    return ((x << r) | (x >> (32 - r))) & 0xFFFFFFFF


def murmur3_long(value: int) -> int:
    """Iceberg `bucket` hash of a long (signed 32-bit result); spec test vector 34 → 2017239379."""
    data = (value & 0xFFFFFFFFFFFFFFFF).to_bytes(8, "little")
    c1, c2 = 0xCC9E2D51, 0x1B873593
    h = 0
    for i in (0, 4):
        k = int.from_bytes(data[i:i + 4], "little")
        k = (k * c1) & 0xFFFFFFFF
        k = _rotl32(k, 15)
        k = (k * c2) & 0xFFFFFFFF
        h ^= k
        h = _rotl32(h, 13)
        h = (h * 5 + 0xE6546B64) & 0xFFFFFFFF
    h ^= 8
    h ^= h >> 16
    h = (h * 0x85EBCA6B) & 0xFFFFFFFF
    h ^= h >> 13
    h = (h * 0xC2B2AE35) & 0xFFFFFFFF
    h ^= h >> 16
    return h - (1 << 32) if h & 0x80000000 else h


def bucket_long(value: int, n: int) -> int:
    return (murmur3_long(value) & 0x7FFFFFFF) % n


def partition_str(day: date, bucket: int) -> str:
    """Partition path as the proxy journal writes it (capl.enricher.partition_to_str)."""
    return f"dt={day.isoformat()}/key_bucket={bucket}"


def parse_partition(p: str) -> tuple[date, int]:
    d, b = p.split("/")
    return date.fromisoformat(d.split("=", 1)[1]), int(b.split("=", 1)[1])


# ── seeds ────────────────────────────────────────────────────────────────────

def derive_seed(*parts: Any) -> int:
    """Stable 63-bit seed from the run seed and names (independent of PYTHONHASHSEED)."""
    h = hashlib.sha256("|".join(str(p) for p in parts).encode()).digest()
    return int.from_bytes(h[:8], "little") & 0x7FFFFFFFFFFFFFFF


def rng(*parts: Any) -> np.random.Generator:
    return np.random.Generator(np.random.PCG64(derive_seed(*parts)))


# ── intents ──────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class Intent:
    op_id: str
    op_key: str              # controlled_ops[].id
    op_type: str
    seq: int                 # number of the operation of this op_key
    t_offset_s: float        # intent time from the start of the run (warm-up included)
    sla_s: float
    op_seed: int
    footprint: dict[str, Any] = field(default_factory=dict)   # selector, params, partitions/keys

    @property
    def state_dependent(self) -> bool:
        return self.footprint.get("selector") in STATE_SELECTORS

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


def drift_factor(sc: Scenario, t: float, target: str) -> float:
    d = sc.drift
    if d is None or target not in d.applies_to:
        return 1.0
    return 1.0 + d.amplitude * math.sin(2 * math.pi * t / d.period_s)


def _arrivals(sc: Scenario, op: ControlledOp, seed: int, t0: float, t1: float) -> list[float]:
    r = rng(seed, "arrival", op.id)
    base = op.arrival.interval_s
    peak = 1.0 + (sc.drift.amplitude if sc.drift and "controlled_ops" in sc.drift.applies_to else 0)
    out: list[float] = []
    if op.arrival.process == "fixed":
        t = t0 + float(r.uniform(0, base))           # seeded phase
        while t < t1:
            out.append(t)
            t += base / drift_factor(sc, t, "controlled_ops")
    else:
        t = t0
        while True:
            t += float(r.exponential(base / peak))
            if t >= t1:
                break
            # thinning for the drifting rate
            if r.uniform() * peak <= drift_factor(sc, t, "controlled_ops"):
                out.append(t)
    return out


def _zipf_offset(r: np.random.Generator, a: float, n: int) -> int:
    w = np.array([(k + 1) ** -a for k in range(n)])
    return int(r.choice(n, p=w / w.sum()))


def resolve_footprint(sc: Scenario, op: ControlledOp, r: np.random.Generator) -> dict[str, Any]:
    """Footprint of a seeded selector; for state selectors only the parameters."""
    fp = op.footprint
    days = sc.table.days                          # oldest first
    today = sc.table.logical_today
    nb = sc.table.n_buckets
    out: dict[str, Any] = {"selector": fp.selector}
    if fp.selector in STATE_SELECTORS:
        out.update({k: v for k, v in fp.model_dump().items() if v is not None and k != "selector"})
        return out
    if fp.selector == "recency_zipf":
        assert fp.partitions_per_op is not None and fp.days is not None and fp.zipf_a is not None
        n = int(r.integers(fp.partitions_per_op.min, fp.partitions_per_op.max + 1))
        n = min(n, fp.days * nb)
        parts: list[str] = []
        while len(parts) < n:
            off = _zipf_offset(r, fp.zipf_a, fp.days)
            p = partition_str(today - timedelta(days=off), int(r.integers(0, nb)))
            if p not in parts:
                parts.append(p)
        out.update(partitions=sorted(parts), rows=fp.rows)
    elif fp.selector == "keys_all_days":
        assert fp.buckets is not None and fp.n_keys is not None
        buckets = sorted(int(b) for b in r.choice(nb, size=fp.buckets, replace=False))
        card = sc.seed_data.key_cardinality
        keys: list[int] = []
        seen: set[int] = set()
        while len(keys) < fp.n_keys:
            k = int(r.integers(0, card))
            if k not in seen and bucket_long(k, nb) in buckets:
                seen.add(k)
                keys.append(k)
        out.update(buckets=buckets, keys=sorted(keys),
                   partitions=sorted(partition_str(d, b) for d in days for b in buckets))
    elif fp.selector == "single_day":
        lo = fp.min_age_days or 0
        hi = fp.max_age_days if fp.max_age_days is not None else sc.table.logical_days - 1
        age = int(r.integers(lo, hi + 1))
        day = today - timedelta(days=age)
        out.update(day=day.isoformat(), rows=fp.rows,
                   partitions=sorted(partition_str(day, b) for b in range(nb)))
    elif fp.selector == "table":
        out.update(partitions=[])                  # expire: no data partitions
    return out


def generate(sc: Scenario, seed: int, run_id: str) -> list[Intent]:
    """Intents in [warmup_s, warmup_s + duration_s), sorted by time (ties by op_key)."""
    t0, t1 = sc.warmup_s, sc.warmup_s + sc.duration_s
    out: list[Intent] = []
    for op in sc.active_ops:
        for seq, t in enumerate(_arrivals(sc, op, seed, t0, t1)):
            r = rng(seed, "footprint", op.id, seq)
            out.append(Intent(
                op_id=f"{run_id}-{op.id}-{seq:04d}", op_key=op.id, op_type=op.type, seq=seq,
                t_offset_s=round(t, 3), sla_s=op.sla_s,
                op_seed=derive_seed(seed, "op", op.id, seq),
                footprint=resolve_footprint(sc, op, r)))
    out.sort(key=lambda i: (i.t_offset_s, i.op_key, i.seq))
    return out


def fingerprint(intents: list[Intent]) -> list[tuple[Any, ...]]:
    """What the determinism check compares: op key, sequence, type, time, seeded footprint.
    The run id is not part of it (it differs between the two runs)."""
    return [(i.op_key, i.seq, i.op_type, i.t_offset_s, i.op_seed,
             tuple(sorted((k, str(v)) for k, v in i.footprint.items()))) for i in intents]
