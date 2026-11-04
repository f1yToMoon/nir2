"""Scenario YAML loader (tz/06 step 3.1): pydantic models, `extends`, variants, concurrency levels.

    python -m runner.scenario --validate config/workloads config/experiments/matrix.yaml

Merge rules: dictionaries are merged recursively, lists are replaced, except `controlled_ops` in a
variant or an override: its items patch the base operations with the same `id` (an unknown id is
an error, so a typo in a variant cannot silently add an operation).

Runs on Python 3.10 (runner image) and 3.11 (dev).
"""
from __future__ import annotations

import argparse
import copy
import math
import sys
from datetime import date
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

OpType = Literal["rewrite_data", "rewrite_deletes", "cow_dml", "mor_delete", "mor_update",
                 "mor_merge", "overwrite", "expire"]
Selector = Literal["small_files_topn", "delete_files_topn", "recency_zipf", "keys_all_days",
                   "single_day", "table"]
Statement = Literal["merge_upsert", "merge_update", "update_status", "delete_by_keys"]
InvariantName = Literal["ledger_row_counts", "no_duplicate_event_ids", "committed_ops_in_lineage",
                        "aborted_ops_absent", "version_monotonic"]

WORKLOADS_DIR = Path(__file__).resolve().parents[2] / "config" / "workloads"


class _M(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Column(_M):
    name: str
    type: str
    required: bool = False


class Table(_M):
    name: str
    format_version: Literal[1, 2, 3] = 2
    schema_: list[Column] = Field(alias="schema")
    partition_spec: list[str]
    logical_days: int = Field(gt=0)
    logical_today: date
    properties: dict[str, str] = Field(default_factory=dict)

    @property
    def days(self) -> list[date]:
        """Logical days, oldest first."""
        from datetime import timedelta
        return [self.logical_today - timedelta(days=self.logical_days - 1 - i)
                for i in range(self.logical_days)]

    @property
    def n_buckets(self) -> int:
        for p in self.partition_spec:
            if p.startswith("bucket("):
                return int(p[len("bucket("):].split(",")[0])
        raise ValueError("partition_spec has no bucket transform")


class SeedData(_M):
    rows_total: int = Field(gt=0)
    files_per_partition: int = Field(gt=0)
    key_cardinality: int = Field(gt=0)
    payload_bytes: int = Field(ge=0)


class PartitionTarget(_M):
    hot_offset_days: int = 0
    late_fraction: float = Field(0.0, ge=0, le=1)
    late_max_days: int = 0


class KeyDist(_M):
    dist: Literal["uniform", "zipf"] = "uniform"
    a: float | None = None


class Stream(_M):
    id: str
    engine: Literal["flink", "spark"]
    mode: Literal["append", "upsert"] = "append"
    writers: int = Field(1, ge=1)
    rate_events_per_s: float = Field(gt=0)
    commit_period_s: float = Field(gt=0)
    partition_target: PartitionTarget = PartitionTarget()
    keys: KeyDist = KeyDist()


class ExpireMaintenance(_M):
    enabled: bool = True
    interval_s: float = Field(gt=0)
    older_than_s: float = Field(gt=0)
    retain_last: int = Field(ge=1)
    writer_class: str = "maintenance"


class Maintenance(_M):
    expire_snapshots: ExpireMaintenance | None = None


class Arrival(_M):
    process: Literal["poisson", "fixed"]
    interval_s: float = Field(gt=0)


class Range(_M):
    min: int = Field(ge=1)
    max: int = Field(ge=1)


class Footprint(_M):
    selector: Selector
    top_n: int | None = None
    min_age_days: int | None = None
    max_age_days: int | None = None
    min_files: int | None = None
    zipf_a: float | None = None
    days: int | None = None
    partitions_per_op: Range | None = None
    rows: int | None = None
    n_keys: int | None = None
    buckets: int | None = None

    @model_validator(mode="after")
    def _required(self) -> Footprint:
        need = {
            "small_files_topn": ("top_n", "min_age_days", "min_files"),
            "delete_files_topn": ("top_n",),
            "recency_zipf": ("zipf_a", "days", "partitions_per_op", "rows"),
            "keys_all_days": ("n_keys", "buckets"),
            "single_day": ("min_age_days",),
            "table": (),
        }[self.selector]
        missing = [k for k in need if getattr(self, k) is None]
        if missing:
            raise ValueError(f"selector {self.selector} needs {missing}")
        return self


class ControlledOp(_M):
    id: str
    type: OpType
    arrival: Arrival
    max_concurrent: int = Field(ge=1)
    sla_s: float = Field(gt=0)
    narrowable: bool = False
    format_versions: list[int] | None = None
    footprint: Footprint
    params: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _statement(self) -> ControlledOp:
        if self.type in ("cow_dml", "mor_delete", "mor_update", "mor_merge"):
            st = self.params.get("statement")
            allowed = {"cow_dml": {"merge_upsert", "merge_update", "delete_by_keys"},
                       "mor_delete": {"delete_by_keys"},
                       "mor_update": {"update_status"},
                       "mor_merge": {"merge_upsert", "merge_update"}}[self.type]
            if st not in allowed:
                raise ValueError(f"{self.id}: type {self.type} needs params.statement in "
                                 f"{sorted(allowed)}, got {st!r}")
        if self.type == "overwrite":
            # without isolation-level no validation (K1); isolation-level without
            # validate-from-snapshot-id gives a false K2 on every run (ADR-006, test_overwrite_iso_only)
            iso = self.params.get("isolation_level_option")
            vfs = self.params.get("validate_from_snapshot_id")
            if iso and vfs != "planning_snapshot":
                raise ValueError(f"{self.id}: isolation_level_option needs "
                                 "validate_from_snapshot_id: planning_snapshot (ADR-006)")
        if self.type == "expire" and self.footprint.selector != "table":
            raise ValueError(f"{self.id}: expire needs footprint selector 'table'")
        return self


class Level(_M):
    rate_mult: float = Field(gt=0)
    max_concurrent_mult: float = Field(gt=0)
    stream_rate_mult: float = Field(gt=0)


class Profile(_M):
    size: Literal["full", "lite"] = "full"


class Latency(_M):
    mean: float = 0
    jitter: float = 0


class RunnerCfg(_M):
    n_slots: int = Field(ge=1)
    heartbeat_interval_s: float = 5
    health_sample_interval_s: float = 30
    spark_pool_by_type: dict[str, str]


class Invariants(_M):
    enabled: bool = True
    checks: list[InvariantName] = Field(default_factory=list)


class Drift(_M):
    shape: Literal["sine"]
    period_s: float = Field(gt=0)
    amplitude: float = Field(ge=0, lt=1)
    applies_to: list[Literal["streams", "controlled_ops"]]


class Scenario(_M):
    name: str
    description: str = ""
    duration_s: float = Field(gt=0)
    warmup_s: float = Field(ge=0)
    cooldown_s: float = Field(ge=0)
    seed: int = 0
    profile: Profile = Profile()
    table: Table
    seed_data: SeedData
    streams: list[Stream] = Field(default_factory=list)
    maintenance: Maintenance = Maintenance()
    controlled_ops: list[ControlledOp] = Field(default_factory=list)
    concurrency_levels: dict[str, Level]
    s3_latency_ms: Latency = Latency()
    runner: RunnerCfg
    invariants: Invariants = Invariants()
    variants: dict[str, dict[str, Any]] = Field(default_factory=dict)
    drift: Drift | None = None
    extends: str | None = None
    # filled by load(): which variant / level / overrides were applied
    variant: str | None = None
    level: str | None = None

    @model_validator(mode="after")
    def _consistency(self) -> Scenario:
        ids = [o.id for o in self.controlled_ops]
        if len(ids) != len(set(ids)):
            raise ValueError(f"duplicate controlled_ops ids: {ids}")
        sids = [s.id for s in self.streams]
        if len(sids) != len(set(sids)):
            raise ValueError(f"duplicate stream ids: {sids}")
        for o in self.controlled_ops:
            if o.type not in self.runner.spark_pool_by_type:
                raise ValueError(f"{o.id}: no Spark pool for type {o.type}")
            # rewrite_position_delete_files commits nothing in v3 (ADR-006): the op must exclude v3
            if (o.type == "rewrite_deletes" and self.table.format_version == 3
                    and (o.format_versions is None or 3 in o.format_versions)):
                raise ValueError(f"{o.id}: rewrite_deletes is not planned in format v3 (ADR-006)")
        mx = self.maintenance.expire_snapshots
        if mx is not None and mx.enabled and any(o.type == "expire" for o in self.active_ops):
            raise ValueError("background expire and a controlled expire must not run together "
                             "(ADR-008)")
        return self

    @property
    def active_ops(self) -> list[ControlledOp]:
        """Controlled operations planned for this table format (`format_versions`)."""
        fv = self.table.format_version
        return [o for o in self.controlled_ops if o.format_versions is None or fv in o.format_versions]

    @property
    def expire(self) -> ExpireMaintenance | None:
        mx = self.maintenance.expire_snapshots
        return mx if mx is not None and mx.enabled else None


# ── loading ──────────────────────────────────────────────────────────────────

def deep_merge(base: dict[str, Any], patch: dict[str, Any], *, by_id: bool = False) -> dict[str, Any]:
    """Recursive dict merge; lists are replaced. With by_id, `controlled_ops` items patch the
    base items with the same id."""
    out = copy.deepcopy(base)
    for k, v in patch.items():
        if by_id and k == "controlled_ops" and isinstance(v, list):
            ops = {o["id"]: o for o in out.get("controlled_ops", [])}
            for item in v:
                if item.get("id") not in ops:
                    raise ValueError(f"variant patches unknown controlled op {item.get('id')!r}")
                ops[item["id"]] = deep_merge(ops[item["id"]], item)
            out["controlled_ops"] = [ops[o["id"]] for o in out.get("controlled_ops", [])]
        elif isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = deep_merge(out[k], v, by_id=by_id)
        else:
            out[k] = copy.deepcopy(v)
    return out


def load_raw(path: Path) -> dict[str, Any]:
    raw: dict[str, Any] = yaml.safe_load(path.read_text()) or {}
    parent = raw.get("extends")
    if parent:
        base = load_raw((path.parent / parent).resolve())
        base.pop("variants", None)
        raw = deep_merge(base, raw)
        raw["extends"] = parent
    return raw


def scenario_path(name: str, workloads: Path = WORKLOADS_DIR) -> Path:
    p = Path(name)
    if p.suffix == ".yaml" and p.exists():
        return p
    cands = [workloads / f"{name}.yaml", *sorted(workloads.glob(f"{name}_*.yaml"))]
    for c in cands:
        if c.exists():
            return c
    raise FileNotFoundError(f"scenario {name!r} not found in {workloads}")


LITE_FACTOR = 0.3


def load(name: str, *, variant: str | None = None, level: str | None = None,
         overrides: dict[str, Any] | None = None, size: str | None = None,
         workloads: Path = WORKLOADS_DIR) -> Scenario:
    """Load a scenario with a variant, matrix overrides and (optionally) a concurrency level
    applied. With `level`, rates and limits are scaled (`apply_level`)."""
    raw = load_raw(scenario_path(name, workloads))
    if variant:
        variants = raw.get("variants", {})
        if variant not in variants:
            raise ValueError(f"{raw.get('name')}: unknown variant {variant!r}")
        raw = deep_merge(raw, variants[variant], by_id=True)
    if overrides:
        raw = deep_merge(raw, overrides, by_id=True)
    if size:
        raw.setdefault("profile", {})["size"] = size
    if raw.get("profile", {}).get("size") == "lite":
        sd = raw["seed_data"]
        sd["rows_total"] = int(sd["rows_total"] * LITE_FACTOR)
        sd["files_per_partition"] = max(1, sd["files_per_partition"] // 2)
        sd["key_cardinality"] = int(sd["key_cardinality"] * LITE_FACTOR)
        for s in raw.get("streams", []):
            s["engine"] = "spark"            # lite: no Flink (tz/05 §1)
    sc = Scenario.model_validate(raw)
    sc.variant = variant
    if level:
        sc = apply_level(sc, level)
    return sc


def apply_level(sc: Scenario, level: str) -> Scenario:
    """rate_mult divides arrival intervals; max_concurrent_mult multiplies limits (ceil, ≥ 1);
    stream_rate_mult multiplies events per second, not the commit period (tz/05 §2)."""
    if level not in sc.concurrency_levels:
        raise ValueError(f"unknown concurrency level {level!r}")
    lv = sc.concurrency_levels[level]
    out = sc.model_copy(deep=True)
    for o in out.controlled_ops:
        o.arrival.interval_s = o.arrival.interval_s / lv.rate_mult
        o.max_concurrent = max(1, math.ceil(o.max_concurrent * lv.max_concurrent_mult))
    for s in out.streams:
        s.rate_events_per_s = s.rate_events_per_s * lv.stream_rate_mult
    out.level = level
    return out


# ── experiment matrix ────────────────────────────────────────────────────────

POLICIES = {"P0", "P1", "P2", "P3", "P4", "P5", "P6", "explore"}


def validate_matrix(path: Path, workloads: Path = WORKLOADS_DIR) -> list[str]:
    """Errors of config/experiments/matrix.yaml: scenarios, variants, policies, levels."""
    mx = yaml.safe_load(path.read_text())
    errs: list[str] = []
    for sname, s in mx.get("series", {}).items():
        for scen in s.get("scenarios", []):
            try:
                variant = s.get("variant")
                sc = load(scen, variant=variant, overrides=s.get("overrides"), workloads=workloads)
            except (ValueError, FileNotFoundError, ValidationError) as exc:
                errs.append(f"{sname}/{scen}: {exc}")
                continue
            runs = s.get("runs", [])
            for r in runs:
                pol = r.get("policy")
                if pol not in POLICIES:
                    errs.append(f"{sname}: unknown policy {pol!r}")
                levels = r.get("concurrency", s.get("concurrency", ["mid"]))
                for lv in levels:
                    if lv not in sc.concurrency_levels:
                        errs.append(f"{sname}/{scen}: unknown level {lv!r}")
    return errs


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="runner.scenario")
    ap.add_argument("--validate", nargs="+", required=True,
                    help="workload directories / scenario files / matrix.yaml")
    args = ap.parse_args(argv)
    errors: list[str] = []
    n = 0
    for item in args.validate:
        p = Path(item)
        if p.is_dir():
            for f in sorted(p.glob("S*.yaml")):
                raw = load_raw(f)
                for variant in [None, *raw.get("variants", {})]:
                    for size in ("full", "lite"):
                        try:
                            sc = load(str(f), variant=variant, size=size, workloads=p)
                            for lv in sc.concurrency_levels:
                                apply_level(sc, lv)
                            n += 1
                        except (ValueError, ValidationError) as exc:
                            errors.append(f"{f.name} variant={variant} size={size}: {exc}")
        elif p.name == "matrix.yaml" or "experiments" in p.parts:
            errors += [f"{p}: {e}" for e in validate_matrix(p)]
            n += 1
        else:
            try:
                load(str(p))
                n += 1
            except (ValueError, ValidationError) as exc:
                errors.append(f"{p}: {exc}")
    for e in errors:
        print("ERROR", e)
    print(f"validated {n} configurations, {len(errors)} errors")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
