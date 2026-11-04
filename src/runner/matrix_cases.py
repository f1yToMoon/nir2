"""Cases of the conflict-matrix experiment derived from `config/conflict_matrix.yaml: rules`.

Every rule must give at least one case (REVIEWER.md P3, review S2-R4-06). Two rules may name
the same pair (pending, committed, isolation, format) with a different overlap condition:
`cow_dml x [mor_delete, mor_update, mor_merge]` (file) and `cow_dml x [mor_update, mor_merge]`
(partition). Such cases are different experiments (the second one must show the outcome
without file overlap), so the overlap is part of the dedup key; the case id of the first
one keeps its stage-1 form (results are keyed by it), later ones get the overlap suffix.
"""
from __future__ import annotations

import itertools
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

# '*' in a rule expands to one op of each commit kind
STAR_COMMITTED = ["append", "cow_dml", "mor_delete", "rewrite_data"]


@dataclass
class Case:
    pending: str
    committed: str
    isolation: str          # "serializable" | "snapshot"
    fmt_ver: int
    overlap: str            # "none" | "partition" | "file" | "predicate"
    expected: str           # "K1" | "K2" | "success" | other observed class
    variant: str = ""       # "" | "nostartseq"
    note: str = ""
    control_expected_k2: bool = False
    rule_index: int = -1
    # overlap differs from an earlier rule with the same pair: the case must hold without the
    # stronger overlap of that rule (e.g. partition without file overlap)
    overlap_only: bool = False

    @property
    def base_id(self) -> str:
        v = f"_{self.variant}" if self.variant else ""
        return f"{self.pending}_x_{self.committed}_{self.isolation}_v{self.fmt_ver}{v}"

    @property
    def case_id(self) -> str:
        return f"{self.base_id}_ov{self.overlap}" if self.overlap_only else self.base_id

    @property
    def table_name(self) -> str:
        v = f"_{self.variant}" if self.variant else ""
        iso = "ser" if self.isolation == "serializable" else "snap"
        ov = f"_{self.overlap[:4]}" if self.overlap_only else ""
        return f"t_{self.pending}_{self.committed}_{iso}_v{self.fmt_ver}{v}{ov}"

    @property
    def cow_stmt(self) -> str:
        """SQL statement used for cow_dml: the one the MoR op of the pair does not use."""
        return "merge" if "mor_update" in (self.pending, self.committed) else "update"


def _as_list(x: Any) -> list[Any]:
    return x if isinstance(x, list) else [x]


def expand_rules(mx: Mapping[str, Any], fmt_versions: Iterable[int]) -> list[Case]:
    """All (rule x pending x committed x format) cases, in rule order, without dedup."""
    fvs_all = list(fmt_versions)
    default_iso = mx.get("defaults", {}).get("isolation", "serializable")
    cases: list[Case] = []
    for idx, rule in enumerate(mx["rules"]):
        when = rule.get("when", {}) or {}
        committed_ = rule["committed"]
        committed_list = STAR_COMMITTED if committed_ == "*" else _as_list(committed_)
        variant = "nostartseq" if when.get("rewrite_use_starting_sequence_number") is False else ""
        rule_fv = when.get("format_version")
        rule_fvs = _as_list(rule_fv) if rule_fv else fvs_all
        for pending, committed, fv in itertools.product(_as_list(rule["pending"]),
                                                        committed_list, rule_fvs):
            if fv not in fvs_all:
                continue
            cases.append(Case(
                pending=pending, committed=committed,
                isolation=when.get("isolation", default_iso), fmt_ver=fv,
                overlap=rule["overlap"], expected=rule["outcome"], variant=variant,
                note=rule.get("note", ""), rule_index=idx,
            ))
    return cases


def load_cases(mx: Mapping[str, Any], fmt_versions: Iterable[int], only: str = "") -> list[Case]:
    """Deduplicated cases. Key: (pair, isolation, format, variant, overlap).

    The first case of a pair keeps the stage-1 id; a later rule with the same pair and a
    different overlap gives a separate case marked `overlap_only`."""
    seen_key: set[tuple[str, str]] = set()
    seen_base: set[str] = set()
    unique: list[Case] = []
    for c in expand_rules(mx, fmt_versions):
        key = (c.base_id, c.overlap)
        if key in seen_key:
            continue
        seen_key.add(key)
        if c.base_id in seen_base:
            c.overlap_only = True
        seen_base.add(c.base_id)
        if only in c.case_id:
            unique.append(c)
    return unique


def rules_without_cases(mx: Mapping[str, Any], fmt_versions: Iterable[int]) -> list[int]:
    """Indices of rules none of whose cases is run (must be empty).

    A case of a rule is run if the deduplicated set has a case with the same key and the
    same expected outcome (a repeated rule is covered by its first occurrence)."""
    fvs = list(fmt_versions)
    run = {(c.base_id, c.overlap): c.expected for c in load_cases(mx, fvs)}
    by_rule: dict[int, bool] = {}
    for c in expand_rules(mx, fvs):
        hit = run.get((c.base_id, c.overlap)) == c.expected
        by_rule[c.rule_index] = by_rule.get(c.rule_index, False) or hit
    out: list[int] = []
    for idx, rule in enumerate(mx["rules"]):
        when = rule.get("when", {}) or {}
        rule_fv = when.get("format_version")
        if rule_fv and not set(_as_list(rule_fv)) & set(fvs):
            continue                      # rule for a format that is not run
        if not by_rule.get(idx, False):
            out.append(idx)
    return out


def conflicting_cases(mx: Mapping[str, Any], fmt_versions: Iterable[int]) -> list[str]:
    """Keys that two rules give with different expected outcomes (must be empty)."""
    first: dict[tuple[str, str], str] = {}
    out: list[str] = []
    for c in expand_rules(mx, fmt_versions):
        key = (c.base_id, c.overlap)
        if key in first and first[key] != c.expected:
            out.append(f"{c.base_id} overlap={c.overlap}: {first[key]} vs {c.expected}")
        first.setdefault(key, c.expected)
    return out
