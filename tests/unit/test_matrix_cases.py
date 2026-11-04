"""Every rule of config/conflict_matrix.yaml gives a regression case (S2-R4-06, REVIEWER.md P3)."""
from __future__ import annotations

from pathlib import Path

import yaml

from runner.matrix_cases import conflicting_cases, load_cases, rules_without_cases

MATRIX = Path(__file__).parents[2] / "config/conflict_matrix.yaml"


def _mx() -> dict:
    return yaml.safe_load(MATRIX.read_text())


def test_every_rule_gives_a_case() -> None:
    mx = _mx()
    fvs = mx.get("format_versions", [2, 3])
    missing = rules_without_cases(mx, fvs)
    assert missing == [], [mx["rules"][i] for i in missing]


def test_no_rule_contradicts_another() -> None:
    mx = _mx()
    assert conflicting_cases(mx, mx.get("format_versions", [2, 3])) == []


def test_same_pair_other_overlap_is_a_separate_case() -> None:
    # the case dropped before S2-R4-06: cow_dml x mor_update, file (K2) and partition (K2)
    mx = {"rules": [
        {"pending": "cow_dml", "committed": ["mor_delete", "mor_update"], "outcome": "K2",
         "overlap": "file"},
        {"pending": "cow_dml", "committed": ["mor_update"], "outcome": "K2", "overlap": "partition",
         "when": {"isolation": "serializable"}},
    ]}
    cases = load_cases(mx, [2])
    ids = [c.case_id for c in cases]
    assert ids == ["cow_dml_x_mor_delete_serializable_v2", "cow_dml_x_mor_update_serializable_v2",
                   "cow_dml_x_mor_update_serializable_v2_ovpartition"]
    assert [c.overlap_only for c in cases] == [False, False, True]
    assert rules_without_cases(mx, [2]) == []
    assert len({c.table_name for c in cases}) == 3


def test_detects_a_rule_without_cases() -> None:
    # same key and different outcome: the second rule is never run → reported
    mx = {"rules": [
        {"pending": "append", "committed": "cow_dml", "outcome": "K1", "overlap": "none"},
        {"pending": "append", "committed": "cow_dml", "outcome": "K2", "overlap": "none"},
    ]}
    assert rules_without_cases(mx, [2]) == [1]
    assert conflicting_cases(mx, [2]) != []


def test_stage1_case_ids_unchanged() -> None:
    mx = _mx()
    ids = {c.case_id for c in load_cases(mx, [2, 3])}
    for cid in ("rewrite_data_x_cow_dml_serializable_v2", "mor_delete_x_mor_delete_serializable_v2",
                "cow_dml_x_mor_update_serializable_v3"):
        assert cid in ids
