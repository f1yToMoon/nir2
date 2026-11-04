"""commit_parser on real commit bodies captured through the proxy (tests/fixtures/commits,
scripts/capture_commit_fixtures.py; Iceberg 1.12.0 client, Spark 4.0, Flink, Java API).

Expected classes come from the operation the capture script executed, not from the parser.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, get_args

import pytest
import yaml

from capl.config import ProxyConfig
from capl.proxy.commit_parser import (
    AMBIGUOUS,
    EQUIVALENT,
    CommitParseError,
    CommitPath,
    OpClass,
    PathMatcher,
    classify,
    decode_namespace,
    parse_table_commit,
    parse_transaction_commit,
    response_error_type,
)

MATRIX = yaml.safe_load((Path(__file__).parents[2] / "config" / "conflict_matrix.yaml").read_text())
OP_CLASSES: dict[str, Any] = MATRIX["op_classes"]
FIXTURES = sorted((Path(__file__).parent.parent / "fixtures" / "commits").glob("*.json"))
_P = ProxyConfig()
MATCHER = PathMatcher(_P.commit_path_regex, _P.txn_commit_path_regex, _P.metrics_report_path_regex)


def _load(p: Path) -> dict[str, Any]:
    return json.loads(p.read_text())  # type: ignore[no-any-return]


def _as_list(x: Any) -> list[str]:
    return list(OP_CLASSES) if x == "*" else [x] if isinstance(x, str) else list(x)


def _outcomes(cls: str) -> dict[tuple[str, str, str], tuple[str, str]]:
    """(outcome, overlap) of every matrix rule that names `cls`, in both roles (S2-R3-04): as
    pending against each committed class and as committed against each pending class, per
    condition. A class named via "*" only is not distinguished from the others."""
    table = {}
    for r in MATRIX["rules"]:
        when = json.dumps(r.get("when", {}), sort_keys=True)
        val = (r["outcome"], r["overlap"])
        if cls in _as_list(r["pending"]):
            for c in _as_list(r["committed"]):
                table[("pending", c, when)] = val
        if r["committed"] != "*" and cls in _as_list(r["committed"]):
            for pnd in _as_list(r["pending"]):
                table[("committed", pnd, when)] = val
    return table


def _body_classes() -> set[str]:
    """Every class the proxy can put into the journal: classify() results and the candidates of
    ambiguous / equivalent body classes."""
    emitted = set(get_args(OpClass))
    for cands in (*AMBIGUOUS.values(), *EQUIVALENT.values()):
        emitted |= set(cands)
    return emitted


def _named_in_rules(cls: str, role: str) -> bool:
    return any(r[role] != "*" and cls in _as_list(r[role]) for r in MATRIX["rules"])


def test_fixture_set_is_complete() -> None:
    docs = [_load(p) for p in FIXTURES]
    assert len(docs) >= 20
    classes = {d["expected"]["op_class"] for d in docs}
    assert classes >= {"append", "overwrite", "cow_dml", "mor_delete", "mor_update", "rewrite_data",
                       "rewrite_deletes", "rewrite_manifests", "stream_upsert", "expire", "metadata"}
    ops = {d["expected"]["iceberg_op"] for d in docs}
    assert ops >= {"append", "overwrite", "replace", "delete", None}
    assert any(d["source"] == "flink" for d in docs)
    assert any(d["kind"] == "transaction" for d in docs)
    assert any("metadata delete" in d["operation"] for d in docs)
    assert {d["format_version"] for d in docs} >= {2, 3}
    # every ambiguity case has at least one real body, incl. MERGE with inserts only (S2-R1-03)
    assert {d["expected"]["ambiguity"] for d in docs} >= set(AMBIGUOUS)
    assert {"cow_dml", "mor_merge"} <= {d["expected"]["true_class"] for d in docs
                                         if d["expected"]["iceberg_op"] == "append"}
    # MoR MERGE deleting only has the body of MoR DELETE (S2-R2-03)
    assert "mor_merge" in {d["expected"]["true_class"] for d in docs
                           if d["expected"]["ambiguity"] == "delete_files_added_only"}
    assert all(d["git_sha"] not in ("unknown", "") for d in docs)


def test_ambiguity_candidates_are_matrix_classes() -> None:
    for amb, cands in AMBIGUOUS.items():
        assert set(cands) <= set(OP_CLASSES), (amb, set(cands) - set(OP_CLASSES))
        # an ambiguity is declared where candidates differ in the outcome of at least one pair of
        # the matrix (a boolean `validates` misses mor_delete vs mor_merge, S2-R2-03) or in validation
        tables = {json.dumps(sorted(_outcomes(c).items())) for c in cands}
        validates = {str(OP_CLASSES[c]["validates"]) for c in cands}
        assert len(tables) > 1 or len(validates) > 1, amb


def _relabel(table: dict[tuple[str, str, str], tuple[str, str]], cands: tuple[str, ...],
             to: str) -> str:
    """Outcome table with the other candidates renamed to `to` (mor_update x mor_merge and
    mor_merge x mor_update are the same pair for equivalent classes)."""
    return json.dumps(sorted(((role, to if c in cands else c, w), v) for (role, c, w), v in table.items()))


def test_equivalent_classes_have_identical_outcomes() -> None:
    for body_class, cands in EQUIVALENT.items():
        assert body_class in cands and set(cands) <= set(OP_CLASSES)
        tables = {_relabel(_outcomes(c), cands, body_class) for c in cands}
        assert len(tables) == 1, (body_class, cands)
        out = _outcomes(body_class)
        assert {k[0] for k in out} == {"pending", "committed"}, out    # compared in both roles
        assert len({str(OP_CLASSES[c]["validates"]) for c in cands}) == 1


def test_outcomes_cover_both_roles_and_overlap() -> None:
    # the helper itself: a class that differs only as committed, or only in overlap, is told apart
    t = _outcomes("mor_update")
    assert ("committed", "cow_dml", json.dumps({}, sort_keys=True)) in t
    assert t[("pending", "cow_dml", "{}")] == ("K2", "file")
    assert _outcomes("mor_delete") != _outcomes("mor_merge")


def test_body_classes_are_in_rules() -> None:
    """Every class the proxy journals is named in the matrix rules in both roles (not only via "*"),
    or the role is marked `no_rules` with a reason in op_classes (S2-R3-04); no rule names a
    class that is not in op_classes or that the proxy no longer emits (`superseded_by`)."""
    for cls in sorted(_body_classes()):
        assert cls in OP_CLASSES, cls
        marks = OP_CLASSES[cls].get("no_rules", {})
        for role in ("pending", "committed"):
            assert _named_in_rules(cls, role) or marks.get(role), (cls, role)
    superseded = {c for c, v in OP_CLASSES.items() if v.get("superseded_by")}
    named = {c for r in MATRIX["rules"] for role in ("pending", "committed") if r[role] != "*"
             for c in _as_list(r[role])}
    pseudo = {"append_expire"}           # committed append + expire of the base (stage 1)
    assert not named & superseded, named & superseded
    assert named - pseudo <= set(OP_CLASSES), named - pseudo - set(OP_CLASSES)


@pytest.mark.parametrize("path", FIXTURES, ids=lambda p: p.stem)
def test_real_commit_body(path: Path) -> None:
    doc = _load(path)
    exp = doc["expected"]
    body = doc["body"].encode()
    cpath = MATCHER.match("POST", doc["path"])
    assert cpath is not None and cpath.kind == doc["kind"]
    if cpath.kind == "transaction":
        commits = parse_transaction_commit(body)
        assert [c.table for c in commits] == exp["tables"]
    else:
        commits = [parse_table_commit(body, cpath)]
        assert commits[0].table == cpath.table_name
    raw = json.loads(body)
    changes = raw["table-changes"] if cpath.kind == "transaction" else [raw]
    for c, ch in zip(commits, changes, strict=True):
        assert c.op_class == exp["op_class"], (c.op_class, c.summary)
        assert c.ambiguity == exp["ambiguity"]
        # the class of the executed operation must be reachable from the body: either exact, or
        # one of the declared candidates (then only the slot → op binding decides)
        assert exp["true_class"] in c.candidates, (exp["true_class"], c.candidates)
        if exp["true_class"] not in EQUIVALENT.get(c.op_class, (c.op_class,)):
            assert c.ambiguity is not None
        assert c.iceberg_op == exp["iceberg_op"]
        assert c.has_deletes == exp["has_deletes"]
        assert c.is_flink == exp["is_flink"]
        if exp["is_flink"]:
            assert c.flink_job_id and c.flink_job_id == c.summary["flink.job-id"]
        # fields read from the body agree with the raw JSON
        snaps = [u["snapshot"] for u in ch["updates"] if u["action"] == "add-snapshot"]
        if snaps:
            s = snaps[0]
            assert c.new_snapshot_id == s["snapshot-id"]
            assert c.parent_snapshot_id == s.get("parent-snapshot-id")
            assert c.sequence_number == s.get("sequence-number")
            assert c.manifest_list == s["manifest-list"] and c.manifest_list.startswith("s3://")
            assert c.summary == s["summary"]
        else:
            assert c.new_snapshot_id is None and c.manifest_list is None
        refs = [r["snapshot-id"] for r in ch["requirements"]
                if r["type"] == "assert-ref-snapshot-id" and r.get("ref", "main") == "main"]
        assert c.base_snapshot_id == (refs[0] if refs else None)
        if exp["op_class"] == "expire":
            assert c.removed_snapshot_ids


def test_classify_synthetic_edges() -> None:
    assert classify("append", {}, ("add-snapshot",)) == "append"
    assert classify("overwrite", {"replace-partitions": "true"}, ()) == "overwrite"
    assert classify("overwrite", {"added-equality-deletes": "3"}, ()) == "stream_upsert"
    assert classify("overwrite", {"added-dvs": "1", "added-position-deletes": "2"}, ()) == "mor_update"
    assert classify("delete", {"added-position-deletes": "1"}, ()) == "mor_delete"
    rewrite_manifests = {"manifests-created": "1", "manifests-replaced": "3"}
    assert classify("replace", rewrite_manifests, ()) == "rewrite_manifests"
    assert classify("delete", {"deleted-data-files": "2"}, ()) == "cow_dml"
    assert classify("replace", {"added-data-files": "1", "deleted-data-files": "4"}, ()) == "rewrite_data"
    assert classify("replace", {"removed-delete-files": "3", "added-delete-files": "1"}, ()) == \
        "rewrite_deletes"
    assert classify(None, {}, ("remove-snapshots",)) == "expire"
    assert classify(None, {}, ("set-properties",)) == "metadata"
    assert classify("something-new", {}, ()) == "unknown"


def test_paths_with_prefix_and_namespace_separator() -> None:
    m = MATCHER.match("POST", "/v1/wh-123/namespaces/a%1Fb/tables/t")
    assert m == CommitPath("table", "wh-123", "a.b", "t")
    assert MATCHER.match("POST", "/v1/namespaces/a/tables/t/metrics").kind == "metrics"  # type: ignore[union-attr]
    assert MATCHER.match("POST", "/v1/wh/transactions/commit").kind == "transaction"  # type: ignore[union-attr]
    assert MATCHER.match("GET", "/v1/namespaces/a/tables/t") is None
    assert MATCHER.match("POST", "/v1/namespaces/a/tables") is None          # create table
    assert MATCHER.match("POST", "/v1/namespaces/a/tables/t/plan") is None
    assert decode_namespace("x%1Fy%1Fz") == "x.y.z"


def test_parse_errors() -> None:
    cp = CommitPath("table", None, "a", "t")
    with pytest.raises(CommitParseError):
        parse_table_commit(b"not json", cp)
    with pytest.raises(CommitParseError):
        parse_table_commit(b'{"updates": [{"no-action": 1}]}', cp)
    with pytest.raises(CommitParseError):
        parse_transaction_commit(b'{"table-changes": [{"updates": []}]}')
    assert parse_table_commit(b'{"updates": []}', cp).table == "a.t"


def test_response_error_type() -> None:
    assert response_error_type(b'{"error": {"type": "CommitFailedException", "code": 409}}') == \
        "CommitFailedException"
    assert response_error_type(b"<html>") is None


def test_consumers_use_journal_classes() -> None:
    """Consumers of the journal op_class know every class the proxy writes, and the features
    count only classes it writes (a renamed class would leave a feature always 0, S2-R3-04)."""
    root = Path(__file__).parents[2]
    schema = json.loads((root / "schemas" / "commit_event.schema.json").read_text())
    assert _body_classes() <= set(schema["properties"]["op_class"]["enum"])
    feats = yaml.safe_load((root / "config" / "features.yaml").read_text())
    ks = {k for f in feats["numeric"] if isinstance(f.get("for"), dict) for k in f["for"].get("k", [])}
    assert ks and ks <= _body_classes(), ks - _body_classes()
