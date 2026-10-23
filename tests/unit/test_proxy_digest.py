"""The proxy's debug digest reads the commit body without changing it (AGENTS.md, constraint 1)."""
from __future__ import annotations

import json

from capl.digest import commit_digest as _commit_digest

BODY = {
    "requirements": [{"type": "assert-table-uuid", "uuid": "u"},
                     {"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": 7}],
    "updates": [
        {"action": "add-snapshot",
         "snapshot": {"snapshot-id": 8, "parent-snapshot-id": 7,
                      "summary": {"operation": "delete", "added-position-deletes": "10"},
                      "manifest-list": "s3://w/m.avro"}},
        {"action": "set-snapshot-ref", "ref-name": "main", "type": "branch", "snapshot-id": 8},
        {"action": "remove-snapshots", "snapshot-ids": [1, 2]},
        {"action": "set-properties", "updates": {"k": "v"}},
    ],
}


def test_digest_extracts_requirements_and_updates() -> None:
    raw = json.dumps(BODY).encode()
    d = _commit_digest(raw)
    assert d["requirements"] == BODY["requirements"]
    assert [u["action"] for u in d["updates"]] == [
        "add-snapshot", "set-snapshot-ref", "remove-snapshots", "set-properties"]
    assert d["updates"][0]["snapshot"]["summary"]["added-position-deletes"] == "10"
    assert d["updates"][1]["snapshot-id"] == 8
    assert d["updates"][2]["snapshot-ids"] == [1, 2]


def test_digest_does_not_mutate_body() -> None:
    raw = json.dumps(BODY).encode()
    before = bytes(raw)
    _commit_digest(raw)
    assert raw == before


def test_digest_non_json() -> None:
    assert _commit_digest(b"not json") == {"body_error": "not json"}
    assert _commit_digest(b"[1]") == {"body_error": "not an object"}
