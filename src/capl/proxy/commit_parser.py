"""Parsing of Iceberg REST commit bodies and classification of the operation class.

The parser works on a read-only copy of the body; the proxy forwards the original bytes.
Body shapes: CommitTableRequest and CommitTransactionRequest (Iceberg REST OpenAPI).
op_class mapping (tz/04 §1.2) follows the operation chosen by the Iceberg 1.12.0 commit
classes: BaseRowDelta.java:50-60, BaseOverwriteFiles.java:50-55, BaseReplacePartitions.java:36,46,
BaseRewriteFiles.java:43, FastAppend.java:62, StreamingDelete.java:37.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any, Literal
from urllib.parse import unquote

from pydantic import BaseModel, ConfigDict, Field, ValidationError

NAMESPACE_SEP = "\x1f"   # REST spec: multi-level namespace parts joined by %1F

OpClass = Literal["append", "stream_upsert", "overwrite", "cow_dml", "mor_delete", "mor_update",
                  "rewrite_data", "rewrite_deletes", "rewrite_manifests", "expire", "metadata", "unknown"]

# Body classes that do not determine whether (and how) the commit validates data: the operation
# of the commit class depends only on what files were added/removed (tz/04 §1.2, ADR-007 п. 5).
# ambiguity → classes of conflict_matrix.yaml op_classes the commit may belong to; candidates differ
# in the outcome of at least one pair of the matrix rules (tests/unit/test_commit_parser.py).
AMBIGUOUS: dict[str, tuple[str, ...]] = {
    # OverwriteFiles / RowDelta that only add data files report "append"
    # (BaseOverwriteFiles.java:54-56, BaseRowDelta.java:51-53): CoW/MoR MERGE with inserts only,
    # which validates at serializable (SparkWrite.java:460-476, SparkPositionDeltaWrite.java:282-284)
    "data_files_added_only": ("append", "cow_dml", "mor_merge"),
    # RowDelta that only adds delete files reports "delete" (BaseRowDelta.java:55-57): MoR DELETE,
    # or MoR MERGE with WHEN MATCHED THEN DELETE only, which also validates conflicting delete files
    # (SparkPositionDeltaWrite.java:277-280): mor_delete x mor_delete is K1 in v2, mor_merge x
    # mor_delete is K2 (conflict_matrix.yaml rules)
    "delete_files_added_only": ("mor_delete", "mor_merge"),
    # ReplacePartitions validates only with the write option isolation-level (SparkWrite.java:353-356);
    # options are not in the body → K1 (overwrite) or K2 (overwrite_opts)
    "overwrite_options": ("overwrite", "overwrite_opts"),
    # OverwriteFiles: CoW DML and static INSERT OVERWRITE (OverwriteByFilter, SparkWrite.java:374-404)
    "overwrite_by_filter": ("cow_dml", "insert_overwrite", "insert_overwrite_opts"),
    # data files removed, nothing added: StreamingDelete (metadata delete, no validation,
    # StreamingDelete.java:73-78, SparkTable.java:458-462) or CoW OverwriteFiles (validates)
    "data_files_removed_only": ("cow_dml", "metadata_delete"),
}

# Exact body classes that stand for several operation classes with identical outcomes in every
# rule of the matrix (so the conflict model does not need to tell them apart): RowDelta with data
# and delete files is MoR UPDATE or MERGE (both validateNoConflictingDataFiles and
# validateNoConflictingDeleteFiles, SparkPositionDeltaWrite.java:277-284).
EQUIVALENT: dict[str, tuple[str, ...]] = {
    "mor_update": ("mor_update", "mor_merge"),
}


# ---------- pydantic models (only the fields CAPL reads; everything else is allowed) ----------

class _Open(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)


class TableIdentifier(_Open):
    namespace: list[str]
    name: str


class Snapshot(_Open):
    snapshot_id: int = Field(alias="snapshot-id")
    parent_snapshot_id: int | None = Field(default=None, alias="parent-snapshot-id")
    sequence_number: int | None = Field(default=None, alias="sequence-number")
    timestamp_ms: int | None = Field(default=None, alias="timestamp-ms")
    manifest_list: str | None = Field(default=None, alias="manifest-list")
    summary: dict[str, str] = Field(default_factory=dict)
    schema_id: int | None = Field(default=None, alias="schema-id")


class Requirement(_Open):
    type: str
    ref: str | None = None
    snapshot_id: int | None = Field(default=None, alias="snapshot-id")


class Update(_Open):
    action: str
    snapshot: Snapshot | None = None
    snapshot_ids: list[int] = Field(default_factory=list, alias="snapshot-ids")
    ref_name: str | None = Field(default=None, alias="ref-name")


class CommitTableRequest(_Open):
    identifier: TableIdentifier | None = None
    requirements: list[Requirement] = Field(default_factory=list)
    updates: list[Update] = Field(default_factory=list)


class CommitTransactionRequest(_Open):
    table_changes: list[CommitTableRequest] = Field(alias="table-changes")


# ---------- result ----------

@dataclass(frozen=True)
class ParsedCommit:
    table: str                              # "ns.name" (namespace levels joined by ".")
    base_snapshot_id: int | None            # assert-ref-snapshot-id for main
    new_snapshot_id: int | None
    parent_snapshot_id: int | None
    sequence_number: int | None
    timestamp_ms: int | None
    iceberg_op: str | None                  # summary.operation of add-snapshot
    manifest_list: str | None
    summary: dict[str, str]
    removed_snapshot_ids: tuple[int, ...]
    actions: tuple[str, ...]
    op_class: OpClass
    has_deletes: bool                       # delete files added
    is_flink: bool                          # flink.job-id in summary
    flink_job_id: str | None
    op_id: str | None                       # summary[capl.op-id]
    ambiguity: str | None                   # key of AMBIGUOUS, None if the body class is exact
    candidates: tuple[str, ...]             # classes the commit may belong to (AMBIGUOUS, EQUIVALENT)
    requirements: tuple[dict[str, Any], ...] = field(default=())


@dataclass(frozen=True)
class CommitPath:
    kind: Literal["table", "transaction", "metrics"]
    prefix: str | None
    namespace: str | None
    table: str | None

    @property
    def table_name(self) -> str | None:
        return f"{self.namespace}.{self.table}" if self.namespace and self.table else None


class CommitParseError(ValueError):
    pass


def decode_namespace(raw: str) -> str:
    """'a%1Fb' -> 'a.b' (path segments are URL-encoded; levels separated by 0x1F)."""
    return ".".join(unquote(raw).split(NAMESPACE_SEP))


class PathMatcher:
    """Recognises commit / transaction / metrics-report paths by regex (prefix is optional)."""

    def __init__(self, commit_re: str, txn_re: str, metrics_re: str) -> None:
        self._commit = re.compile(commit_re)
        self._txn = re.compile(txn_re)
        self._metrics = re.compile(metrics_re)

    def match(self, method: str, path: str) -> CommitPath | None:
        if method != "POST":
            return None
        m = self._commit.match(path)
        if m:
            return CommitPath("table", m.group("prefix"), decode_namespace(m.group("ns")),
                              unquote(m.group("table")))
        m = self._txn.match(path)
        if m:
            return CommitPath("transaction", m.group("prefix"), None, None)
        m = self._metrics.match(path)
        if m:
            return CommitPath("metrics", m.group("prefix"), decode_namespace(m.group("ns")),
                              unquote(m.group("table")))
        return None


def _int(summary: dict[str, str], key: str) -> int:
    try:
        return int(summary.get(key, "0"))
    except ValueError:
        return 0


def classify(iceberg_op: str | None, summary: dict[str, str], actions: tuple[str, ...]) -> OpClass:
    """Operation class (conflict_matrix.yaml op_classes) from summary of add-snapshot."""
    if iceberg_op is None:
        if "remove-snapshots" in actions:
            return "expire"
        return "metadata" if actions else "unknown"
    added_pos = _int(summary, "added-position-deletes") + _int(summary, "added-dvs")
    added_eq = _int(summary, "added-equality-deletes")
    added_del_files = _int(summary, "added-delete-files")
    removed_del_files = _int(summary, "removed-delete-files")
    added_data = _int(summary, "added-data-files")
    deleted_data = _int(summary, "deleted-data-files")
    if iceberg_op == "append":
        return "append"
    if iceberg_op == "replace":
        if added_data == 0 and deleted_data == 0:
            if added_del_files or removed_del_files:
                return "rewrite_deletes"
            # RewriteManifests: no file changes, validates manifests only
            # (BaseRewriteManifests.java:87-89, 174, 286-298)
            return "rewrite_manifests"
        return "rewrite_data"
    if iceberg_op == "overwrite":
        if added_eq > 0:
            return "stream_upsert"
        if added_pos > 0:
            return "mor_update"               # MoR UPDATE/MERGE: data + delete files (EQUIVALENT)
        if summary.get("replace-partitions") == "true":
            return "overwrite"                # ReplacePartitions (dynamic overwrite)
        return "cow_dml"                      # OverwriteFiles (CoW DML, overwrite by filter)
    if iceberg_op == "delete":
        if added_pos > 0:
            return "mor_delete"               # MoR DELETE, or MERGE deleting only (AMBIGUOUS)
        if added_eq > 0:
            return "stream_upsert"
        return "cow_dml"                      # metadata delete / CoW removing whole files
    return "unknown"


def ambiguity(op_class: str, iceberg_op: str | None, summary: dict[str, str]) -> str | None:
    """Why the body class may not be the operation's class (None: exact)."""
    if op_class == "append":
        # Flink appends come from AppendFiles/RowDelta of a stream without validation
        return None if "flink.job-id" in summary else "data_files_added_only"
    if op_class == "overwrite":
        return "overwrite_options"
    if op_class == "cow_dml":
        return "data_files_removed_only" if iceberg_op == "delete" else "overwrite_by_filter"
    if op_class == "mor_delete":
        return "delete_files_added_only"
    return None


def _parse_table_change(req: CommitTableRequest, table: str) -> ParsedCommit:
    base = next((r.snapshot_id for r in req.requirements
                 if r.type == "assert-ref-snapshot-id" and (r.ref or "main") == "main"), None)
    snap = next((u.snapshot for u in req.updates if u.action == "add-snapshot" and u.snapshot), None)
    removed = tuple(i for u in req.updates if u.action == "remove-snapshots" for i in u.snapshot_ids)
    actions = tuple(u.action for u in req.updates)
    summary = dict(snap.summary) if snap else {}
    iceberg_op = summary.get("operation") if snap else None
    flink_job = summary.get("flink.job-id")
    added_deletes = _int(summary, "added-delete-files") > 0   # includes DVs (SnapshotSummary.java:298-311)
    op_class = classify(iceberg_op, summary, actions)
    amb = ambiguity(op_class, iceberg_op, summary)
    return ParsedCommit(
        table=table,
        base_snapshot_id=base,
        new_snapshot_id=snap.snapshot_id if snap else None,
        parent_snapshot_id=snap.parent_snapshot_id if snap else None,
        sequence_number=snap.sequence_number if snap else None,
        timestamp_ms=snap.timestamp_ms if snap else None,
        iceberg_op=iceberg_op,
        manifest_list=snap.manifest_list if snap else None,
        summary=summary,
        removed_snapshot_ids=removed,
        actions=actions,
        op_class=op_class,
        has_deletes=added_deletes,
        is_flink=flink_job is not None,
        flink_job_id=flink_job,
        op_id=summary.get("capl.op-id"),
        ambiguity=amb,
        candidates=AMBIGUOUS[amb] if amb else EQUIVALENT.get(op_class, (op_class,)),
        requirements=tuple(r.model_dump(by_alias=True, exclude_none=True) for r in req.requirements),
    )


def _load(body: bytes) -> Any:
    try:
        return json.loads(body)
    except ValueError as exc:
        raise CommitParseError(f"not json: {exc}") from exc


def parse_table_commit(body: bytes, path: CommitPath) -> ParsedCommit:
    """CommitTableRequest; the table comes from the identifier, else from the path."""
    try:
        req = CommitTableRequest.model_validate(_load(body))
    except ValidationError as exc:
        raise CommitParseError(str(exc)) from exc
    if req.identifier is not None:
        table = ".".join([*req.identifier.namespace, req.identifier.name])
    else:
        table = path.table_name or "?"
    return _parse_table_change(req, table)


def parse_transaction_commit(body: bytes) -> list[ParsedCommit]:
    """CommitTransactionRequest: one ParsedCommit per table change."""
    try:
        req = CommitTransactionRequest.model_validate(_load(body))
    except ValidationError as exc:
        raise CommitParseError(str(exc)) from exc
    out = []
    for ch in req.table_changes:
        if ch.identifier is None:
            raise CommitParseError("table change without identifier")
        out.append(_parse_table_change(ch, ".".join([*ch.identifier.namespace, ch.identifier.name])))
    return out


def response_error_type(body: bytes) -> str | None:
    """error.type of an Iceberg REST ErrorModel response (e.g. CommitFailedException)."""
    try:
        doc = json.loads(body)
    except ValueError:
        return None
    err = doc.get("error") if isinstance(doc, dict) else None
    if isinstance(err, dict) and isinstance(err.get("type"), str):
        return str(err["type"])
    return None
