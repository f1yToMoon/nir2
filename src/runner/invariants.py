"""Correctness invariants of a run (tz/08 §4, NFR-4) and orphan files (FR-RUN-7).

The runner's own ledger (`Ledger`) is built from independent inputs: golden partition counts,
events produced to Kafka and appended by agents, and exact effects of committed operations.
Deleted event ids are removed only from the expectation; actual rows are never filtered.
Updates and reinserts are replayed in successful snapshot lineage order.

  ledger_row_counts       per (partition, origin): rows in the table == ledger; plus the proxy
                          journal: golden + Σ(added-records - deleted-records) of the successful
                          commits == current snapshot total-records
  no_duplicate_event_ids  no event_id twice
  committed_ops_in_lineage  main lineage rebuilt from the proxy journal (parent links from the
                          current snapshot back to the golden one) is unbroken; every committed
                          operation's snapshot is in it exactly once
  aborted_ops_absent      no snapshot of a failed operation in the lineage, no row of it in the table
  version_monotonic       `version` = committed updates since the last insertion, for every origin
"""
from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any

import numpy as np

from runner.datagen import AGENT_BASE, OP_BASE, STREAM_BASE
from runner.slots import job_scope

if TYPE_CHECKING:
    from runner.db import DB

FAILED = ("data_conflict", "retry_exhausted", "base_expired", "history_unresolved")


@dataclass
class Ledger:
    golden_snapshot_id: int
    stream_events: list[tuple[str, Any]] = field(default_factory=list)
    # (origin, runner.streaming.EventLog) of rows that must be in the table
    excluded_ranges: list[tuple[int, int]] = field(default_factory=list)   # failed agent appends
    merge_inserted: list[tuple[int, int, str]] = field(default_factory=list)  # committed: id, key, part
    aborted_inserted: list[int] = field(default_factory=list)
    deleted_ids: set[int] = field(default_factory=set)
    merge_updated: Counter[int] = field(default_factory=Counter)

    def apply_effects(self, jr: dict[str, Any]) -> None:
        """Replay one committed operation, in successful snapshot lineage order."""
        for i, _k, _p in jr.get("deleted_rows", []):
            self.deleted_ids.add(int(i))
            self.merge_updated.pop(int(i), None)
        for i, k, part in jr.get("effect_inserted_rows", []):
            self.deleted_ids.discard(int(i))
            self.merge_updated.pop(int(i), None)
            # Golden/stream reinserts already occur in the independent baseline ledger.
            if int(i) >= OP_BASE:
                self.merge_inserted = [r for r in self.merge_inserted if r[0] != int(i)]
                self.merge_inserted.append((int(i), int(k), part))
        self.merge_updated.update(int(i) for i in jr.get("effect_updated_ids", []))


def origin_expr(col: str = "event_id") -> str:
    return (f"CASE WHEN {col} < {STREAM_BASE} THEN 'golden' "
            f"WHEN {col} >= {OP_BASE} THEN 'merge' "
            f"WHEN pmod({col}, {STREAM_BASE}) >= {AGENT_BASE} THEN 'agent' ELSE 'stream' END")


PART_EXPR = ("concat('dt=', CAST(dt AS STRING), '/key_bucket=', "
             "CAST(system.bucket({nb}, key) AS STRING))")


class Invariants:
    def __init__(self, spark: Any, db: DB, run_id: str, table: str, n_buckets: int,
                 current_snapshot_id: int | None, env: Any = None) -> None:
        self.spark, self.db, self.run_id, self.table = spark, db, run_id, table
        self.env = env
        self.nb = n_buckets
        self.current = current_snapshot_id

    def _sql(self, q: str) -> list[Any]:
        with job_scope(self.spark, f"inv-{self.run_id}", "invariants", "admin"):
            return list(self.spark.sql(q).collect())

    def _keys_view(self, keys: set[int]) -> str:
        from pyspark.sql.types import LongType, StructField, StructType
        df = self.spark.createDataFrame([(int(k),) for k in sorted(keys)] or [(-1,)],
                                        StructType([StructField("dkey", LongType(), False)]))
        df.createOrReplaceTempView("inv_deleted_keys")
        return "inv_deleted_keys"

    def run(self, led: Ledger, ops: list[dict[str, Any]], checks: list[str]) -> dict[str, Any]:
        out: dict[str, Any] = {}
        self._keys_view(led.deleted_ids)
        for name in checks:
            try:
                out[name] = getattr(self, name)(led, ops)
            except Exception as exc:
                out[name] = {"ok": False, "error": repr(exc)[:1000]}
        out["ok"] = all(v.get("ok") for k, v in out.items() if isinstance(v, dict))
        return out

    # ── checks ──
    def no_duplicate_event_ids(self, led: Ledger, ops: list[dict[str, Any]]) -> dict[str, Any]:
        r = self._sql(f"SELECT count(*) AS n FROM (SELECT event_id FROM lake.{self.table} "
                      "GROUP BY event_id HAVING count(*) > 1)")
        n = int(r[0]["n"])
        total = int(self._sql(f"SELECT count(*) AS n FROM lake.{self.table}")[0]["n"])
        return {"ok": n == 0, "duplicates": n, "rows": total}

    def ledger_row_counts(self, led: Ledger, ops: list[dict[str, Any]]) -> dict[str, Any]:
        part = PART_EXPR.format(nb=self.nb)
        actual: Counter[tuple[str, str]] = Counter()
        for r in self._sql(
                f"SELECT {part} AS p, {origin_expr()} AS o, count(*) AS n FROM lake.{self.table} t "
                f"GROUP BY 1, 2"):
            actual[(r["p"], r["o"])] += int(r["n"])
        expected: Counter[tuple[str, str]] = Counter()
        for r in self._sql(
                f"SELECT {part} AS p, count(*) AS n FROM lake.{self.table} "
                f"VERSION AS OF {led.golden_snapshot_id} t "
                f"LEFT ANTI JOIN inv_deleted_keys d ON t.event_id = d.dkey GROUP BY 1"):
            expected[(r["p"], "golden")] += int(r["n"])
        dk = np.array(sorted(led.deleted_ids), dtype=np.int64)
        for origin, log in led.stream_events:
            ids = np.frombuffer(log.ids, dtype=np.int64)
            pidx = np.frombuffer(log.part_idx, dtype=np.uint16)
            keep = ~np.isin(ids, dk)
            for ex_lo, ex_hi in led.excluded_ranges:
                keep &= ~((ids >= ex_lo) & (ids < ex_hi))
            counts = np.bincount(pidx[keep], minlength=len(log.labels))
            for i, n in enumerate(counts.tolist()):
                if n:
                    expected[(log.labels[i], origin)] += int(n)
        for _i, _k, p in led.merge_inserted:
            if _i not in led.deleted_ids:
                expected[(p, "merge")] += 1
        diff = {f"{p}|{o}": {"expected": expected.get((p, o), 0), "actual": actual.get((p, o), 0)}
                for (p, o) in set(actual) | set(expected)
                if expected.get((p, o), 0) != actual.get((p, o), 0)}
        by_origin: Counter[str] = Counter()
        for (_p, o), n in actual.items():
            by_origin[o] += n
        # proxy journal: Σ(added - deleted records) of successful commits since the golden snapshot
        # only snapshots of the main lineage (a commit later rolled back is not part of the table)
        chain, _err = self._lineage(led)
        jr = self.db.query(
            "SELECT coalesce(sum((summary->>'added-records')::bigint), 0) AS added,"
            " coalesce(sum((summary->>'deleted-records')::bigint), 0) AS deleted, count(*) AS n"
            " FROM {s}.commit_attempts WHERE run_id = %s AND table_name = %s"
            " AND http_status BETWEEN 200 AND 299 AND summary IS NOT NULL"
            " AND new_snapshot_id = ANY(%s)", [self.run_id, self.table, chain])[0]
        golden_total = int(self._sql(f"SELECT count(*) AS n FROM lake.{self.table} "
                                     f"VERSION AS OF {led.golden_snapshot_id}")[0]["n"])
        total = int(self._sql(f"SELECT count(*) AS n FROM lake.{self.table}")[0]["n"])
        # physical rows of data files: the journal must contain every commit since the golden
        # snapshot; with MoR, deleted rows are position deletes (not deleted-records), so the
        # comparison is with total-records of the current snapshot, not with count(*)
        from runner.ops.executor import load_table_state
        st = load_table_state(self.env, self.table) if self.env is not None else None
        cur_summary = {}
        if st is not None and st.current_snapshot_id is not None:
            cur_summary = next((x.get("summary", {}) for x in st.metadata.get("snapshots", [])
                                if int(x["snapshot-id"]) == st.current_snapshot_id), {})
        physical = int(cur_summary.get("total-records", -1))
        journal_total = golden_total + int(jr["added"]) - int(jr["deleted"])
        nonzero = {o: n for o, n in by_origin.items() if n}
        return {"ok": not diff and journal_total == physical, "partitions_checked": len(expected),
                "mismatches": dict(list(diff.items())[:50]), "n_mismatches": len(diff),
                "rows_by_origin": nonzero, "journal_physical_records": journal_total,
                "snapshot_total_records": physical, "table_live_rows": total,
                "journal_commits": int(jr["n"])}

    def _lineage(self, led: Ledger) -> tuple[list[int], str | None]:
        rows = self.db.query("SELECT snapshot_id, parent_snapshot_id FROM {s}.commit_events"
                             " WHERE table_name = %s AND run_id = %s", [self.table, self.run_id])
        parent = {int(r["snapshot_id"]): r["parent_snapshot_id"] for r in rows}
        chain: list[int] = []
        cur = self.current
        while cur is not None and cur != led.golden_snapshot_id:
            if cur not in parent:
                return chain, f"snapshot {cur} not in the proxy journal"
            chain.append(cur)
            p = parent[cur]
            cur = int(p) if p is not None else None
        if cur != led.golden_snapshot_id:
            return chain, "lineage does not reach the golden snapshot"
        return chain, None

    def committed_ops_in_lineage(self, led: Ledger, ops: list[dict[str, Any]]) -> dict[str, Any]:
        chain, err = self._lineage(led)
        pos = Counter(chain)
        bad = []
        n = 0
        for o in ops:
            if o.get("outcome") != "committed" or o.get("op_type") == "expire":
                continue
            n += 1
            sid = o.get("snapshot_id")
            if sid is None or pos.get(int(sid), 0) != 1:
                bad.append({"op_id": o["op_id"], "snapshot_id": sid})
        return {"ok": err is None and not bad,
                "lineage_len": len(chain), "lineage_error": err, "committed_checked": n,
                "missing": bad[:20], "n_missing": len(bad)}

    def aborted_ops_absent(self, led: Ledger, ops: list[dict[str, Any]]) -> dict[str, Any]:
        chain, err = self._lineage(led)
        failed = {o["op_id"] for o in ops if o.get("outcome") in FAILED}
        in_chain = self.db.query(
            "SELECT op_id, snapshot_id FROM {s}.commit_events WHERE table_name = %s AND run_id = %s"
            " AND op_id = ANY(%s)", [self.table, self.run_id, list(failed)]) if failed else []
        chain_set = set(chain)
        hits = [r for r in in_chain if int(r["snapshot_id"]) in chain_set]
        rows_present = 0
        if led.aborted_inserted:
            from pyspark.sql.types import LongType, StructField, StructType
            self.spark.createDataFrame([(int(i),) for i in led.aborted_inserted],
                                       StructType([StructField("aid", LongType(), False)])
                                       ).createOrReplaceTempView("inv_aborted")
            rows_present = int(self._sql(
                f"SELECT count(*) AS n FROM lake.{self.table} t JOIN inv_aborted a "
                f"ON t.event_id = a.aid")[0]["n"])
        return {"ok": err is None and not hits and rows_present == 0, "failed_ops": len(failed),
                "snapshots_in_lineage": hits[:20], "aborted_rows_checked": len(led.aborted_inserted),
                "aborted_rows_present": rows_present, "lineage_error": err}

    def version_monotonic(self, led: Ledger, ops: list[dict[str, Any]]) -> dict[str, Any]:
        from pyspark.sql.types import IntegerType, LongType, StructField, StructType
        self.spark.createDataFrame(
            [(int(i), int(c)) for i, c in led.merge_updated.items()] or [(-1, 0)],
            StructType([StructField("uid", LongType(), False), StructField("cnt", IntegerType())])
        ).createOrReplaceTempView("inv_merge_upd")
        q = (f"SELECT count(*) AS checked, sum(CASE WHEN version <> exp THEN 1 ELSE 0 END) AS bad, "
             f"sum(CASE WHEN exp > 0 THEN 1 ELSE 0 END) AS updated FROM ("
             f"SELECT t.version, coalesce(m.cnt, 0) AS exp FROM lake.{self.table} t "
             f"LEFT JOIN inv_merge_upd m ON t.event_id = m.uid)")
        r = self._sql(q)[0]
        bad = int(r["bad"] or 0)
        return {"ok": bad == 0, "rows_checked": int(r["checked"] or 0),
                "rows_expected_updated": int(r["updated"] or 0), "mismatches": bad}


def orphan_scan(spark: Any, env: Any, run_id: str, table: str, since: datetime,
                location: str) -> dict[str, Any]:
    """Orphan files written by the run: objects under the table location modified after the start
    of the run that the current metadata does not reach (all_files, all_manifests, manifest lists,
    metadata log). The Spark procedure remove_orphan_files refuses an interval < 24 h
    (RemoveOrphanFilesProcedure), so the set difference is computed here. Old metadata.json files
    beyond the metadata log are reported separately (they are kept because
    write.metadata.delete-after-commit.enabled = false)."""
    import pyarrow.fs as pafs

    from runner.health import _aws
    reach: set[str] = set()
    with job_scope(spark, f"orphans-{run_id}", "orphans", "admin"):
        for q in (f"SELECT file_path AS p FROM lake.{table}.all_files",
                  f"SELECT path AS p FROM lake.{table}.all_manifests",
                  f"SELECT manifest_list AS p FROM lake.{table}.snapshots",
                  f"SELECT file AS p FROM lake.{table}.metadata_log_entries"):
            reach |= {str(r["p"]).split("://", 1)[-1] for r in spark.sql(q).collect()}
    fs = pafs.S3FileSystem(endpoint_override=env.s3_endpoint, scheme="http",
                           access_key=_aws("AWS_ACCESS_KEY_ID"), secret_key=_aws("AWS_SECRET_ACCESS_KEY"),
                           region=_aws("AWS_REGION", "us-east-1"))
    root = location.split("://", 1)[1].rstrip("/")
    infos = fs.get_file_info(pafs.FileSelector(root, recursive=True, allow_not_found=True))
    out = {"orphan_files": 0, "orphan_bytes": 0, "old_metadata_json": 0, "listed_since_start": 0}
    for inf in infos:
        if inf.type != pafs.FileType.File or inf.mtime is None or inf.mtime.timestamp() < since.timestamp():
            continue
        out["listed_since_start"] += 1
        if inf.path in reach:
            continue
        if inf.path.endswith(".metadata.json"):
            out["old_metadata_json"] += 1
            continue
        out["orphan_files"] += 1
        out["orphan_bytes"] += int(inf.size or 0)
    return out
