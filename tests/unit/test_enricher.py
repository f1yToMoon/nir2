"""Footprint reconstruction from manifests (synthetic Avro files with the Iceberg v2 layout).

The comparison with PyIceberg on real snapshots is in tests/integration/test_proxy_stage2.py.
"""
from __future__ import annotations

import datetime as dt
import functools
from pathlib import Path
from typing import IO, Any

import fastavro

from capl.config import StateConfig
from capl.enricher import Enricher, EnrichJob, Footprint, partition_to_str, read_footprint
from capl.state import StateStore
from capl.telemetry import NullTelemetry

PART = {"type": "record", "name": "r102", "fields": [
    {"name": "dt", "type": ["null", {"type": "int", "logicalType": "date"}]},
    {"name": "key_bucket", "type": ["null", "int"]}]}
ENTRY = {"type": "record", "name": "manifest_entry", "fields": [
    {"name": "status", "type": "int"},
    {"name": "snapshot_id", "type": ["null", "long"]},
    {"name": "data_file", "type": {"type": "record", "name": "r2", "fields": [
        {"name": "content", "type": "int"},
        {"name": "file_path", "type": "string"},
        {"name": "partition", "type": PART},
        {"name": "record_count", "type": "long"},
        {"name": "file_size_in_bytes", "type": "long"}]}}]}
MLIST = {"type": "record", "name": "manifest_file", "fields": [
    {"name": "manifest_path", "type": "string"},
    {"name": "content", "type": "int"},
    {"name": "added_snapshot_id", "type": "long"}]}

D1, D2 = dt.date(2026, 11, 1), dt.date(2026, 11, 2)
SID, OLD = 200, 100


def _df(path: str, d: dt.date, b: int, content: int = 0, size: int = 1000) -> dict[str, Any]:
    return {"content": content, "file_path": path, "partition": {"dt": d, "key_bucket": b},
            "record_count": 10, "file_size_in_bytes": size}


def _write(path: Path, schema: dict[str, Any], records: list[dict[str, Any]]) -> str:
    with path.open("wb") as f:
        fastavro.writer(f, fastavro.parse_schema(schema), records)
    return str(path)


def _fixture(tmp: Path) -> str:
    # manifest written by this snapshot: 2 added data files, 1 deleted, 1 existing (ignored)
    m_new = _write(tmp / "m-new.avro", ENTRY, [
        {"status": 1, "snapshot_id": None, "data_file": _df("s3://w/a1", D1, 3)},
        {"status": 1, "snapshot_id": None, "data_file": _df("s3://w/a2", D2, 0, size=500)},
        {"status": 2, "snapshot_id": SID, "data_file": _df("s3://w/old1", D1, 3)},
        {"status": 0, "snapshot_id": OLD, "data_file": _df("s3://w/keep", D1, 3)},
        {"status": 2, "snapshot_id": OLD, "data_file": _df("s3://w/older-del", D1, 3)},
    ])
    m_del = _write(tmp / "m-del.avro", ENTRY, [
        {"status": 1, "snapshot_id": None, "data_file": _df("s3://w/pd1", D1, 3, content=1, size=77)},
    ])
    m_old = _write(tmp / "m-old.avro", ENTRY, [
        {"status": 1, "snapshot_id": None, "data_file": _df("s3://w/x", D2, 5)},
    ])
    return _write(tmp / "snap.avro", MLIST, [
        {"manifest_path": m_new, "content": 0, "added_snapshot_id": SID},
        {"manifest_path": m_del, "content": 1, "added_snapshot_id": SID},
        {"manifest_path": m_old, "content": 0, "added_snapshot_id": OLD},
    ])


def _open(path: str) -> IO[bytes]:
    return open(path, "rb")


def test_partition_to_str() -> None:
    assert partition_to_str({"dt": D1, "key_bucket": 3}) == "dt=2026-11-01/key_bucket=3"
    assert partition_to_str({"dt": None, "b": True}) == "dt=null/b=true"
    assert partition_to_str(None) == ""


def test_read_footprint(tmp_path: Path) -> None:
    fp = read_footprint(_open, _fixture(tmp_path), SID, keep_deleted_paths=True)
    assert fp.manifests_read == 2
    p1 = fp.partitions["dt=2026-11-01/key_bucket=3"]
    assert (p1.added_data_files, p1.deleted_data_files, p1.added_delete_files) == (1, 1, 1)
    assert p1.deleted_file_paths == ["s3://w/old1"]
    assert p1.added_bytes == 1000 + 77
    p2 = fp.partitions["dt=2026-11-02/key_bucket=0"]
    assert (p2.added_data_files, p2.added_bytes) == (1, 500)
    assert set(fp.partitions) == {"dt=2026-11-01/key_bucket=3", "dt=2026-11-02/key_bucket=0"}
    assert fp.totals() == {"added_data_files": 2, "deleted_data_files": 1, "added_delete_files": 1,
                           "added_bytes": 1577}


async def test_enricher_publishes_event(tmp_path: Path) -> None:
    tel = NullTelemetry()
    tel.keep = True
    state = StateStore(StateConfig())
    enr = Enricher(functools.partial(read_footprint, _open), tel, state, workers=2,
                   read_deleted_paths_for=["cow_dml"])
    await enr.start()
    assert enr.submit(EnrichJob(None, "bench.events", SID, OLD, 5, _fixture(tmp_path), "overwrite",
                                "cow_dml", "op_id", "op-1", "batch_dml", "s1", 1_000_000.0))
    assert enr.submit(EnrichJob(None, "bench.events", 999, SID, 6, str(tmp_path / "missing.avro"),
                                "append", "append", "body", None, None, None, 1_000_001.0))
    await enr.join()
    await enr.stop()
    assert (enr.published, enr.failed) == (1, 1)        # a broken snapshot does not stop workers
    [ev] = tel.rows["commit_events"]
    assert ev["snapshot_id"] == SID and ev["n_partitions"] == 2 and ev["deleted_data_files"] == 1
    parts = {r["partition"]: r for r in tel.rows["commit_partitions"]}
    assert parts["dt=2026-11-01/key_bucket=3"]["deleted_file_paths"] == ["s3://w/old1"]
    assert state.tables["bench.events"].commits[-1].op_class == "cow_dml"



async def test_debug_no_read_skips_manifests(tmp_path: Path) -> None:
    """read_manifests=False (overhead experiments): the job goes through the executor, but no
    manifest is opened; an empty event is published."""
    calls: list[str] = []
    tel = NullTelemetry()
    tel.keep = True

    def reader(ml: str, sid: int, keep: bool) -> Footprint:
        calls.append(ml)
        return read_footprint(_open, ml, sid, keep)

    enr = Enricher(reader, tel, None, workers=1)
    enr.read_manifests = False
    await enr.start()
    enr.submit(EnrichJob(None, "bench.events", SID, OLD, 5, _fixture(tmp_path), "append",
                         "append", "body", None, None, None, 1_000_000.0))
    await enr.join()
    await enr.stop()
    assert calls == [] and enr.published == 1
    [ev] = tel.rows["commit_events"]
    assert ev["snapshot_id"] == SID and ev["n_partitions"] == 0

def test_process_pool_reader_matches_thread(tmp_path: Path) -> None:
    """S3FootprintReader is picklable and gives the same footprint in a spawned worker process
    (PyIceberg FileIO reads local paths as well)."""
    import multiprocessing
    import pickle
    from concurrent.futures import ProcessPoolExecutor

    from capl.enricher import S3FootprintReader

    mlist = _fixture(tmp_path)
    reader = S3FootprintReader(())
    assert pickle.loads(pickle.dumps(reader)) == reader
    with ProcessPoolExecutor(1, mp_context=multiprocessing.get_context("spawn")) as ex:
        fp = ex.submit(reader, mlist, SID, True).result(timeout=60)
    assert fp.partitions == read_footprint(_open, mlist, SID, True).partitions
