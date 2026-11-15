"""Health must not backdate a concurrent commit or hide missed acquisition intervals."""
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any

import pytest

from runner import health
from runner.env import Env
from runner.ops.executor import TableState


def test_delayed_spark_collect_must_not_backdate_rest_state(monkeypatch: Any) -> None:
    t0 = datetime(2026, 1, 1, tzinfo=UTC)
    clock = [t0]

    class Clock:
        @staticmethod
        def now(_tz: Any) -> datetime:
            return clock[0]

    class Rows:
        def collect(self) -> list[Any]:
            clock[0] += timedelta(seconds=151)
            return []

    class DB:
        def __init__(self) -> None:
            self.rows: dict[str, list[Any]] = {}

        def insert_many(self, table: str, rows: list[Any]) -> None:
            self.rows[table] = rows

    db = DB()
    monkeypatch.setattr(health, "datetime", Clock)
    monkeypatch.setattr(health, "job_scope", lambda *_: __import__('contextlib').nullcontext(), raising=False)
    monkeypatch.setattr(health, "load_table_state", lambda *_: TableState(2, {2: {}}, "md", {}))
    monkeypatch.setattr(health, "s3_object_size", lambda *_: 12)
    sampler = health.HealthSampler(SimpleNamespace(sql=lambda _: Rows()), Env(), db, "r", "n.t", 30)
    # New native metadata inspection must bypass this deliberately slow Spark collect.
    if hasattr(health, "inspect_partition_files"):
        monkeypatch.setattr(health, "inspect_partition_files", lambda *_: {})
    sampler.sample()
    assert db.rows['table_meta'][0]['at'] == clock[0]


@pytest.mark.parametrize("gap", [59.4505, 158.434401])
def test_missed_health_intervals_are_rejected(gap: float) -> None:
    t = datetime(2026, 1, 1, tzinfo=UTC)
    result = health.health_coverage([t, t + timedelta(seconds=gap)], t,
                                   t + timedelta(seconds=gap), 30)
    assert not result['ok']


def test_health_coverage_checks_edges_and_jitter() -> None:
    t = datetime(2026, 1, 1, tzinfo=UTC)
    times = [t + timedelta(seconds=x) for x in (0, 30.001, 60.002, 90)]
    assert health.health_coverage(times, t, t + timedelta(seconds=90), 30)['ok']
    assert not health.health_coverage(times[2:], t, t + timedelta(seconds=90), 30)['ok']
    assert not health.health_coverage(times, t, t + timedelta(seconds=150), 30)['ok']


def test_dense_metadata_bytes_detect_spike_between_periodic_samples() -> None:
    from runner.metadata import metadata_bound
    files = [{'location': 'a', 'bytes': 100}, {'location': 'b', 'bytes': 1_510_001},
             {'location': 'c', 'bytes': 110}]
    result = metadata_bound(files, {'b'}, {'a', 'b', 'c'}, deletion_enabled=False)
    assert result['complete']
    assert result['max_metadata_bytes'] > 1_510_000
    assert not metadata_bound(files, {'b', 'missing'}, {'a', 'b', 'c'},
                              deletion_enabled=False)['complete']
    assert not metadata_bound(files, {'b'}, {'a', 'b', 'c'}, deletion_enabled=True)['complete']


def test_future_metadata_is_rejected_even_with_regular_samples() -> None:
    t = datetime(2026, 1, 1, tzinfo=UTC)
    row = dict(at=t, sample_started_at=t, metadata_observed_at=t,
               metadata_location='file', current_snapshot_id=2)
    commits = [dict(new_snapshot_id=2, forwarded_at=t + timedelta(seconds=151))]
    assert not health.validate_health_rows([row], commits, t, t, 30)['ok']
    row.update(at=t + timedelta(seconds=152), metadata_observed_at=t + timedelta(seconds=152))
    assert health.validate_health_rows([row], commits, row['at'], row['at'], 30)['ok']


def test_metadata_archive_bytes_are_verified(tmp_path: Any) -> None:
    import gzip
    import hashlib

    from runner.metadata import verify_metadata_inventory
    raw = b'{"properties":{},"padding":"' + b'x' * 1_510_000 + b'"}'
    entry = dict(location='s3://bucket/spike.metadata.json', bytes=len(raw), archive='spike.json.gz',
                 sha256=hashlib.sha256(raw).hexdigest())
    (tmp_path / entry['archive']).write_bytes(gzip.compress(raw))
    inv = dict(files=[entry], required_locations=[entry['location']], listed=1, complete=True)
    verified = verify_metadata_inventory(inv, tmp_path)
    assert verified['complete'] and verified['max_metadata_bytes'] > 1_510_000
    entry['bytes'] = 100
    with pytest.raises(ValueError, match='bytes/hash mismatch'):
        verify_metadata_inventory(inv, tmp_path)


@pytest.mark.parametrize('initial_start,expected_wait', [(90.0, 20.0), (80.0, 10.0)])
def test_first_deadline_is_anchored_to_initial_acquisition(
        monkeypatch: Any, initial_start: float, expected_wait: float) -> None:
    waits = []
    sampler = health.HealthSampler(None, Env(), None, 'r', 'n.t', 30)
    sampler._stats_at = 90.0
    sampler._sample_started_monotonic = initial_start
    sampler._stop = SimpleNamespace(wait=lambda delay: waits.append(delay) or True)
    monkeypatch.setattr(health.time, 'monotonic', lambda: 100.0)
    sampler._loop()
    assert waits == [expected_wait]  # Include nonzero initial acquisition, not just zero-duration reads.


@pytest.mark.parametrize('bad_field', [None, 'status', 'content'])
def test_manifest_decoder_buffers_once_and_rejects_unknown_enums(monkeypatch: Any, bad_field: Any) -> None:
    import io

    import fastavro
    from pyiceberg.table import StaticTable

    schema = {'type': 'record', 'name': 'entry', 'fields': [
        {'name': 'status', 'type': 'int'},
        {'name': 'data_file', 'type': {'type': 'record', 'name': 'file', 'fields': [
            {'name': 'content', 'type': 'int'}, {'name': 'file_size_in_bytes', 'type': 'long'},
            {'name': 'partition', 'type': {'type': 'record', 'name': 'part', 'fields': [
                {'name': 'dt', 'type': {'type': 'int', 'logicalType': 'date'}},
                {'name': 'key_bucket', 'type': 'int'}]}}]}}]}
    from datetime import date
    rows = [dict(status=status, data_file=dict(content=content, file_size_in_bytes=size,
                partition=dict(dt=date(2026, 1, 1), key_bucket=0)))
            for status, content, size in [(0, 0, 10), (1, 0, 20), (1, 1, 30), (1, 2, 40), (2, 0, 50)]]
    if bad_field == 'status':
        rows[0]['status'] = 3
    elif bad_field == 'content':
        rows[0]['data_file']['content'] = 3
    buffer = io.BytesIO()
    fastavro.writer(buffer, schema, rows)
    payload = buffer.getvalue()
    reads = []

    class Source(io.BytesIO):
        def read(self, size: int = -1) -> bytes:
            reads.append(size)
            assert size == -1, 'Use a single full object read, not S3 reads for individual Avro fields'
            return super().read(size)

    manifests = [SimpleNamespace(manifest_path='s3://bucket/manifest')]
    file_io = SimpleNamespace(new_input=lambda _: SimpleNamespace(open=lambda: Source(payload)))
    table = SimpleNamespace(io=file_io,
                            current_snapshot=lambda: SimpleNamespace(manifests=lambda _: manifests))
    monkeypatch.setattr(StaticTable, 'from_metadata', lambda *_a, **_k: table)
    cache: dict[str, Any] = {}
    if bad_field:
        with pytest.raises(ValueError, match='manifest'):
            health.inspect_partition_files(Env(), 'metadata', cache)
        return
    expected = {'dt=2026-01-01/key_bucket=0': dict(data_files=2, small_files=2,
                                                delete_files=2, total_bytes=100)}
    assert health.inspect_partition_files(Env(), 'metadata', cache) == expected
    assert health.inspect_partition_files(Env(), 'metadata', cache) == expected
    assert reads == [-1]
    manifests.clear()
    assert health.inspect_partition_files(Env(), 'metadata', cache) == {}
