"""Regression prepared against the unchanged measurement implementation."""
import io
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
from datetime import date, datetime, timedelta
from types import SimpleNamespace

import fastavro
import pytest
from pyiceberg.table import StaticTable

from runner import health
from runner.env import Env


@pytest.mark.parametrize('bad_field', [None, 'status', 'content'])
def test_sampler_decodes_outside_collector_interpreter(monkeypatch, bad_field):
    schema = {'type': 'record', 'name': 'entry', 'fields': [
        {'name': 'status', 'type': 'int'},
        {'name': 'data_file', 'type': {'type': 'record', 'name': 'file', 'fields': [
            {'name': 'content', 'type': 'int'}, {'name': 'file_size_in_bytes', 'type': 'long'},
            {'name': 'partition', 'type': {'type': 'record', 'name': 'part', 'fields': [
                {'name': 'dt', 'type': {'type': 'int', 'logicalType': 'date'}},
                {'name': 'key_bucket', 'type': 'int'}]}}]}}]}
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
        def read(self, size=-1):
            reads.append(size)
            assert size == -1
            return super().read(size)

    file_io = SimpleNamespace(new_input=lambda _: SimpleNamespace(open=lambda: Source(payload)))
    table = SimpleNamespace(io=file_io,
                            current_snapshot=lambda: SimpleNamespace(manifests=lambda _: [
                                SimpleNamespace(manifest_path='s3://bucket/manifest')]))
    monkeypatch.setattr(StaticTable, 'from_metadata', lambda *_a, **_kw: table)
    monkeypatch.setattr(health, 'load_table_state', lambda *_a: SimpleNamespace(
        metadata_location='metadata', current_snapshot_id=1, snapshots=[1]))
    monkeypatch.setattr(health, 's3_object_size', lambda *_a: 100)

    def forbidden(*_a, **_kw):
        raise AssertionError('CPU-bound Avro decoding ran in the collector interpreter')

    monkeypatch.setattr(fastavro, 'reader', forbidden)
    writes = {}
    db = SimpleNamespace(insert_many=lambda name, values: writes.update({name: values}))
    sampler = health.HealthSampler(None, Env(), db, 'r', 'n.t', 30)
    # Spawn, rather than fork: no Spark JVM state or patched parent decoder is inherited.
    with ProcessPoolExecutor(2, mp_context=multiprocessing.get_context('spawn')) as pool:
        sampler._decoder_pool = pool
        if bad_field:
            with pytest.raises(ValueError, match='manifest'):
                sampler.sample()
            return
        sampler.sample()
        assert sampler.partition_stats() == {'dt=2026-01-01/key_bucket=0': dict(
            data_files=2, small_files=2, delete_files=2, total_bytes=100)}
        assert reads == [-1]
        assert writes['table_meta'][0]['current_snapshot_id'] == 1


def test_sampler_close_releases_pool_and_is_idempotent(monkeypatch):
    sampler = health.HealthSampler(None, Env(), None, 'r', 'n.t', 30)
    calls = []
    sampler._decoder_pool = SimpleNamespace(shutdown=lambda **kw: calls.append(kw))
    monkeypatch.setattr(sampler, 'stop', lambda: None)
    sampler.close()
    sampler.close()
    assert calls == [dict(wait=True, cancel_futures=True)]
    assert sampler._decoder_pool is None


def test_observed_s1_gap_is_rejected_without_relaxing_budget():
    start = datetime(2026, 1, 1, tzinfo=health.UTC)
    finish = start + timedelta(seconds=31.094039)
    result = health.health_coverage([start, finish], start, finish, 30)
    assert result['ok'] is False
    assert result['jitter_budget_s'] == 1
    assert result['missed_intervals'] == 1
