"""Health is independent of Spark's workload queue and pinned to one metadata file."""
from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from typing import Any

import pytest

from runner import health
from runner.env import REPO, Env
from runner.metadata import capture_metadata_versions, verify_metadata_inventory
from runner.ops.executor import load_table_state
from runner.slots import build_session

UTC = timezone.utc  # noqa: UP017 — runner image is Python 3.10


@pytest.mark.parametrize('format_version', [2, 3])
def test_stage3_health_native_metadata(format_version: int, monkeypatch: pytest.MonkeyPatch) -> None:
    stamp = time.time_ns()
    rid = f'health-native-v{format_version}-{stamp}'
    env = Env()
    spark = build_session(env, rid, rid, n_slots=0)
    name = f'stage3_it.health_{stamp}'
    table = f'lake.{name}'
    out = REPO / 'results/stage-3' / os.environ.get('STAGE3_HEALTH_NAME', rid) / f'v{format_version}'
    out.mkdir(parents=True, exist_ok=False)
    started = datetime.now(UTC)
    result: dict[str, Any] = {}

    class DB:
        def __init__(self) -> None:
            self.rows: dict[str, list[Any]] = {}

        def insert_many(self, table: str, rows: list[Any]) -> None:
            self.rows[table] = rows

    class ForbiddenSpark:
        def sql(self, *_: Any) -> None:
            raise AssertionError('Health must not submit Spark jobs')

    try:
        spark.sql('CREATE NAMESPACE IF NOT EXISTS lake.stage3_it')
        spark.sql(f"CREATE TABLE {table} (event_id BIGINT,key BIGINT,dt DATE) "
                  "USING iceberg PARTITIONED BY (dt,bucket(8,key)) TBLPROPERTIES "
                  f"('format-version'='{format_version}','write.delete.mode'='merge-on-read')")
        spark.sql(f"INSERT INTO {table} SELECT id,97,DATE '2026-11-01' FROM range(10)")
        spark.sql(f'DELETE FROM {table} WHERE event_id=1')
        st = load_table_state(env, name)
        expected: dict[str, dict[str, int]] = {}
        for row in spark.sql(f'SELECT * FROM {table}.files').collect():
            part = row['partition']
            label = f"dt={part['dt']}/key_bucket={part['key_bucket']}"
            v = expected.setdefault(label, dict(data_files=0, small_files=0, delete_files=0, total_bytes=0))
            size = row['file_size_in_bytes']
            v['data_files'] += row['content'] == 0
            v['small_files'] += row['content'] == 0 and size < health.SMALL_FILE_BYTES
            v['delete_files'] += row['content'] != 0
            v['total_bytes'] += size
        assert sum(v['delete_files'] for v in expected.values()) > 0
        inspect_files = health.inspect_partition_files
        concurrent: dict[str, Any] = {}

        def commit_then_read(env: Env, location: str) -> dict[str, dict[str, int]]:
            assert location == st.metadata_location
            spark.sql(f"INSERT INTO {table} VALUES (11,98,DATE '2026-11-02')")
            concurrent['state'] = load_table_state(env, name)
            return inspect_files(env, location)

        monkeypatch.setattr(health, 'inspect_partition_files', commit_then_read)
        db = DB()
        sampler = health.HealthSampler(ForbiddenSpark(), env, db, rid, name, 30)
        sampler.sample()
        meta = db.rows['table_meta'][0]
        assert meta['current_snapshot_id'] == st.current_snapshot_id
        assert meta['current_snapshot_id'] != concurrent['state'].current_snapshot_id
        assert sampler.partition_stats() == expected
        assert meta['sample_started_at'] <= meta['metadata_observed_at'] <= meta['at']
        inventory = capture_metadata_versions(env, concurrent['state'], started, out / 'metadata',
                                              sampler.metadata_locations)
        assert inventory['complete']
        assert verify_metadata_inventory(inventory, out / 'metadata')['complete']
        assert inventory['max_metadata_bytes'] >= meta['metadata_bytes']
        result.update(expected=expected, metadata=meta, inventory=inventory,
                      concurrent_snapshot_id=concurrent['state'].current_snapshot_id)
    finally:
        (out / 'native-health.json').write_text(json.dumps(result, default=str, indent=2))
        spark.sql(f'DROP TABLE IF EXISTS {table} PURGE')
        spark.stop()
