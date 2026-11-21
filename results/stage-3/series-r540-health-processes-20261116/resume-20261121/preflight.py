"""Read-only comparison with the paused runtime and exact planned run identities."""
import importlib.metadata
import json
import sys
from pathlib import Path

import pyspark
import requests

from runner.db import DB
from runner.env import Env
from runner.ops.executor import load_table_state

root = Path(__file__).resolve().parents[1]
env = Env()
runtime = {
    'python': sys.version, 'pyspark': pyspark.__version__,
    'dependencies': {n: importlib.metadata.version(n) for n in
                     ['pyiceberg', 'pyarrow', 'fastavro', 'numpy', 'pandas']},
    'resources': {k: getattr(env, k) for k in ['spark_master', 'executor_memory',
                  'driver_memory', 'executor_cores', 'cores_max']},
    'flink_version': requests.get(f'{env.flink_rest}/config', timeout=15).json()['flink-version'],
}
assert runtime == json.loads((root / 'resume-runtime.json').read_text()), runtime
session = requests.Session()
jobs = session.get(f'{env.flink_rest}/jobs/overview', timeout=15).json()['jobs']
active_flink = [j for j in jobs if j['state'] not in ['FINISHED', 'FAILED', 'CANCELED', 'SUSPENDED']]
active_spark = session.get('http://spark-master:8080/json/', timeout=15).json()['activeapps']
slots = session.get(f'{env.advisor_uri}/v1/state', timeout=15).json()['slots']
holds = session.get(f'{env.advisor_uri}/v1/debug/holds', timeout=15).json()['holds']
assert not active_flink and not active_spark and not slots and not holds
snapshot = load_table_state(env, 'bench.events').current_snapshot_id
assert snapshot == 7534423856391059834, snapshot
plan = json.loads((root / 'plan-amendment.json').read_text())
expected = plan['run_ids'][:6]
db = DB(env.pg_dsn, env.pg_schema)
try:
    runs = db.query('SELECT run_id,status,invariants_ok FROM {s}.runs WHERE run_id = ANY(%s) '
                    'ORDER BY run_id', [expected])
    assert {r['run_id'] for r in runs} == set(expected)
    assert all(r['status'] == 'finished' and r['invariants_ok'] is True
               for r in runs if r['run_id'] in expected[:5])
    assert next(r for r in runs if r['run_id'] == expected[5])['status'] == 'failed'
    others = db.query('SELECT run_id,status FROM {s}.runs WHERE run_id LIKE %s '
                      'AND NOT (run_id = ANY(%s))', ['%r540-health-processes-20261116', expected])
    print(json.dumps({'runtime': runtime, 'snapshot': snapshot, 'runs': runs,
                      'other_matching_runs_preserved': others,
                      'active_spark_apps': active_spark, 'active_flink_jobs': active_flink,
                      'slots': slots, 'holds': holds, 'routing': env.routing}, indent=2, default=str))
finally:
    db.close()
