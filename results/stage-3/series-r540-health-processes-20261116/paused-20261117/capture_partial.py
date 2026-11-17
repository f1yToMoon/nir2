"""Preserve the interrupted S2 without presenting it as a qualified measurement."""
import hashlib
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests

from runner.db import DB
from runner.env import REPO, Env
from runner.export import TABLES
from runner.metadata import capture_metadata_versions
from runner.ops.executor import load_table_state
sys.path.insert(0, str(REPO))
from scripts.stage3_artifacts import save_bytes, save_json

root = Path(__file__).resolve().parent
rid = 'stage3-S2-P0-mid-s11-r540-health-processes-20261116'
target = root / 'partial-S2'
target.mkdir(exist_ok=False)
env = Env()
db = DB(env.pg_dsn, env.pg_schema)
try:
    files = {}
    counts = {}
    for name, query in TABLES.items():
        rows = db.query(query, [rid])
        counts[name] = len(rows)
        save_json(target, name + '.json.gz', rows, len(rows), files)
    run = db.query(TABLES['runs'], [rid])[0]
    assert run['status'] == 'failed'
    for source in sorted((Path('data/runs') / rid).glob('*.json')):
        save_bytes(target, 'journals/' + source.name + '.gz', source.read_bytes(), 1, files)
    state = load_table_state(env, 'bench.events')
    meta = db.query(TABLES['table_meta'], [rid])
    inventory = capture_metadata_versions(env, state, run['started_at'], target / 'metadata',
                                         {r['metadata_location'] for r in meta})
    jobs = requests.get(f'{env.flink_rest}/jobs/overview', timeout=15).json()['jobs']
    active_flink = [j for j in jobs if j['state'] not in ('FINISHED', 'FAILED', 'CANCELED', 'SUSPENDED')]
    active_spark = requests.get('http://spark-master:8080/json/', timeout=15).json()['activeapps']
    slots = requests.get(f'{env.advisor_uri}/v1/state', timeout=15).json()['slots']
    holds = requests.get(f'{env.advisor_uri}/v1/debug/holds', timeout=15).json()['holds']
    assert not active_flink and not active_spark and not slots and not holds
    # Include the inventory and every compressed metadata object in the same manifest.
    for source in sorted(target.rglob('*')):
        if not source.is_file():
            continue
        name = str(source.relative_to(target))
        if name not in files:
            files[name] = {'sha256': hashlib.sha256(source.read_bytes()).hexdigest()}
    result = dict(at=datetime.now(timezone.utc).isoformat(), run_id=rid,
                  status=run['status'], reason='Explicit user pause; SIGINT; incomplete diagnostic run',
                  qualified=False, final_result_exists=(Path('results/stage-3/runs') / (rid + '.json')).exists(),
                  ledger_exists=(Path('data/runs') / rid / 'ledger.pkl').exists(), counts=counts,
                  metadata_versions=len(inventory['files']), metadata_bytes_upper_bound=inventory['max_metadata_bytes'],
                  current_snapshot_id=state.current_snapshot_id, active_flink_jobs=0, active_spark_apps=0,
                  slots=slots, holds=holds, containers_and_volumes_preserved=True,
                  runner_processes=subprocess.check_output(['ps', '-eo', 'pid,ppid,stat,comm'], text=True),
                  files=files)
    (target / 'manifest.json').write_text(json.dumps(result, indent=2))
    print(json.dumps({k: v for k, v in result.items() if k != 'files'}, indent=2))
finally:
    db.close()
