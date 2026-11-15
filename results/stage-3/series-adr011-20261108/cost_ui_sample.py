import json
import sys
import requests
from runner.db import DB
from runner.env import Env

rid = sys.argv[1]
env = Env()
db = DB(env.pg_dsn, env.pg_schema)
try:
    ops = db.query("SELECT op_id, op_type, controlled, outcome, cpu_ms, cost_detail FROM {s}.ops "
                   "WHERE run_id=%s AND cpu_ms > 0 AND outcome IS NOT NULL ORDER BY finished_at", [rid])
finally:
    db.close()
base = 'http://localhost:4040/api/v1/applications'
apps = requests.get(base, timeout=10).json()
assert len(apps) == 1, apps
base += '/' + apps[0]['id']
jobs = requests.get(base + '/jobs', timeout=10).json()
samples = []
for op in ops:
    recorded = op['cost_detail']
    if not recorded.get('complete'):
        continue
    selected = [j for j in jobs if op['op_id'] in j.get('jobTags', [])]
    assert selected, op
    assert set(j['jobId'] for j in selected) == set(recorded['jobs']), op
    stages = sorted({s for j in selected for s in j['stageIds']})
    attempts = []
    skipped = []
    for sid in stages:
        response = requests.get(base + f'/stages/{sid}', timeout=10)
        if response.status_code == 404:
            skipped.append(sid)
            continue
        response.raise_for_status()
        attempts.extend(dict(stage_id=sid, **a) for a in response.json() if a['status'] != 'SKIPPED')
    cpu_ms = sum(int(a['executorCpuTime'] or 0) // 1000000 for a in attempts)
    assert cpu_ms == op['cpu_ms'], (cpu_ms, op)
    samples.append({'op_id': op['op_id'], 'type': op['op_type'], 'controlled': op['controlled'],
                    'outcome': op['outcome'], 'stored_cpu_ms': op['cpu_ms'], 'ui_cpu_ms': cpu_ms,
                    'jobs': selected, 'stage_attempts': attempts, 'never_submitted_stages': skipped})
assert samples and any(x['controlled'] for x in samples), samples
print(json.dumps({'run_id': rid, 'app_id': apps[0]['id'], 'samples': samples,
                  'all_jobs_found_by_ui_tags': True, 'all_cpu_equal': True}, indent=1))
