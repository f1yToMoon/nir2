"""Verify real fault injection without contaminating the measured S5 telemetry."""
import json
import sys
from pathlib import Path
from runner.db import DB
from runner.env import Env
from runner.ops.executor import load_table_state

root = Path('/workspace/results/stage-3/series-r540-sessions-20261115')
rid = 'stage3-S5-P0-mid-s11-r540-sessions-20261115'
env = Env()
db = DB(env.pg_dsn, env.pg_schema)
try:
    counts = {t: db.query('SELECT count(*) AS n FROM {s}.' + t + ' WHERE run_id=%s', [rid])[0]['n']
              for t in ('commit_attempts', 'commit_events', 'ops')}
    counts['current_snapshot_id'] = load_table_state(env, 'bench.events').current_snapshot_id
    if sys.argv[1] == 'before':
        result = json.loads((root.parent / 'runs' / f'{rid}.json').read_text())
        assert result['validation']['ok'] is True
        expected = db.query('SELECT new_snapshot_id FROM {s}.commit_attempts WHERE run_id=%s '
                            'AND http_status BETWEEN 200 AND 299 AND new_snapshot_id IS NOT NULL '
                            'ORDER BY sequence_number DESC LIMIT 1', [rid])[0]['new_snapshot_id']
        assert counts['current_snapshot_id'] == expected, 'table is not the new final S5'
        (root / 's5-before-injection.json').write_text(json.dumps(counts, indent=2))
    else:
        assert counts == json.loads((root / 's5-before-injection.json').read_text())
        files = list((root.parent / 'integration').glob('*/invariants_fault_injection.json'))
        matching = [p for p in files if json.loads(p.read_text())['run_id'] == rid]
        assert matching, 'no fault injection for the new S5'
        path = max(matching, key=lambda p: p.stat().st_mtime)
        assert path.stat().st_mtime >= (root / 's5-before-injection.json').stat().st_mtime
        result = json.loads(path.read_text())
        assert result['clean']['ok'] and result['after_restore']['ok']
        assert len(result['details']) == 6 and 'retained_deleted_row' in result['details']
        n = db.query('SELECT count(*) AS n FROM {s}.commit_attempts WHERE run_id=%s',
                     [result['telemetry_run_id']])[0]['n']
        assert n > 0
        (root / 's5-after-injection.json').write_text(json.dumps({
            'baseline_after': counts, 'baseline_unchanged': True, 'probe_requests': n,
            'artifact': str(path), 'specific_faults': list(result['details'])}, indent=2))
finally:
    db.close()
print(sys.argv[1], counts)
