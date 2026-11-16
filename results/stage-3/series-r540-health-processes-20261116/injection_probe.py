"""Preserve measured S5; verify six fault types without selecting DELETE outcomes."""
import json
import sys
from pathlib import Path
from runner.db import DB
from runner.env import Env
from runner.ops.executor import load_table_state

root = Path('/workspace/results/stage-3/series-r540-health-processes-20261116')
rid = 'stage3-S5-P0-mid-s11-r540-health-processes-20261116'
env = Env()
db = DB(env.pg_dsn, env.pg_schema)
try:
    baseline = {t: db.query('SELECT count(*) AS n FROM {s}.' + t + ' WHERE run_id=%s', [rid])[0]['n']
                for t in ('commit_attempts', 'commit_events', 'ops')}
    baseline['current_snapshot_id'] = load_table_state(env, 'bench.events').current_snapshot_id
    if sys.argv[1] == 'before':
        assert json.loads((root.parent / 'runs' / (rid + '.json')).read_text())['validation']['ok']
        path = root / 's5-before-injection.json'
        assert not path.exists()
        path.write_text(json.dumps(baseline, indent=2))
    else:
        assert baseline == json.loads((root / 's5-before-injection.json').read_text())
        found = [(p, json.loads(p.read_text())) for p in (root.parent / 'integration').glob('*/invariants_fault_injection.json')]
        found = [(p, d) for p, d in found if d['run_id'] == rid]
        assert len(found) == 1, len(found)
        path, primary = found[0]
        must = {'duplicate_row','lost_row','extra_update','aborted_row','lineage'}
        observed = set(primary['details'])
        assert must <= observed <= must | {'retained_deleted_row'}
        assert primary['clean']['ok'] and primary['after_restore']['ok']
        assert all(row['ok'] is False for row in primary['details'].values())
        control_path = root.parent / 'retained-health-processes-20261116/retained_deleted_fixture.json'
        control = json.loads(control_path.read_text())
        assert control['case'] == 'retained_deleted_row'
        assert control['clean']['ok'] and control['after_restore']['ok'] and not control['injected']['ok']
        mismatch = control['injected']['ledger_row_counts']
        assert mismatch['n_mismatches'] == 1
        assert sum(x['actual'] - x['expected'] for x in mismatch['mismatches'].values()) == 1
        assert mismatch['journal_physical_records'] == mismatch['snapshot_total_records'] == 2
        coverage = observed | {control['case']}
        assert coverage == must | {'retained_deleted_row'}
        requests = db.query('SELECT count(*) AS n FROM {s}.commit_attempts WHERE run_id=%s',
                            [primary['telemetry_run_id']])[0]['n']
        assert requests > 0 and len(control['commit_attempts']) >= 3
        output = root / 'fault-coverage.json'
        assert not output.exists()
        output.write_text(json.dumps(dict(run_id=rid, baseline_unchanged=True, baseline=baseline,
            primary_artifact=str(path), primary_types=sorted(observed),
            standalone_artifact=str(control_path), standalone_types=[control['case']],
            all_types=sorted(coverage), n_types=len(coverage), primary_probe_requests=requests), indent=2))
    print(sys.argv[1], baseline)
finally:
    db.close()
