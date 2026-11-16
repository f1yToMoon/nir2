"""Finite, fail-closed handoff from completed measurements to raw-data verification."""
import json
import os
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

root = Path(__file__).resolve().parent
repo = root.parents[2]
plan = json.loads((root / 'plan.json').read_text())
deadline = time.monotonic() + 6 * 3600


def event(kind, **values):
    with (root / 'postprocessing-events.jsonl').open('a') as f:
        f.write(json.dumps(dict(at=datetime.now(timezone.utc).isoformat(), event=kind, **values)) + '\n')


event('waiting', pid=os.getpid(), source_sha=plan['source_sha'])
try:
    while time.monotonic() < deadline:
        ready = True
        for run in plan['runs']:
            result = root.parent / 'runs' / (run['run_id'] + '.json')
            cpu = result.with_name(run['run_id'] + '.cpu.json')
            if not result.exists() or not cpu.exists():
                ready = False
                continue
            try:
                cpu_result = json.loads(cpu.read_text())
                r = json.loads(result.read_text())
            except json.JSONDecodeError:
                if time.time() - max(result.stat().st_mtime, cpu.stat().st_mtime) > 2:
                    raise
                ready = False
                continue
            if not r['validation']['ok'] or r['git_sha'] != plan['source_sha']:
                raise RuntimeError('Measurement rejected: ' + run['run_id'])
            if cpu_result['exit'] != 0:
                raise RuntimeError('Measured wrapper failed: ' + run['run_id'])
        if ready:
            break
        time.sleep(55)
    else:
        raise TimeoutError('Complete series not available within six hours')
    assert not json.loads((root / 'checks-S4-S5.json').read_text())['failed']
    faults = json.loads((root / 'fault-coverage.json').read_text())
    assert faults['n_types'] == 6 and faults['baseline_unchanged'] is True
    sha = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=repo, text=True).strip()
    assert sha == plan['source_sha'], 'HEAD changed during measurements'
    with (root / 'postprocessing-started.json').open('x') as f:
        json.dump(dict(source_sha=sha, at=datetime.now(timezone.utc).isoformat()), f)
    event('started')
    with (root / 'postprocessing.log').open('x') as log:
        p = subprocess.run(['bash', str(root / 'post_series.sh')], cwd=repo,
                           stdout=log, stderr=subprocess.STDOUT)
    with (root / 'postprocessing-result.json').open('x') as f:
        json.dump(dict(exit=p.returncode, source_sha=sha,
                       at=datetime.now(timezone.utc).isoformat()), f, indent=2)
    event('finished', exit=p.returncode)
    raise SystemExit(p.returncode)
except Exception as exc:
    event('failed', error=repr(exc))
    raise
