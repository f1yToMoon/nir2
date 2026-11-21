"""Finite continuation: preserve five completed runs; new S2, unstarted S3, full checks."""
import hashlib
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path

here = Path(__file__).resolve().parent
root = here.parent
repo = root.parents[2]
plan = json.loads((root / 'plan-amendment-20261121.json').read_text())


def event(kind, **fields):
    row = {'at': datetime.now(timezone.utc).isoformat(), 'event': kind, **fields}
    with (here / 'progress.jsonl').open('a') as f:
        f.write(json.dumps(row) + '\n')
    print(json.dumps(row), flush=True)


def check_source():
    assert subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=repo, text=True).strip() == plan['source_sha']
    paths = plan['code_equivalence_paths']
    assert not subprocess.check_output(['git', 'status', '--porcelain', '--untracked-files=all', '--', *paths], cwd=repo)
    for field in ['original_plan', 'previous_amendment']:
        assert hashlib.sha256((repo / plan[field]).read_bytes()).hexdigest() == plan[field + '_sha256']
    for sha in plan['code_equivalent_shas']:
        assert not subprocess.check_output(['git', 'diff', plan['code_fix_sha'], sha, '--', *paths], cwd=repo)


def command(args, log_path):
    check_source()
    event('start', command=args, log=str(log_path))
    with log_path.open('x') as log:
        result = subprocess.run(args, cwd=repo, stdout=log, stderr=subprocess.STDOUT)
    event('finish', exit=result.returncode, log=str(log_path))
    if result.returncode:
        raise RuntimeError(f'Command failed: {log_path} ({result.returncode})')


try:
    check_source()
    assert not json.loads((root / 'paused-20261117/checks-five.json').read_text())['failed']
    for item in plan['runs'][:5]:
        result = json.loads((root.parent / 'runs' / (item['run_id'] + '.json')).read_text())
        assert result['validation']['ok'] and result['invariants']['ok']
        assert result['git_sha'] == item['git_sha']
        assert json.loads((root.parent / 'runs' / (item['run_id'] + '.cpu.json')).read_text())['exit'] == 0
    for item in plan['runs'][5:]:
        rid = item['run_id']
        result_file = root.parent / 'runs' / (rid + '.json')
        cpu_file = root.parent / 'runs' / (rid + '.cpu.json')
        assert not result_file.exists() and not cpu_file.exists(), f'Preserve previous attempt: {rid}'
        command(['bash', 'scripts/run_measured.sh', rid, 'make', 'run', f'GIT_SHA={item["git_sha"]}',
                 f'SCENARIO={item["scenario"]}', f'SEED={item["seed"]}', 'POLICY=P0', 'CONC=mid',
                 f'RUNFLAGS=--run-id {rid}'], root.parent / (rid + '.log'))
        data = json.loads(result_file.read_text())
        assert data['validation']['ok'] and data['git_sha'] == item['git_sha']
        assert json.loads(cpu_file.read_text())['exit'] == 0
    command(['bash', str(root / 'post_series.sh')], root / 'postprocessing.log')
    event('completed')
except BaseException as exc:
    event('stopped', error=repr(exc))
    raise
