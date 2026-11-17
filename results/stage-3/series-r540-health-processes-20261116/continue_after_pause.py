"""Resume after the saved S4/S5 check without overwriting completed evidence."""
import hashlib
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path

root = Path(__file__).resolve().parent
repo = root.parents[2]
plan = json.loads((root / 'plan-amendment.json').read_text())
compose = ['docker', 'compose', '--env-file', 'infra/.env', '-f', 'infra/docker-compose.yml']


def event(kind, **fields):
    row = dict(at=datetime.now(timezone.utc).isoformat(), event=kind, **fields)
    with (root / 'resume-progress.jsonl').open('a') as f:
        f.write(json.dumps(row) + '\n')
    print(json.dumps(row), flush=True)


def check_source():
    assert subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=repo, text=True).strip() == plan['source_sha']
    paths = plan['code_equivalence_paths']
    assert not subprocess.check_output(['git', 'status', '--porcelain', '--untracked-files=all', '--', *paths], cwd=repo)
    assert hashlib.sha256((repo / plan['original_plan']).read_bytes()).hexdigest() == plan['original_plan_sha256']
    for sha in plan['code_equivalent_shas']:
        assert not subprocess.check_output(['git', 'diff', plan['code_fix_sha'], sha, '--', *paths], cwd=repo)


def command(args, logfile, input_path=None):
    check_source()
    event('start', command=args, log=str(logfile))
    with logfile.open('x') as log:
        if input_path:
            with input_path.open('rb') as source:
                result = subprocess.run(args, cwd=repo, stdin=source, stdout=log, stderr=subprocess.STDOUT)
        else:
            result = subprocess.run(args, cwd=repo, stdout=log, stderr=subprocess.STDOUT)
    event('finish', exit=result.returncode, log=str(logfile))
    if result.returncode:
        raise RuntimeError(f'Command failed: {logfile} ({result.returncode})')


try:
    check_source()
    assert not json.loads((root / 'checks-S4-S5.json').read_text())['failed']
    for item in plan['runs'][:2]:
        result = json.loads((root.parent / 'runs' / (item['run_id'] + '.json')).read_text())
        assert result['validation']['ok'] and result['git_sha'] == item['git_sha']
    command(compose + ['exec', '-T', 'runner', 'python3', '-', 'before'],
            root / 'injection-before.log', root / 'injection_probe.py')
    command(compose + ['exec', '-T', '-e', f'GIT_SHA={plan["source_sha"]}',
                       '-e', f'STAGE3_RUN={plan["run_ids"][1]}',
                       '-e', 'STAGE3_EFFECTS_NAME=effects-health-processes-20261116',
                       '-e', 'STAGE3_SESSIONS_NAME=sessions-health-processes-20261116',
                       '-e', 'STAGE3_HEALTH_NAME=health-integration-processes-20261116',
                       '-e', 'STAGE3_RETAINED_NAME=retained-health-processes-20261116',
                       'runner', 'pytest', '-q', '-p', 'no:cacheprovider', 'tests/integration', '-k', 'stage3'],
            root / 'integration.log')
    command(compose + ['exec', '-T', 'runner', 'python3', '-', 'after'],
            root / 'injection-after.log', root / 'injection_probe.py')
    for item in plan['runs'][2:]:
        rid = item['run_id']
        result_file = root.parent / 'runs' / (rid + '.json')
        cpu_file = result_file.with_name(rid + '.cpu.json')
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
