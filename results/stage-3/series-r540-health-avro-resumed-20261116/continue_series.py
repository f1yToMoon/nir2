"""Finite continuation of the frozen stage-3 plan after explicit user resume."""
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path

root = Path(__file__).resolve().parent
repo = root.parents[2]
plan = json.loads((root / 'plan.json').read_text())
sha = plan['source_sha']
compose = ['docker', 'compose', '--env-file', 'infra/.env', '-f', 'infra/docker-compose.yml']


def event(kind, **fields):
    with (root / 'progress.jsonl').open('a') as f:
        f.write(json.dumps(dict(at=datetime.now(timezone.utc).isoformat(), event=kind, **fields)) + '\n')
    print(kind, fields, flush=True)


def check_source():
    assert subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=repo, text=True).strip() == sha
    paths = ['src', 'tests', 'scripts', 'config', 'infra', 'api', 'schemas', 'Makefile', 'pyproject.toml']
    assert not subprocess.check_output(['git', 'status', '--porcelain', '--untracked-files=all', '--', *paths], cwd=repo)


def command(args, log, input_path=None):
    check_source()
    event('start', command=args, log=str(log))
    with log.open('x') as f:
        if input_path:
            with input_path.open('rb') as source:
                p = subprocess.run(args, cwd=repo, stdin=source, stdout=f, stderr=subprocess.STDOUT)
        else:
            p = subprocess.run(args, cwd=repo, stdout=f, stderr=subprocess.STDOUT)
    event('finish', log=str(log), exit=p.returncode)
    if p.returncode:
        raise RuntimeError(f'Command failed ({p.returncode}); see {log}')


def run(item):
    rid = item['run_id']
    result = root.parent / 'runs' / (rid + '.json')
    cpu = result.with_name(rid + '.cpu.json')
    assert not result.exists() and not cpu.exists(), f'Preserve previous measurement: {rid}'
    command(['bash', 'scripts/run_measured.sh', rid, 'make', 'run', f'GIT_SHA={sha}',
             f'SCENARIO={item["scenario"]}', f'SEED={item["seed"]}', 'POLICY=P0', 'CONC=mid',
             f'RUNFLAGS=--run-id {rid}'], root.parent / (rid + '.log'))
    data = json.loads(result.read_text())
    assert data['validation']['ok'] is True and data['git_sha'] == sha
    assert json.loads(cpu.read_text())['exit'] == 0


try:
    check_source()
    s4 = json.loads((root.parent / 'runs' / (plan['run_ids'][0] + '.json')).read_text())
    assert s4['validation']['ok'] is True and s4['git_sha'] == sha
    run(plan['runs'][1])
    command(compose + ['exec', '-T', 'runner', 'python3', 'scripts/stage3_checks.py', '--runs',
                       *plan['run_ids'][:2], '--out', str(root.relative_to(repo) / 'checks-S4-S5.json')],
            root / 'checks-S4-S5.log')
    command(compose + ['exec', '-T', 'runner', 'python3', '-', 'before'],
            root / 'injection-before.log', root / 'injection_probe.py')
    command(compose + ['exec', '-T', '-e', f'GIT_SHA={sha}', '-e', f'STAGE3_RUN={plan["run_ids"][1]}',
                       '-e', 'STAGE3_EFFECTS_NAME=effects-health-avro-resumed-20261116',
                       '-e', 'STAGE3_SESSIONS_NAME=sessions-health-avro-resumed-20261116',
                       '-e', 'STAGE3_HEALTH_NAME=health-integration-avro-resumed-20261116',
                       '-e', 'STAGE3_RETAINED_NAME=retained-health-avro-resumed-20261116',
                       'runner', 'pytest', '-q', '-p', 'no:cacheprovider', 'tests/integration', '-k', 'stage3'],
            root / 'integration.log')
    command(compose + ['exec', '-T', 'runner', 'python3', '-', 'after'],
            root / 'injection-after.log', root / 'injection_probe.py')
    for item in plan['runs'][2:]:
        run(item)
    command(['bash', str(root / 'post_series.sh')], root / 'postprocessing.log')
    event('completed', source_sha=sha)
except BaseException as exc:
    event('stopped', error=repr(exc))
    raise
