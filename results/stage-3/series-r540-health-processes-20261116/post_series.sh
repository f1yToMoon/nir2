#!/usr/bin/env bash
# Run only after the entire measured pipeline exits successfully.
set -euo pipefail
cd /home/an/nir
capl_root=results/stage-3/series-r540-health-processes-20261116
mapfile -t capl_runs < <(python3 -c 'import json; print("\n".join(json.load(open("results/stage-3/series-r540-health-processes-20261116/plan-amendment-20261121.json"))["run_ids"]))')
python3 - <<'PY'
import hashlib
import json
import subprocess
from pathlib import Path
root=Path('results/stage-3/series-r540-health-processes-20261116')
plan=json.loads((root/'plan-amendment-20261121.json').read_text())
assert len(plan['runs'])==7
assert hashlib.sha256(Path(plan['original_plan']).read_bytes()).hexdigest()==plan['original_plan_sha256']
assert hashlib.sha256(Path(plan['previous_amendment']).read_bytes()).hexdigest()==plan['previous_amendment_sha256']
assert subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip()==plan['source_sha']
for sha in plan['code_equivalent_shas']:
    assert not subprocess.check_output(['git','diff',plan['code_fix_sha'],sha,'--',*plan['code_equivalence_paths']])
ui=json.loads((root/'ui-sampling-status.json').read_text())
assert [row['label'] for row in ui]==['early','mid','late'] and all(row['exit']==0 for row in ui)
for label in ('early','mid','late'):
    sample=json.loads((root/f'cost-ui-S4-{label}.json').read_text())
    assert sample['all_cpu_equal'] and sample['all_jobs_found_by_ui_tags']
fault=json.loads((root/'fault-coverage.json').read_text())
assert fault['n_types']==6 and fault['baseline_unchanged'] is True
for run in plan['runs']:
    result=json.loads((root.parent/'runs'/(run['run_id']+'.json')).read_text())
    assert result['validation']['ok'] is True
    assert json.loads((root.parent/'runs'/(run['run_id']+'.cpu.json')).read_text())['exit']==0
    assert result['git_sha']==run['git_sha']
    assert run['git_sha'] in plan['code_equivalent_shas']
    assert result['warmup_s']==180 and result['duration_s']==1500 and result['cooldown_s']==120
PY
test ! -e "$capl_root/checks.json"
docker compose --env-file infra/.env -f infra/docker-compose.yml exec -T runner \
  python3 scripts/stage3_checks.py --runs "${capl_runs[@]}" \
  --determinism stage3-S0-P0-mid-s1-r540-health-processes-20261116 stage3-S0-P0-mid-s1-rep-r540-health-processes-20261116 \
  --out "$capl_root/checks.json" > "$capl_root/checks.log" 2>&1
test ! -e "$capl_root/evidence/manifest.json"
docker compose --env-file infra/.env -f infra/docker-compose.yml exec -T runner \
  python3 scripts/stage3_artifacts.py --runs "${capl_runs[@]}" --out "$capl_root/evidence" \
  > "$capl_root/export.log" 2>&1
for capl_rid in "${capl_runs[@]}"; do
  docker compose --env-file infra/.env -f infra/docker-compose.yml exec -T runner \
    python3 -m runner.export --run "$capl_rid" --out data/traces >> "$capl_root/export.log" 2>&1
done
PYTHONPATH=src .venv/bin/python "$capl_root/verify_evidence.py" > "$capl_root/verification.log" 2>&1
.venv/bin/python "$capl_root/summarize.py" > "$capl_root/summary-table.log" 2>&1
.venv/bin/python "$capl_root/maintenance_summary.py" "$capl_root" > "$capl_root/maintenance-summary.log" 2>&1
.venv/bin/python "$capl_root/plot_timeline.py" > "$capl_root/plot-timeline.log" 2>&1
