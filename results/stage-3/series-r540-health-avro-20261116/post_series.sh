#!/usr/bin/env bash
# Run only after the entire measured pipeline exits successfully.
set -euo pipefail
cd /home/an/nir
capl_root=results/stage-3/series-r540-health-avro-20261116
mapfile -t capl_runs < <(python3 -c 'import json; print("\n".join(json.load(open("results/stage-3/series-r540-health-avro-20261116/plan.json"))["run_ids"]))')
python3 - <<'PY'
import json
from pathlib import Path
root=Path('results/stage-3/series-r540-health-avro-20261116')
plan=json.loads((root/'plan.json').read_text())
assert len(plan['runs'])==7
for run in plan['runs']:
    result=json.loads((root.parent/'runs'/(run['run_id']+'.json')).read_text())
    assert result['validation']['ok'] is True
    assert result['git_sha']==plan['source_sha']==run['git_sha']
    assert result['warmup_s']==180 and result['duration_s']==1500 and result['cooldown_s']==120
PY
test ! -e "$capl_root/checks.json"
docker compose --env-file infra/.env -f infra/docker-compose.yml exec -T runner \
  python3 scripts/stage3_checks.py --runs "${capl_runs[@]}" \
  --determinism stage3-S0-P0-mid-s1-r540-health-avro-20261116 stage3-S0-P0-mid-s1-rep-r540-health-avro-20261116 \
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
