#!/usr/bin/env bash
set -euo pipefail
cd /home/an/nir
capl_root=results/stage-3/series-r540-health-cache-20261115
capl_sha=$(git rev-parse HEAD)
git merge-base --is-ancestor 3f135180216ce831c01818b4140502f8fb3c5b6e "$capl_sha"
capl_planned_sha=$(python3 -c 'import json; print(json.load(open("results/stage-3/series-r540-health-cache-20261115/plan.json"))["source_sha"])')
test "$capl_sha" = "$capl_planned_sha"
STAGE3_FROM=S4 STAGE3_SUFFIX=-r540-health-cache-20261115 bash scripts/stage3_runs.sh
docker compose --env-file infra/.env -f infra/docker-compose.yml exec -T runner \
  python3 scripts/stage3_checks.py \
  --runs stage3-S4-P0-mid-s11-r540-health-cache-20261115 stage3-S5-P0-mid-s11-r540-health-cache-20261115 \
  --out "$capl_root/checks-S4-S5.json" > "$capl_root/checks-S4-S5.log" 2>&1
docker compose --env-file infra/.env -f infra/docker-compose.yml exec -T runner \
  python3 - before < "$capl_root/injection_probe.py"
docker compose --env-file infra/.env -f infra/docker-compose.yml exec -T \
  -e GIT_SHA="$capl_sha" -e STAGE3_RUN=stage3-S5-P0-mid-s11-r540-health-cache-20261115 \
  -e STAGE3_EFFECTS_NAME=effects-health-cache-20261115 \
  -e STAGE3_SESSIONS_NAME=sessions-health-cache-20261115 \
  -e STAGE3_HEALTH_NAME=health-integration-cache-20261115 \
  -e STAGE3_RETAINED_NAME=retained-health-cache-20261115 \
  runner pytest -q -p no:cacheprovider tests/integration -k stage3 \
  > "$capl_root/integration.log" 2>&1
docker compose --env-file infra/.env -f infra/docker-compose.yml exec -T runner \
  python3 - after < "$capl_root/injection_probe.py"
STAGE3_FROM=S0 STAGE3_SUFFIX=-r540-health-cache-20261115 bash scripts/stage3_runs.sh
