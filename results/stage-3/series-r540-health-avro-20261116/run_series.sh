#!/usr/bin/env bash
set -euo pipefail
cd /home/an/nir
capl_root=results/stage-3/series-r540-health-avro-20261116
capl_sha=$(git rev-parse HEAD)
git merge-base --is-ancestor 89e8dd0cd3d95200d7893ee191f19aacbc04df7a "$capl_sha"
capl_planned_sha=$(python3 -c 'import json; print(json.load(open("results/stage-3/series-r540-health-avro-20261116/plan.json"))["source_sha"])')
test "$capl_sha" = "$capl_planned_sha"
STAGE3_FROM=S4 STAGE3_SUFFIX=-r540-health-avro-20261116 bash scripts/stage3_runs.sh
docker compose --env-file infra/.env -f infra/docker-compose.yml exec -T runner \
  python3 scripts/stage3_checks.py \
  --runs stage3-S4-P0-mid-s11-r540-health-avro-20261116 stage3-S5-P0-mid-s11-r540-health-avro-20261116 \
  --out "$capl_root/checks-S4-S5.json" > "$capl_root/checks-S4-S5.log" 2>&1
docker compose --env-file infra/.env -f infra/docker-compose.yml exec -T runner \
  python3 - before < "$capl_root/injection_probe.py"
docker compose --env-file infra/.env -f infra/docker-compose.yml exec -T \
  -e GIT_SHA="$capl_sha" -e STAGE3_RUN=stage3-S5-P0-mid-s11-r540-health-avro-20261116 \
  -e STAGE3_EFFECTS_NAME=effects-health-avro-20261116 \
  -e STAGE3_SESSIONS_NAME=sessions-health-avro-20261116 \
  -e STAGE3_HEALTH_NAME=health-integration-avro-20261116 \
  -e STAGE3_RETAINED_NAME=retained-health-avro-20261116 \
  runner pytest -q -p no:cacheprovider tests/integration -k stage3 \
  > "$capl_root/integration.log" 2>&1
docker compose --env-file infra/.env -f infra/docker-compose.yml exec -T runner \
  python3 - after < "$capl_root/injection_probe.py"
STAGE3_FROM=S0 STAGE3_SUFFIX=-r540-health-avro-20261116 bash scripts/stage3_runs.sh
