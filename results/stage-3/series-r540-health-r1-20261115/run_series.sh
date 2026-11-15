#!/usr/bin/env bash
set -euo pipefail
cd /home/an/nir
capl_root=results/stage-3/series-r540-health-r1-20261115
capl_sha=$(git rev-parse HEAD)
test "$capl_sha" = e0c01c163edee5c490d5d83637b83138697d8e20
STAGE3_FROM=S4 STAGE3_SUFFIX=-r540-health-r1-20261115 bash scripts/stage3_runs.sh
docker compose --env-file infra/.env -f infra/docker-compose.yml exec -T runner \
  python3 - before < "$capl_root/injection_probe.py"
docker compose --env-file infra/.env -f infra/docker-compose.yml exec -T \
  -e GIT_SHA="$capl_sha" -e STAGE3_RUN=stage3-S5-P0-mid-s11-r540-health-r1-20261115 \
  -e STAGE3_EFFECTS_NAME=effects-health-r1-20261115 \
  -e STAGE3_SESSIONS_NAME=sessions-health-r1-20261115 \
  -e STAGE3_HEALTH_NAME=health-integration-r1-20261115 \
  -e STAGE3_RETAINED_NAME=retained-health-r1-20261115 \
  runner pytest -q -p no:cacheprovider tests/integration -k stage3 \
  > "$capl_root/integration.log" 2>&1
docker compose --env-file infra/.env -f infra/docker-compose.yml exec -T runner \
  python3 - after < "$capl_root/injection_probe.py"
STAGE3_FROM=S0 STAGE3_SUFFIX=-r540-health-r1-20261115 bash scripts/stage3_runs.sh
