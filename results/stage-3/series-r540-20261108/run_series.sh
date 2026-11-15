#!/usr/bin/env bash
set -euo pipefail
cd /home/an/nir
capl_root=results/stage-3/series-r540-20261108
capl_sha=$(git rev-parse HEAD)
STAGE3_FROM=S4 STAGE3_SUFFIX=-r540-20261108 bash scripts/stage3_runs.sh
docker compose --env-file infra/.env -f infra/docker-compose.yml exec -T runner \
  python3 - before < "$capl_root/probe.py"
docker compose --env-file infra/.env -f infra/docker-compose.yml exec -T \
  -e GIT_SHA="$capl_sha" -e STAGE3_RUN=stage3-S5-P0-mid-s11-r540-20261108 \
  -e STAGE3_EFFECTS_NAME=effects-r540-20261108 runner pytest -q tests/integration -k stage3 \
  > "$capl_root/integration.log" 2>&1
docker compose --env-file infra/.env -f infra/docker-compose.yml exec -T runner \
  python3 - after < "$capl_root/probe.py"
STAGE3_FROM=S0 STAGE3_SUFFIX=-r540-20261108 bash scripts/stage3_runs.sh
