#!/usr/bin/env bash
# Run only AFTER the measured series: same pytest entry as matrix_verify.sh,
# preserving the full existing YAML verified section with --no-write.
set -euo pipefail
cd /home/an/nir
capl_out=results/stage-1/iter-stage3-partition-20261115
if [ -e "$capl_out" ]; then
  echo "Preserve existing matrix results; choose a new output directory." >&2
  exit 1
fi
mkdir -p "$capl_out"
git rev-parse HEAD > "$capl_out/git_sha"
date -u +%FT%TZ > "$capl_out/started_at"
capl_sha=$(git rev-parse HEAD)
capl_one() {
  local capl_fv="$1"
  local capl_status=0
  docker compose --env-file infra/.env -f infra/docker-compose.yml exec -T \
    -e GIT_SHA="$capl_sha" -e MATRIX_FORMAT_VERSIONS="$capl_fv" \
    -e MATRIX_SLOT_SUFFIX="-stage3-v$capl_fv" \
    -e MATRIX_OUT="/workspace/$capl_out/v$capl_fv" \
    -e MATRIX_TRIALS_OVERLAP=20 -e MATRIX_TRIALS_CONTROL=5 \
    -e MATRIX_MIN_VALID=10 -e MATRIX_ONLY=ovpartition \
    runner pytest -q -rA tests/integration/test_conflict_matrix.py \
    > "$capl_out/pytest-v$capl_fv.log" 2>&1 || capl_status=$?
  echo "$capl_status" > "$capl_out/exit-v$capl_fv"
  return "$capl_status"
}
capl_one 2 &
capl_pid2=$!
sleep 30
capl_one 3 &
capl_pid3=$!
capl_status2=0
capl_status3=0
wait "$capl_pid2" || capl_status2=$?
wait "$capl_pid3" || capl_status3=$?
date -u +%FT%TZ > "$capl_out/finished_at"
if [ "$capl_status2" -ne 0 ] || [ "$capl_status3" -ne 0 ]; then
  echo "Matrix failed: v2=$capl_status2 v3=$capl_status3; see full logs."
  exit 1
fi
.venv/bin/python scripts/update_matrix.py --no-write --out "$capl_out" \
  --results "$capl_out/v2/matrix_results.json" \
  --results "$capl_out/v3/matrix_results.json" \
  > "$capl_out/update-matrix.log" 2>&1
echo "Matrix v2/v3 passed: $capl_out"
