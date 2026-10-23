#!/usr/bin/env bash
# Stage 1: run tests/integration/test_conflict_matrix.py for format v2 and v3 in parallel
# (separate Spark applications, hold slots and output dirs), then scripts/update_matrix.py.
# Usage: scripts/matrix_verify.sh [out-dir-name]   (default: iter3)
# Env passed through: MATRIX_TRIALS_OVERLAP, MATRIX_TRIALS_CONTROL, MATRIX_MIN_VALID, MATRIX_ONLY
set -u
cd "$(dirname "$0")/.."
NAME="${1:-iter3}"
OUT="results/stage-1/$NAME"
mkdir -p "$OUT"
COMPOSE="docker compose --env-file infra/.env -f infra/docker-compose.yml"

run_fv() {
  local fv="$1"
  $COMPOSE exec -T \
    -e MATRIX_FORMAT_VERSIONS="$fv" -e MATRIX_SLOT_SUFFIX="-v$fv" \
    -e MATRIX_OUT="/workspace/$OUT/v$fv" \
    -e MATRIX_TRIALS_OVERLAP="${MATRIX_TRIALS_OVERLAP:-20}" \
    -e MATRIX_TRIALS_CONTROL="${MATRIX_TRIALS_CONTROL:-5}" \
    -e MATRIX_MIN_VALID="${MATRIX_MIN_VALID:-10}" \
    -e MATRIX_ONLY="${MATRIX_ONLY:-}" \
    runner pytest -q -rA tests/integration/test_conflict_matrix.py > "$OUT/pytest-v$fv.log" 2>&1
  echo "exit $?" >> "$OUT/pytest-v$fv.log"
}

git rev-parse HEAD > "$OUT/git_sha"
date -u +%FT%TZ > "$OUT/started_at"
run_fv 2 &
sleep 30
run_fv 3 &
wait
date -u +%FT%TZ > "$OUT/finished_at"
tail -n 3 "$OUT"/pytest-v*.log
python3 scripts/update_matrix.py --out "$OUT" \
  --results "$OUT/v2/matrix_results.json" --results "$OUT/v3/matrix_results.json"
grep -q "^exit 0" "$OUT/pytest-v2.log" && grep -q "^exit 0" "$OUT/pytest-v3.log"
