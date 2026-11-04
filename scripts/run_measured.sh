#!/usr/bin/env bash
# Прогон runner с замером CPU контейнеров Spark-воркеров (cgroup v2, usage_usec) до и после:
# независимая верхняя граница для Σ executorCpuTime (проверка учёта стоимости, этап 3).
#   scripts/run_measured.sh <run_id> make run SCENARIO=... RUNFLAGS="--run-id <run_id> ..."
set -euo pipefail
RUN_ID="$1"; shift
OUT="results/stage-3/runs/${RUN_ID}.cpu.json"
mkdir -p results/stage-3/runs
read_cpu() {
  local total=0
  for c in nir-spark-worker-1-1 nir-spark-worker-2-1; do
    u=$(docker exec "$c" awk '/^usage_usec/ {print $2}' /sys/fs/cgroup/cpu.stat)
    total=$((total + u))
  done
  echo "$total"
}
b=$(read_cpu); tb=$(date +%s)
set +e
"$@"
rc=$?
set -e
a=$(read_cpu); ta=$(date +%s)
printf '{"run_id": "%s", "workers_cpu_ms": %d, "wall_s": %d, "exit": %d}\n' \
  "$RUN_ID" $(( (a - b) / 1000 )) $((ta - tb)) "$rc" > "$OUT"
cat "$OUT"
exit $rc
