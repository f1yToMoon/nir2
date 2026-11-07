#!/usr/bin/env bash
# Полные прогоны этапа 3 (P0, mid): S0 дважды с одним зерном (детерминизм), S1–S5.
set -euo pipefail
cd "$(dirname "$0")/.."
STARTED=false
run() {  # run_id scenario seed
  local short="${2%%_*}"
  if [ "$short" = "${STAGE3_FROM:-S0}" ]; then STARTED=true; fi
  if [ "$STARTED" = false ]; then return; fi
  local rid="$1${STAGE3_SUFFIX:-}"
  set -- "$rid" "$2" "$3"
  if [ -f "results/stage-3/runs/$1.json" ]; then echo "skip $1"; return; fi
  bash scripts/run_measured.sh "$1" make run SCENARIO="$2" SEED="$3" CONC=mid \
    RUNFLAGS="--run-id $1 --force" > "results/stage-3/$1.log" 2>&1
  echo "$1 exit=0"
}
run stage3-S0-P0-mid-s1 S0_baseline 1
run stage3-S0-P0-mid-s1-rep S0_baseline 1
run stage3-S1-P0-mid-s11 S1_compaction_vs_stream_append 11
run stage3-S2-P0-mid-s11 S2_compaction_vs_cdc_merge 11
run stage3-S3-P0-mid-s11 S3_compaction_vs_mor_dml 11
run stage3-S4-P0-mid-s11 S4_merge_vs_stream_serializable 11
run stage3-S5-P0-mid-s11 S5_compliance_delete 11
