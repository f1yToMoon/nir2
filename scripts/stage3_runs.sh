#!/usr/bin/env bash
# Полные прогоны этапа 3 (P0, mid): S0 дважды с одним зерном (детерминизм), S1–S5.
set -euo pipefail
cd "$(dirname "$0")/.."
SOURCE_PATHS=(src tests scripts config infra api schemas Makefile pyproject.toml)
if [ -n "$(git status --porcelain --untracked-files=all -- "${SOURCE_PATHS[@]}")" ]; then
  echo "Cannot start series: dirty source or scenario configuration; commit changes first." >&2
  exit 1
fi
RUN_SHA=$(git rev-parse HEAD)
STARTED=false
run() {  # run_id scenario seed
  local short="${2%%_*}"
  if [ "$short" = "${STAGE3_FROM:-S0}" ]; then STARTED=true; fi
  if [ "$STARTED" = false ]; then return; fi
  local rid="$1${STAGE3_SUFFIX:-}"
  set -- "$rid" "$2" "$3"
  if [ -f "results/stage-3/runs/$1.json" ]; then
    python3 - "$1" <<'PY'
import json
import sys
from pathlib import Path

rid = sys.argv[1]
try:
    result = json.loads(Path(f"results/stage-3/runs/{rid}.json").read_text())
    valid = (result.get("validation") or {}).get("ok") is True
except (OSError, ValueError, AttributeError) as exc:
    sys.exit(f"Cannot resume {rid}: unreadable result ({exc}); preserve it and use a new run_id (STAGE3_SUFFIX).")
if not valid:
    sys.exit(f"Cannot resume {rid}: validation.ok is not true; preserve it and use a new run_id (STAGE3_SUFFIX).")
PY
    echo "skip $1 (validation.ok=true)"
    return
  fi
  bash scripts/run_measured.sh "$1" make run GIT_SHA="$RUN_SHA" SCENARIO="$2" SEED="$3" CONC=mid \
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
