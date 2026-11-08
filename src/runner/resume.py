"""Read-only validation of a finished run before any entry point skips it."""
from __future__ import annotations

import json
from pathlib import Path

from runner.env import REPO

RESULTS_DIR = REPO / "results" / "stage-3" / "runs"


def require_valid_result(path: Path) -> None:
    """A finished DB status alone is insufficient; preserve invalid/missing artifacts."""
    recovery = "preserve it and use a new run_id (STAGE3_SUFFIX for the stage-3 series)."
    try:
        result = json.loads(path.read_text())
    except (OSError, ValueError) as exc:
        raise ValueError(f"Cannot resume {path.stem}: unreadable result ({exc}); {recovery}") from exc
    validation = result.get("validation") if isinstance(result, dict) else None
    if not isinstance(validation, dict) or validation.get("ok") is not True:
        raise ValueError(f"Cannot resume {path.stem}: validation.ok is not true; {recovery}")
