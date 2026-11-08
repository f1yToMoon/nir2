"""Acceptance rejects invalid and incompletely observed runs."""
import importlib.util
import json
import os
import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("stage3_checks", ROOT / "scripts/stage3_checks.py")
assert spec and spec.loader
checks = importlib.util.module_from_spec(spec)
spec.loader.exec_module(checks)


@pytest.fixture
def measured(monkeypatch: Any, tmp_path: Path) -> tuple[dict[str, Any], list[dict[str, Any]], Path]:
    t = datetime(2026, 1, 1, tzinfo=UTC)
    ops = [dict(op_id="op", outcome="committed", controlled=True, in_metrics=True,
                cpu_ms=10, cost_detail={"complete": True}, footprint_requested=["a"],
                footprint_executed=["a"], outcome_detail={}, error_class=None,
                planning_snapshot_at=t, last_attempt_at=t + timedelta(seconds=1), finished_at=t)]
    result = {"validation": {"ok": True}, "stream_freshness": {"complete": True, "n_samples": 1},
              "spark_app_totals": {"cpu_ms": 20}}

    class DB:
        def __init__(self, *_args: Any) -> None:
            pass

        def query(self, sql: str, _args: Any) -> Any:
            if sql.startswith("SELECT snapshots,current_snapshot_id"):
                return [{"snapshots": 1, "current_snapshot_id": 100}]
            if sql.startswith("SELECT forwarded_at,responded_at"):
                return [{"forwarded_at": t, "responded_at": t, "new_snapshot_id": 101,
                         "removed_snapshot_ids": None}]
            if "run_summary" in sql:
                return [{"detail": {"max_snapshots": 1, "max_metadata_bytes": 100}}]
            if "commit_attempts" in sql:
                return [{"n": 1, "s": 1}]
            if ".runs " in sql:
                return [{"scenario": "S0_baseline", "status": "finished", "invariants_ok": True,
                         "git_sha": "sha", "params": {"scenario": {"table": {"name": "bench.events"},
                         "maintenance": {
                             "expire_snapshots": {"enabled": True, "older_than_s": 1000}}}}}]
            return ops

    monkeypatch.setattr(checks, "DB", DB)
    monkeypatch.setattr(checks, "RES", tmp_path)
    monkeypatch.setattr(checks, "_load", lambda _rid: result)
    return result, ops, tmp_path / "out.json"


def invoke(measured: Any) -> dict[str, Any]:
    _, _, out = measured
    checks.main(["--runs", "r", "--out", str(out)])
    return json.loads(out.read_text())


def test_complete_run_passes(measured: Any) -> None:
    assert invoke(measured)["failed"] == []


@pytest.mark.parametrize("validation", [{"ok": False}, {}, {"ok": 1}])
def test_validation_is_required(measured: Any, validation: dict[str, Any]) -> None:
    measured[0]["validation"] = validation
    assert "r: validation.ok" in invoke(measured)["failed"]


def test_history_unresolved_is_runner_error_even_within_retention(measured: Any) -> None:
    measured[1][0]["outcome"] = "history_unresolved"
    assert "r: no runner/infrastructure errors" in invoke(measured)["failed"]


@pytest.mark.parametrize("missing", ["planning_snapshot_at", "end", "all"])
def test_retention_requires_complete_coverage(measured: Any, missing: str) -> None:
    op = measured[1][0]
    if missing in ("planning_snapshot_at", "all"):
        op["planning_snapshot_at"] = None
    if missing in ("end", "all"):
        op["last_attempt_at"] = op["finished_at"] = None
    report = invoke(measured)
    assert "r: complete history timing coverage" in report["failed"]
    assert "retention: nonempty timing coverage" in report["failed"]


def test_partial_timing_coverage_cannot_pass(measured: Any) -> None:
    incomplete = {**measured[1][0], "op_id": "missing", "planning_snapshot_at": None}
    measured[1].append(incomplete)
    report = invoke(measured)
    assert "r: complete history timing coverage" in report["failed"]
    assert report["checks"]["r: complete history timing coverage"]["observed"] == 1


def test_empty_retention_population_fails(measured: Any) -> None:
    measured[1].clear()
    assert "retention: nonempty timing coverage" in invoke(measured)["failed"]


def test_negative_duration_is_invalid(measured: Any) -> None:
    op = measured[1][0]
    op["last_attempt_at"] = op["planning_snapshot_at"] - timedelta(seconds=1)
    assert "r: complete history timing coverage" in invoke(measured)["failed"]


def test_finished_at_fallback_and_noop_without_planning_are_valid(measured: Any) -> None:
    op = measured[1][0]
    op["last_attempt_at"] = None
    op["finished_at"] = op["planning_snapshot_at"] + timedelta(seconds=1)
    measured[1].append({**op, "op_id": "noop", "outcome": "noop", "planning_snapshot_at": None,
                        "footprint_requested": []})
    assert invoke(measured)["failed"] == []


def test_nonempty_noop_requires_planning_time(measured: Any) -> None:
    measured[1][0].update(outcome="noop", planning_snapshot_at=None)
    assert "r: complete history timing coverage" in invoke(measured)["failed"]


def init_series_repo(tmp_path: Path) -> Path:
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    script = scripts / "stage3_runs.sh"
    script.write_bytes((ROOT / "scripts/stage3_runs.sh").read_bytes())
    package = tmp_path / "src/runner"
    package.mkdir(parents=True)
    for name in ("__init__.py", "env.py", "resume.py"):
        (package / name).write_bytes((ROOT / "src/runner" / name).read_bytes())
    subprocess.run(["git", "init", "-q", str(tmp_path)], check=True)
    subprocess.run(["git", "-C", str(tmp_path), "add", "scripts", "src"], check=True)
    subprocess.run(["git", "-C", str(tmp_path), "-c", "user.name=Test", "-c",
                    "user.email=test@example.test", "commit", "-qm", "fixture"], check=True)
    return script


@pytest.mark.parametrize("validation,code", [({"ok": True}, 0), ({"ok": False}, 1), ({}, 1), (None, 1)])
def test_resume_preserves_existing_results(tmp_path: Path, validation: Any, code: int) -> None:
    script = init_series_repo(tmp_path)
    runs = tmp_path / "results/stage-3/runs"
    runs.mkdir(parents=True)
    result = runs / "stage3-S5-P0-mid-s11.json"
    original = json.dumps({"validation": validation}).encode() if validation is not None else b"{broken"
    result.write_bytes(original)
    completed = subprocess.run(["bash", str(script)], env={**os.environ, "STAGE3_FROM": "S5",
                                                        "STAGE3_SUFFIX": ""},
                               capture_output=True, text=True)
    assert completed.returncode == code
    assert result.read_bytes() == original
    if code:
        assert "new run_id" in completed.stderr
        assert "skip" not in completed.stdout


@pytest.mark.parametrize("path", ["src/new.py", "config/workloads/scenario.yaml"])
def test_series_rejects_uncommitted_sources(tmp_path: Path, path: str) -> None:
    script = init_series_repo(tmp_path)
    changed = tmp_path / path
    changed.parent.mkdir(parents=True, exist_ok=True)
    changed.write_text("uncommitted")
    completed = subprocess.run(["bash", str(script)], capture_output=True, text=True)
    assert completed.returncode == 1
    assert "dirty source or scenario configuration" in completed.stderr


def test_series_uses_actual_head_for_every_run(tmp_path: Path) -> None:
    script = init_series_repo(tmp_path)
    measured = tmp_path / "scripts/run_measured.sh"
    measured.write_text('printf "%s\\n" "$@" >> results/arguments.txt\n')
    subprocess.run(["git", "-C", str(tmp_path), "add", "scripts"], check=True)
    subprocess.run(["git", "-C", str(tmp_path), "-c", "user.name=Test", "-c",
                    "user.email=test@example.test", "commit", "-qm", "measurement fixture"], check=True)
    sha = subprocess.check_output(["git", "-C", str(tmp_path), "rev-parse", "HEAD"], text=True).strip()
    (tmp_path / "results/stage-3").mkdir(parents=True)
    completed = subprocess.run(["bash", str(script)], capture_output=True, text=True,
                               env={**os.environ, "GIT_SHA": "untrusted", "STAGE3_FROM": "S4",
                                    "STAGE3_SUFFIX": ""})
    assert completed.returncode == 0, completed.stderr
    args = (tmp_path / "results/arguments.txt").read_text().splitlines()
    assert args.count(f"GIT_SHA={sha}") == 2
    assert "GIT_SHA=untrusted" not in args
    assert not any("--force" in arg for arg in args)
