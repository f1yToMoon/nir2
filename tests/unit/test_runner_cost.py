"""Cost collection must detect missing jobs and include failed stage attempts."""
from __future__ import annotations

from typing import Any

from runner.cost import CostReader


def reader(jobs: list[int], responses: dict[str, Any]) -> CostReader:
    result = CostReader.__new__(CostReader)
    result.job_ids = lambda tag: jobs  # type: ignore[method-assign]
    result._get = lambda path: responses[path]  # type: ignore[method-assign]
    return result


def test_missing_jobs_are_incomplete() -> None:
    assert not reader([], {})._read_once("missing").complete


def test_all_attempts_and_shared_stages_counted_once() -> None:
    r = reader([1, 2], {
        "/jobs/1": {"status": "SUCCEEDED", "stageIds": [10], "jobGroup": "prepare"},
        "/jobs/2": {"status": "SUCCEEDED", "stageIds": [10, 11]},
        "/stages/10": [
            {"status": "FAILED", "executorCpuTime": 2_000_000, "executorRunTime": 4},
            {"status": "COMPLETE", "executorCpuTime": 3_000_000, "executorRunTime": 5},
        ],
        "/stages/11": [{"status": "COMPLETE", "executorCpuTime": 7_000_000,
                        "executorRunTime": 8}],
    })
    cost = r._read_once("op")
    assert cost.complete and cost.cpu_ms == 12 and cost.run_ms == 17
    assert cost.stages == 3 and cost.job_groups == ["prepare"]


def test_running_stage_is_incomplete() -> None:
    r = reader([1], {"/jobs/1": {"status": "SUCCEEDED", "stageIds": [10]},
                     "/stages/10": [{"status": "ACTIVE", "executorCpuTime": 1_000_000}]})
    assert not r._read_once("op").complete
