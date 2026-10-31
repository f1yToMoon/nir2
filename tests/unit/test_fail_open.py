"""Fail-open (FR-PROXY-8): advisor down / failing / slow → commits pass unchanged, without hold."""
from __future__ import annotations

import asyncio
import time

import pytest

from capl import metrics
from tests.unit.conftest import Env

COMMIT = "/v1/namespaces/ns/tables/t"


def _failopen(reason: str) -> float:
    return metrics.REGISTRY.get_sample_value(
        "capl_fail_open_total", {"stage": "advisor", "reason": reason}) or 0.0


@pytest.mark.parametrize("fault,reason", [("down", "AdvisorUnavailable"), ("error", "RuntimeError"),
                                          ("slow", "TimeoutError")])
async def test_commit_passes_when_advisor_fails(env: Env, fault: str, reason: str) -> None:
    before = _failopen(reason)
    await env.adv.post("/v1/debug/hold", json={"slot": "s1", "max_ms": 10000})
    assert (await env.adv.put("/v1/debug/advisor", json={"fault": fault})).status_code == 200
    t0 = time.monotonic()
    r = await env.proxy.post(COMMIT, content=b'{"updates": []}', headers={"X-CAPL-Slot": "s1"})
    elapsed = time.monotonic() - t0
    assert r.status_code == 200 and env.upstream.bodies[-1] == b'{"updates": []}'
    # not held although a gate is armed: at most the advisor call timeout (20 ms) + overhead
    assert elapsed < 0.5, elapsed
    assert not env.service.proxy.last_commits[-1]["held"]
    assert _failopen(reason) == before + 1
    # commit is journaled even in fail-open
    assert env.telemetry.rows["commit_attempts"][-1]["http_status"] == 200


async def test_advisor_recovers(env: Env) -> None:
    await env.adv.put("/v1/debug/advisor", json={"fault": "down"})
    await env.adv.post("/v1/debug/hold", json={"slot": "s1", "max_ms": 150})
    await env.proxy.post(COMMIT, content=b"{}", headers={"X-CAPL-Slot": "s1"})
    await env.adv.put("/v1/debug/advisor", json={"fault": "none"})
    await env.proxy.post(COMMIT, content=b"{}", headers={"X-CAPL-Slot": "s1"})
    assert [e["held"] for e in env.service.proxy.last_commits] == [False, True]


async def test_concurrent_commits_under_fault(env: Env) -> None:
    await env.adv.put("/v1/debug/advisor", json={"fault": "slow"})
    rs = await asyncio.gather(*[env.proxy.post(COMMIT, content=b"{}", headers={"X-CAPL-Slot": f"s{i}"})
                                for i in range(50)])
    assert [r.status_code for r in rs] == [200] * 50
    assert len(env.upstream.requests) == 50


async def test_bookkeeping_error_does_not_affect_response(env: Env) -> None:
    def boom(*a: object, **k: object) -> None:
        raise RuntimeError("telemetry broken")

    env.service.proxy.telemetry.write = boom  # type: ignore[method-assign]
    r = await env.proxy.post(COMMIT, content=b'{"updates": []}')
    assert r.status_code == 200 and r.json() == {"ok": True}
    assert (metrics.REGISTRY.get_sample_value(
        "capl_fail_open_total", {"stage": "after_commit", "reason": "RuntimeError"}) or 0) >= 1
