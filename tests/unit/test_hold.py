"""Hold gates and POST /v1/debug/hold: max_ms of the request bounds the hold (capped by hold.max_ms)."""
from __future__ import annotations

import asyncio
import time

import httpx
import pytest

from capl.config import CaplConfig
from capl.proxy.hold import HoldGates
from tests.unit.conftest import Env, make_cfg

COMMIT_PATH = "/v1/namespaces/ns/tables/t"


async def _commit(prx: httpx.AsyncClient, slot: str) -> tuple[int, float]:
    t0 = time.monotonic()
    r = await prx.post(COMMIT_PATH, content=b'{"updates": []}', headers={"X-CAPL-Slot": slot})
    return r.status_code, time.monotonic() - t0


async def test_hold_uses_request_max_ms(env: Env) -> None:
    r = await env.adv.post("/v1/debug/hold", json={"slot": "s1", "max_ms": 200})
    assert r.status_code == 200 and r.json()["max_ms"] == 200
    status, elapsed = await _commit(env.proxy, "s1")
    assert status == 200
    # released by the request's 200 ms deadline, not by hold.max_ms (30 s)
    assert 0.18 <= elapsed < 2.0, elapsed
    entry = env.service.proxy.last_commits[-1]
    assert entry["held"] and entry["hold_timeout"] and entry["hold_max_ms"] == 200
    assert not env.service.proxy.gates.is_armed("s1")    # a deadline disarms the gate


@pytest.mark.parametrize("cfg", [make_cfg(max_ms=100)])
async def test_hold_capped_by_service_limit(env: Env, cfg: CaplConfig) -> None:
    r = await env.adv.post("/v1/debug/hold", json={"slot": "s2", "max_ms": 60000})
    assert r.json()["max_ms"] == 100
    status, elapsed = await _commit(env.proxy, "s2")
    assert status == 200 and elapsed < 2.0, elapsed
    assert env.service.proxy.last_commits[-1]["hold_max_ms"] == 100


async def test_hold_released_before_deadline(env: Env) -> None:
    await env.adv.post("/v1/debug/hold", json={"slot": "s3", "max_ms": 10000})
    task = asyncio.create_task(_commit(env.proxy, "s3"))
    await asyncio.sleep(0.1)
    assert not task.done()
    assert len(env.upstream.requests) == 0          # not forwarded while held
    assert (await env.adv.delete("/v1/debug/hold/s3")).status_code == 200
    status, elapsed = await task
    assert status == 200 and elapsed < 2.0
    assert "hold_timeout" not in env.service.proxy.last_commits[-1]
    assert env.service.proxy.last_commits[-1]["released_by"] == "release"


async def test_hold_released_by_competitor_commit(env: Env) -> None:
    await env.adv.post("/v1/debug/hold",
                       json={"slot": "s4", "max_ms": 10000, "release_on_commit_of": "s5"})
    held = asyncio.create_task(_commit(env.proxy, "s4"))
    await asyncio.sleep(0.1)
    assert not held.done()
    status_b, _ = await _commit(env.proxy, "s5")      # competitor commits (200)
    assert status_b == 200
    status_a, elapsed = await held
    assert status_a == 200 and elapsed < 2.0
    entry_a = next(e for e in env.service.proxy.last_commits if e["slot"] == "s4")
    assert entry_a["released_by"] == "commit:s5"
    # A was forwarded after B
    assert [r.headers["x-capl-slot"] for r in env.upstream.requests] == ["s5", "s4"]


async def test_competitor_failure_does_not_release(env: Env) -> None:
    env.upstream.handler = lambda req: httpx.Response(
        409 if req.headers.get("x-capl-slot") == "s7" else 200, json={})
    await env.adv.post("/v1/debug/hold",
                       json={"slot": "s6", "max_ms": 300, "release_on_commit_of": "s7"})
    held = asyncio.create_task(_commit(env.proxy, "s6"))
    await asyncio.sleep(0.05)
    assert (await _commit(env.proxy, "s7"))[0] == 409
    _, elapsed = await held
    assert elapsed >= 0.28                                # released only by the deadline
    assert env.service.proxy.last_commits[0]["released_by"] == "deadline"


async def test_run_stop_releases_all(env: Env) -> None:
    for s in ("a", "b"):
        await env.adv.post("/v1/debug/hold", json={"slot": s, "max_ms": 10000})
    tasks = [asyncio.create_task(_commit(env.proxy, s)) for s in ("a", "b")]
    await asyncio.sleep(0.1)
    assert (await env.adv.post("/v1/debug/run-stop")).json() == {"released": 2}
    results = await asyncio.gather(*tasks)
    assert all(st == 200 and el < 2.0 for st, el in results)


async def test_hold_rejects_non_positive_max_ms(env: Env) -> None:
    assert (await env.adv.post("/v1/debug/hold", json={"slot": "s", "max_ms": 0})).status_code == 422


async def test_gate_deadline_is_hard() -> None:
    gates = HoldGates(max_ms_limit=50)
    assert gates.register("x", 10_000) == 50
    t0 = time.monotonic()
    res = await gates.wait("x")
    assert res.timed_out and 0.045 <= time.monotonic() - t0 < 0.5
