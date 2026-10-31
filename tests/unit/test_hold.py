"""POST /v1/debug/hold: the max_ms of the request bounds the hold (capped by CAPL_HOLD_MAX_MS)."""
from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator

import httpx
import pytest

from capl import main

COMMIT_PATH = "/v1/namespaces/ns/tables/t"


@pytest.fixture
async def clients() -> AsyncIterator[tuple[httpx.AsyncClient, httpx.AsyncClient]]:
    main._holds.clear()
    main._client = httpx.AsyncClient(
        base_url="http://upstream",
        transport=httpx.MockTransport(lambda req: httpx.Response(200, json={"ok": True})),
    )
    adv = httpx.AsyncClient(transport=httpx.ASGITransport(app=main.advisor_app), base_url="http://adv")
    prx = httpx.AsyncClient(transport=httpx.ASGITransport(app=main.proxy_app), base_url="http://prx")
    yield adv, prx
    await adv.aclose()
    await prx.aclose()
    await main._client.aclose()
    main._holds.clear()


async def _commit(prx: httpx.AsyncClient, slot: str) -> tuple[int, float]:
    t0 = time.monotonic()
    r = await prx.post(COMMIT_PATH, content=b'{"updates": []}', headers={"X-CAPL-Slot": slot})
    return r.status_code, time.monotonic() - t0


async def test_hold_uses_request_max_ms(clients: tuple[httpx.AsyncClient, httpx.AsyncClient]) -> None:
    adv, prx = clients
    r = await adv.post("/v1/debug/hold", json={"slot": "s1", "max_ms": 200})
    assert r.status_code == 200 and r.json()["max_ms"] == 200
    status, elapsed = await _commit(prx, "s1")
    assert status == 200
    # the commit is released by the request's 200 ms deadline, not by CAPL_HOLD_MAX_MS (30 s)
    assert 0.18 <= elapsed < 2.0, elapsed
    entry = main._last_commits[-1]
    assert entry["held"] and entry["hold_timeout"] and entry["hold_max_ms"] == 200


async def test_hold_capped_by_service_limit(
    clients: tuple[httpx.AsyncClient, httpx.AsyncClient], monkeypatch: pytest.MonkeyPatch
) -> None:
    adv, prx = clients
    monkeypatch.setattr(main, "HOLD_MAX_S", 0.1)
    r = await adv.post("/v1/debug/hold", json={"slot": "s2", "max_ms": 60000})
    assert r.json()["max_ms"] == 100
    status, elapsed = await _commit(prx, "s2")
    assert status == 200 and elapsed < 2.0, elapsed
    assert main._last_commits[-1]["hold_max_ms"] == 100


async def test_hold_released_before_deadline(clients: tuple[httpx.AsyncClient, httpx.AsyncClient]) -> None:
    adv, prx = clients
    await adv.post("/v1/debug/hold", json={"slot": "s3", "max_ms": 10000})
    task = asyncio.create_task(_commit(prx, "s3"))
    await asyncio.sleep(0.1)
    assert not task.done()
    assert (await adv.delete("/v1/debug/hold/s3")).status_code == 200
    status, elapsed = await task
    assert status == 200 and elapsed < 2.0
    assert "hold_timeout" not in main._last_commits[-1]


async def test_hold_rejects_non_positive_max_ms(
    clients: tuple[httpx.AsyncClient, httpx.AsyncClient]
) -> None:
    adv, _ = clients
    assert (await adv.post("/v1/debug/hold", json={"slot": "s4", "max_ms": 0})).status_code == 422
