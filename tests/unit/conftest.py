"""Shared fixtures: CAPL service wired to a mock catalog (httpx.MockTransport)."""
from __future__ import annotations

from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass, field
from typing import Any

import httpx
import pytest

from capl.config import CaplConfig
from capl.main import Service, build_service
from capl.proxy.app import make_upstream_client
from capl.telemetry import NullTelemetry

Handler = Callable[[httpx.Request], Any]


class _Body(httpx.AsyncByteStream):
    def __init__(self, data: bytes, chunk: int = 7) -> None:
        self.data, self.chunk = data, chunk

    async def __aiter__(self) -> AsyncIterator[bytes]:
        for i in range(0, len(self.data), self.chunk):
            yield self.data[i:i + self.chunk]


@dataclass
class Upstream(httpx.AsyncBaseTransport):
    """Mock catalog. Like a network transport, it consumes request.stream (what is actually sent)
    and returns a streamed (not preloaded) response, so raw-byte passthrough is exercised."""
    handler: Handler | None = None
    requests: list[httpx.Request] = field(default_factory=list)
    bodies: list[bytes] = field(default_factory=list)
    fail: Exception | None = None

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        if self.fail is not None:
            raise self.fail
        body = b"".join([c async for c in request.stream])  # type: ignore[union-attr]
        self.requests.append(request)
        self.bodies.append(body)
        r = self.handler(request) if self.handler else httpx.Response(200, json={"ok": True})
        if isinstance(r, tuple):
            # (status, exact header list, body): no header is added by httpx
            status, hdrs, raw = r
            return httpx.Response(status, headers=hdrs, stream=_Body(raw), request=request)
        raw = b"".join(r.stream)  # type: ignore[arg-type]  # undecoded bytes as given to the mock
        return httpx.Response(r.status_code, headers=r.headers.raw, stream=_Body(raw),
                              request=request)


@dataclass
class Env:
    service: Service
    upstream: Upstream
    telemetry: NullTelemetry
    proxy: httpx.AsyncClient
    adv: httpx.AsyncClient


def make_cfg(**hold: int) -> CaplConfig:
    cfg = CaplConfig()
    cfg.proxy.integrity_sample_rate = 1.0
    cfg.proxy.upstream_uri = "http://catalog:8181"
    if hold:
        cfg.hold.max_ms = hold["max_ms"]
    return cfg


@pytest.fixture
def cfg() -> CaplConfig:
    return make_cfg(max_ms=30000)


@pytest.fixture
async def env(cfg: CaplConfig) -> AsyncIterator[Env]:
    up = Upstream()
    tel = NullTelemetry()
    tel.keep = True
    client = make_upstream_client(cfg, transport=up)
    svc = build_service(cfg, env={}, client=client, telemetry=tel, enricher=False)
    prx = httpx.AsyncClient(transport=httpx.ASGITransport(app=svc.proxy_app), base_url="http://capl:8180")
    adv = httpx.AsyncClient(transport=httpx.ASGITransport(app=svc.advisor_app), base_url="http://capl:8190")
    yield Env(svc, up, tel, prx, adv)
    await prx.aclose()
    await adv.aclose()
    await client.aclose()
