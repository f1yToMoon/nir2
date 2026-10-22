"""CAPL — Stage-0/1 proxy with hold mechanism.

Ports:
  8180  catch-all reverse proxy → CAPL_UPSTREAM_URI (catalog)
  8190  advisor stub + debug endpoints (hold, last-commits)
  9101  Prometheus metrics stub
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import re
from collections import deque
from typing import Any

import httpx
import structlog
import uvicorn
from fastapi import FastAPI, HTTPException, Request, Response
from pydantic import BaseModel

# ---------- env / defaults -----------------------------------------------
UPSTREAM_URI  = os.environ.get("CAPL_UPSTREAM_URI", "http://catalog:8181")
PROXY_PORT    = int(os.environ.get("CAPL_PROXY_PORT", "8180"))
ADV_PORT      = int(os.environ.get("CAPL_ADV_PORT", "8190"))
METRICS_PORT  = int(os.environ.get("CAPL_METRICS_PORT", "9101"))
LOG_LEVEL     = os.environ.get("CAPL_LOG_LEVEL", "INFO")
HOLD_MAX_S    = float(os.environ.get("CAPL_HOLD_MAX_MS", "30000")) / 1000.0

# ---------- logging -------------------------------------------------------
structlog.configure(
    processors=[
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer(),
    ],
    wrapper_class=structlog.make_filtering_bound_logger(
        getattr(logging, LOG_LEVEL.upper(), logging.INFO)
    ),
    logger_factory=structlog.PrintLoggerFactory(),
)
log = structlog.get_logger("capl.proxy")

# ---------- shared state --------------------------------------------------
_last_commits: deque[dict[str, Any]] = deque(maxlen=500)

# slot → asyncio.Event; when event is set, the held commit is released
_holds: dict[str, asyncio.Event] = {}

_client: httpx.AsyncClient | None = None

COMMIT_RE     = re.compile(
    r"^/v1/(?:[^/]+/)?namespaces/[^/]+/tables/[^/]+$"
)
TXN_COMMIT_RE = re.compile(r"^/v1/(?:[^/]+/)?transactions/commit$")

HOP_BY_HOP_REQ = frozenset([
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "te", "trailers", "transfer-encoding", "upgrade",
])
HOP_BY_HOP_RESP = HOP_BY_HOP_REQ | frozenset(["content-encoding"])

# ---------- proxy app -----------------------------------------------------
proxy_app = FastAPI(title="CAPL Proxy", docs_url=None)


@proxy_app.api_route(
    "/{path:path}",
    methods=["GET", "POST", "PUT", "DELETE", "HEAD", "PATCH", "OPTIONS"],
)
async def proxy(request: Request, path: str) -> Response:
    assert _client is not None, "HTTP client not initialised"

    full_path = "/" + path
    if request.url.query:
        full_path = f"{full_path}?{request.url.query}"

    headers = [
        (k, v)
        for k, v in request.headers.raw
        if k.decode().lower() not in HOP_BY_HOP_REQ
    ]

    body = await request.body()

    is_commit = request.method == "POST" and (
        COMMIT_RE.match("/" + path) or TXN_COMMIT_RE.match("/" + path)
    )
    if is_commit:
        slot   = request.headers.get("X-CAPL-Slot", "")
        writer = request.headers.get("X-CAPL-Writer", "")
        run    = request.headers.get("X-CAPL-Run", "")
        log.info("commit_attempt", path="/" + path, slot=slot, writer=writer, run=run)
        _last_commits.append(
            {"path": "/" + path, "slot": slot, "writer": writer, "run": run}
        )
        # Hold: if this slot is registered, wait until released or timeout
        if slot in _holds:
            evt = _holds[slot]
            try:
                await asyncio.wait_for(evt.wait(), timeout=HOLD_MAX_S)
            except TimeoutError:
                log.warning("hold_timeout", slot=slot, max_s=HOLD_MAX_S)

    upstream_resp = await _client.request(
        method=request.method,
        url=httpx.URL(full_path),
        headers=headers,
        content=body,
    )

    resp_headers = {
        k: v
        for k, v in upstream_resp.headers.items()
        if k.lower() not in HOP_BY_HOP_RESP
    }
    return Response(
        content=upstream_resp.content,
        status_code=upstream_resp.status_code,
        headers=resp_headers,
    )


# ---------- advisor stub + debug ------------------------------------------
advisor_app = FastAPI(title="CAPL Advisor (stage-0/1 stub)", docs_url=None)


@advisor_app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok", "stage": "1", "upstream": UPSTREAM_URI}


@advisor_app.get("/_capl/debug/last-commits")
async def debug_last_commits() -> list[dict[str, Any]]:
    return list(_last_commits)


class HoldRequest(BaseModel):
    slot: str
    max_ms: int = 30000


@advisor_app.post("/v1/debug/hold")
async def hold_start(req: HoldRequest) -> dict[str, Any]:
    """Register a hold on the named slot.
    Any commit from that slot will block at the proxy until released
    or HOLD_MAX_S seconds elapse.
    """
    if req.slot in _holds:
        # Reset: clear old event so a new hold starts fresh
        _holds[req.slot].set()
    evt = asyncio.Event()
    _holds[req.slot] = evt
    log.info("hold_registered", slot=req.slot, max_ms=req.max_ms)
    return {"status": "held", "slot": req.slot}


@advisor_app.delete("/v1/debug/hold/{slot}")
async def hold_release(slot: str) -> dict[str, Any]:
    """Release a held slot, allowing its pending commit to proceed."""
    evt = _holds.pop(slot, None)
    if evt is None:
        raise HTTPException(status_code=404, detail=f"slot '{slot}' not held")
    evt.set()
    log.info("hold_released", slot=slot)
    return {"status": "released", "slot": slot}


@advisor_app.get("/v1/debug/holds")
async def holds_list() -> dict[str, Any]:
    return {"holds": list(_holds.keys())}


# ---------- metrics stub --------------------------------------------------
metrics_app = FastAPI(title="CAPL Metrics stub", docs_url=None)
_METRICS_BODY = (
    "# HELP capl_up Service up\n"
    "# TYPE capl_up gauge\n"
    "capl_up 1\n"
    "# HELP capl_stage Stage number\n"
    "# TYPE capl_stage gauge\n"
    "capl_stage 1\n"
)


@metrics_app.get("/metrics")
async def metrics() -> Response:
    return Response(content=_METRICS_BODY, media_type="text/plain; version=0.0.4")


# ---------- multi-server runner -------------------------------------------
async def _serve(app: Any, port: int) -> None:
    config = uvicorn.Config(
        app,
        host="0.0.0.0",
        port=port,
        log_level="warning",
        access_log=False,
    )
    server = uvicorn.Server(config)
    await server.serve()


async def _run() -> None:
    global _client
    log.info("capl_start", upstream=UPSTREAM_URI, proxy_port=PROXY_PORT,
             adv_port=ADV_PORT, metrics_port=METRICS_PORT)
    _client = httpx.AsyncClient(
        base_url=UPSTREAM_URI,
        timeout=httpx.Timeout(connect=5.0, read=180.0, write=30.0, pool=5.0),
        follow_redirects=False,
        limits=httpx.Limits(max_connections=200, max_keepalive_connections=50),
    )
    try:
        await asyncio.gather(
            _serve(proxy_app, PROXY_PORT),
            _serve(advisor_app, ADV_PORT),
            _serve(metrics_app, METRICS_PORT),
        )
    finally:
        await _client.aclose()


def main() -> None:
    p = argparse.ArgumentParser(description="CAPL service")
    p.add_argument("--config", default=None, help="path to capl.yaml (unused stage 0-1)")
    p.parse_args()
    asyncio.run(_run())


if __name__ == "__main__":
    main()
