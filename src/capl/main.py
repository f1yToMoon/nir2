"""CAPL — Stage-0 minimal passthrough proxy.

Ports:
  8180  catch-all reverse proxy → CAPL_UPSTREAM_URI (catalog)
  8190  advisor stub: /healthz, /_capl/debug/last-commits
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
from fastapi import FastAPI, Request, Response

# ---------- env / defaults -----------------------------------------------
UPSTREAM_URI = os.environ.get("CAPL_UPSTREAM_URI", "http://catalog:8181")
PROXY_PORT   = int(os.environ.get("CAPL_PROXY_PORT", "8180"))
ADV_PORT     = int(os.environ.get("CAPL_ADV_PORT", "8190"))
METRICS_PORT = int(os.environ.get("CAPL_METRICS_PORT", "9101"))
LOG_LEVEL    = os.environ.get("CAPL_LOG_LEVEL", "INFO")

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
# Shared between proxy and advisor (same process, same event loop)
_last_commits: deque[dict[str, Any]] = deque(maxlen=500)

# HTTP client — initialised in main() before starting servers
_client: httpx.AsyncClient | None = None

# commit patterns from capl.yaml
COMMIT_RE     = re.compile(
    r"^/v1/(?:[^/]+/)?namespaces/[^/]+/tables/[^/]+$"
)
TXN_COMMIT_RE = re.compile(r"^/v1/(?:[^/]+/)?transactions/commit$")

# headers not forwarded upstream (request)
HOP_BY_HOP_REQ = frozenset([
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "te", "trailers", "transfer-encoding", "upgrade",
])
# headers not forwarded downstream (response):
# content-encoding removed because httpx decodes the body before we re-send it
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

    # detect commit attempts and log slot header
    is_commit = request.method == "POST" and (
        COMMIT_RE.match("/" + path) or TXN_COMMIT_RE.match("/" + path)
    )
    if is_commit:
        slot   = request.headers.get("X-CAPL-Slot", "")
        writer = request.headers.get("X-CAPL-Writer", "")
        run    = request.headers.get("X-CAPL-Run", "")
        log.info(
            "commit_attempt",
            path="/" + path,
            slot=slot,
            writer=writer,
            run=run,
        )
        _last_commits.append(
            {"path": "/" + path, "slot": slot, "writer": writer, "run": run}
        )

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


# ---------- advisor stub --------------------------------------------------
advisor_app = FastAPI(title="CAPL Advisor (stage-0 stub)", docs_url=None)


@advisor_app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok", "stage": "0", "upstream": UPSTREAM_URI}


@advisor_app.get("/_capl/debug/last-commits")
async def debug_last_commits() -> list[dict[str, Any]]:
    """Returns last logged commit attempts (for smoke-test slot-header check)."""
    return list(_last_commits)


# ---------- metrics stub --------------------------------------------------
metrics_app = FastAPI(title="CAPL Metrics stub", docs_url=None)
_METRICS_BODY = (
    "# HELP capl_up Service up\n"
    "# TYPE capl_up gauge\n"
    "capl_up 1\n"
    "# HELP capl_stage Stage number\n"
    "# TYPE capl_stage gauge\n"
    "capl_stage 0\n"
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
    p.add_argument("--config", default=None, help="path to capl.yaml (unused stage 0)")
    p.parse_args()
    asyncio.run(_run())


if __name__ == "__main__":
    main()
