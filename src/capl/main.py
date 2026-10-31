"""CAPL service: three ASGI apps in one process (tz/02 §4).

Ports:
  8180  catch-all reverse proxy → proxy.upstream_uri (catalog)
  8190  advisor (stage 2: debug hold registry, state, fault flag)
  9101  Prometheus metrics
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import multiprocessing
import os
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from typing import Any

import structlog
import uvicorn
from fastapi import FastAPI
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from starlette.responses import Response

from capl import metrics
from capl.advisor.app import DebugHoldAdvisor, build_advisor_app
from capl.config import CaplConfig, load_config
from capl.enricher import Enricher, S3FootprintReader
from capl.proxy.app import ProxyService, build_proxy_app, make_upstream_client
from capl.proxy.hold import HoldGates
from capl.state import StateStore
from capl.telemetry import NullTelemetry, PgTelemetry, TelemetrySink

log = structlog.get_logger("capl.main")


def configure_logging(level: str) -> None:
    structlog.configure(
        processors=[
            structlog.stdlib.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, level.upper(), logging.INFO)),
        logger_factory=structlog.PrintLoggerFactory(),
    )


def build_metrics_app() -> FastAPI:
    app = FastAPI(title="CAPL metrics", docs_url=None)

    @app.get("/metrics")
    async def prom() -> Response:
        return Response(generate_latest(metrics.REGISTRY), media_type=CONTENT_TYPE_LATEST)

    return app


def s3_properties(cfg: CaplConfig, env: dict[str, str]) -> dict[str, str]:
    s3 = cfg.enricher.s3
    props = {
        "s3.endpoint": str(s3.get("endpoint") or env.get("CAPL_S3_ENDPOINT", "")),
        "s3.region": str(s3.get("region") or env.get("AWS_REGION", "us-east-1")),
        "s3.access-key-id": env.get("AWS_ACCESS_KEY_ID", ""),
        "s3.secret-access-key": env.get("AWS_SECRET_ACCESS_KEY", ""),
    }
    if s3.get("path_style", True):
        props["s3.force-virtual-addressing"] = "false"
    return {k: v for k, v in props.items() if v}


@dataclass
class Service:
    cfg: CaplConfig
    proxy: ProxyService
    advisor: DebugHoldAdvisor
    telemetry: TelemetrySink
    enricher: Enricher | None
    proxy_app: Any
    advisor_app: Any
    metrics_app: Any

    async def start(self) -> None:
        await self.telemetry.start()
        if self.enricher is not None:
            await self.enricher.start()

    async def stop(self) -> None:
        if self.enricher is not None:
            await self.enricher.stop()
        await self.telemetry.stop()
        await self.proxy.client.aclose()


def build_service(cfg: CaplConfig, env: dict[str, str] | None = None, client: Any = None,
                  telemetry: TelemetrySink | None = None, enricher: bool = True) -> Service:
    env = dict(os.environ) if env is None else env
    if telemetry is None:
        telemetry = (PgTelemetry(cfg.telemetry.pg_dsn, cfg.telemetry.schema_, cfg.telemetry.batch_size,
                                 cfg.telemetry.flush_interval_s, cfg.telemetry.queue_max)
                     if cfg.telemetry.pg_dsn else NullTelemetry())
    state = StateStore(cfg.state)
    gates = HoldGates(cfg.hold.max_ms)
    advisor = DebugHoldAdvisor(gates)
    enr = None
    if enricher:
        props = s3_properties(cfg, env)
        executor = None
        if cfg.enricher.executor == "process":
            executor = ProcessPoolExecutor(max_workers=cfg.enricher.workers,
                                           mp_context=multiprocessing.get_context("spawn"))
        enr = Enricher(S3FootprintReader(tuple(sorted(props.items()))), telemetry, state,
                       cfg.enricher.workers, cfg.enricher.queue_max,
                       cfg.enricher.read_deleted_paths_for, executor)
    svc = ProxyService(cfg, client or make_upstream_client(cfg), gates, advisor, telemetry, enr, state)
    flush = getattr(telemetry, "flush", None)
    return Service(cfg, svc, advisor, telemetry, enr, build_proxy_app(svc),
                   build_advisor_app(svc, advisor, flush), build_metrics_app())


async def _serve(app: Any, port: int) -> None:
    config = uvicorn.Config(
        app, host="0.0.0.0", port=port, log_level="warning", access_log=False,
        # the proxy must not add headers to catalog responses
        server_header=False, date_header=False,
    )
    await uvicorn.Server(config).serve()


async def _run(cfg: CaplConfig) -> None:
    service = build_service(cfg)
    log.info("capl_start", upstream=cfg.proxy.upstream_uri, mode=cfg.proxy.mode,
             hold_max_ms=cfg.hold.max_ms, telemetry=bool(cfg.telemetry.pg_dsn),
             ports=[cfg.service.proxy_port, cfg.service.advisor_port, cfg.service.metrics_port])
    await service.start()
    try:
        await asyncio.gather(
            _serve(service.proxy_app, cfg.service.proxy_port),
            _serve(service.advisor_app, cfg.service.advisor_port),
            _serve(service.metrics_app, cfg.service.metrics_port),
        )
    finally:
        await service.stop()


def main() -> None:
    p = argparse.ArgumentParser(description="CAPL service")
    p.add_argument("--config", default=os.environ.get("CAPL_CONFIG"), help="path to capl.yaml")
    args = p.parse_args()
    cfg = load_config(args.config)
    configure_logging(cfg.service.log_level)
    asyncio.run(_run(cfg))


if __name__ == "__main__":
    main()
