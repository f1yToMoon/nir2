"""Configuration of the CAPL service: config/capl.yaml with ${VAR} expansion from the environment."""
from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field

_VAR_RE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")


def expand_env(value: Any, env: dict[str, str] | None = None) -> Any:
    """Replace ${VAR} in every string of a nested structure; unset variables become ''."""
    env = dict(os.environ) if env is None else env
    if isinstance(value, str):
        return _VAR_RE.sub(lambda m: env.get(m.group(1), ""), value)
    if isinstance(value, dict):
        return {k: expand_env(v, env) for k, v in value.items()}
    if isinstance(value, list):
        return [expand_env(v, env) for v in value]
    return value


class _Section(BaseModel):
    model_config = ConfigDict(extra="allow")


class ServiceConfig(_Section):
    proxy_port: int = 8180
    advisor_port: int = 8190
    metrics_port: int = 9101
    log_level: str = "INFO"
    log_format: str = "json"


class HeaderNames(_Section):
    slot: str = "X-CAPL-Slot"
    writer: str = "X-CAPL-Writer"
    run: str = "X-CAPL-Run"
    idempotency_key: str = "Idempotency-Key"


class ProxyConfig(_Section):
    upstream_uri: str = "http://catalog:8181"
    connect_timeout_s: float = 5.0
    read_timeout_s: float = 180.0
    write_timeout_s: float = 30.0
    max_connections: int = 200
    max_keepalive_connections: int = 50
    commit_path_regex: str = (
        r"^/v1/(?:(?P<prefix>[^/]+)/)?namespaces/(?P<ns>[^/]+)/tables/(?P<table>[^/]+)$"
    )
    txn_commit_path_regex: str = r"^/v1/(?:(?P<prefix>[^/]+)/)?transactions/commit$"
    metrics_report_path_regex: str = (
        r"^/v1/(?:(?P<prefix>[^/]+)/)?namespaces/(?P<ns>[^/]+)/tables/(?P<table>[^/]+)/metrics$"
    )
    headers: HeaderNames = Field(default_factory=HeaderNames)
    summary_op_id_key: str = "capl.op-id"
    fail_open: bool = True
    advisor_call_timeout_ms: float = 20.0
    # full: parse, journal, hold; passthrough: forward only (no parsing, journal or hold)
    mode: Literal["full", "passthrough"] = "full"
    # share of requests whose incoming and outgoing bodies/headers are compared (SHA-256)
    integrity_sample_rate: float = 0.01


class HoldConfig(_Section):
    enabled: bool = True
    max_ms: int = 60000


class EnricherConfig(_Section):
    workers: int = 4
    # process: manifests are decoded in a process pool (no GIL contention with the proxy);
    # thread: in the event loop's default thread pool (unit tests)
    executor: Literal["process", "thread"] = "process"
    queue_max: int = 10000
    read_deleted_paths_for: list[str] = Field(
        default_factory=lambda: ["cow_dml", "mor_dml", "rewrite_data", "overwrite"])
    s3: dict[str, Any] = Field(default_factory=dict)


class PeriodicWriterConfig(_Section):
    min_commits_for_period: int = 5
    max_period_cv: float = 0.25
    partition_history: int = 50


class StateConfig(_Section):
    ring_buffer_per_table: int = 50000
    intensity_windows_s: list[float] = Field(default_factory=lambda: [60.0, 300.0, 900.0])
    intensity_weights: list[float] = Field(default_factory=lambda: [0.5, 0.3, 0.2])
    periodic_writer: PeriodicWriterConfig = Field(default_factory=PeriodicWriterConfig)
    flush_interval_s: float = 2.0


class TelemetryConfig(_Section):
    pg_dsn: str = ""
    schema_: str = Field(default="stand", alias="schema")
    batch_size: int = 200
    flush_interval_s: float = 0.5
    queue_max: int = 100000


class CaplConfig(_Section):
    service: ServiceConfig = Field(default_factory=ServiceConfig)
    proxy: ProxyConfig = Field(default_factory=ProxyConfig)
    hold: HoldConfig = Field(default_factory=HoldConfig)
    enricher: EnricherConfig = Field(default_factory=EnricherConfig)
    state: StateConfig = Field(default_factory=StateConfig)
    telemetry: TelemetryConfig = Field(default_factory=TelemetryConfig)


def load_config(path: str | Path | None, env: dict[str, str] | None = None) -> CaplConfig:
    """Load capl.yaml (or defaults when path is None) and apply environment overrides."""
    env = dict(os.environ) if env is None else env
    raw: dict[str, Any] = {}
    if path is not None:
        raw = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    cfg = CaplConfig.model_validate(expand_env(raw, env))
    # overrides kept from stages 0-1 (compose and tests set them without a config file)
    if env.get("CAPL_UPSTREAM_URI"):
        cfg.proxy.upstream_uri = env["CAPL_UPSTREAM_URI"]
    if env.get("CAPL_HOLD_MAX_MS"):
        cfg.hold.max_ms = int(env["CAPL_HOLD_MAX_MS"])
    if env.get("CAPL_PROXY_MODE"):
        cfg.proxy.mode = env["CAPL_PROXY_MODE"]  # type: ignore[assignment]
    if env.get("CAPL_INTEGRITY_SAMPLE_RATE"):
        cfg.proxy.integrity_sample_rate = float(env["CAPL_INTEGRITY_SAMPLE_RATE"])
    if env.get("CAPL_PG_DSN"):
        cfg.telemetry.pg_dsn = env["CAPL_PG_DSN"]
    for key, port in (("CAPL_PROXY_PORT", "proxy_port"), ("CAPL_ADV_PORT", "advisor_port"),
                      ("CAPL_METRICS_PORT", "metrics_port")):
        if env.get(key):
            setattr(cfg.service, port, int(env[key]))
    return cfg
