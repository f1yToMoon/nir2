"""Prometheus metrics of the CAPL service (names match infra/grafana/dashboards/capl.json)."""
from __future__ import annotations

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram

REGISTRY = CollectorRegistry(auto_describe=True)

_LAT_BUCKETS = (0.0005, 0.001, 0.002, 0.005, 0.01, 0.015, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60)

COMMIT_ATTEMPTS = Counter(
    "capl_commit_attempts_total", "Commit requests seen by the proxy",
    ["status", "op_class"], registry=REGISTRY)
COMMIT_UPSTREAM = Histogram(
    "capl_commit_upstream_seconds", "Catalog time of a commit request (forward → response)",
    ["op_class"], buckets=_LAT_BUCKETS, registry=REGISTRY)
PROXY_OVERHEAD = Histogram(
    "capl_proxy_overhead_seconds",
    "Time spent in the proxy for a commit, excluding hold and catalog time",
    buckets=_LAT_BUCKETS, registry=REGISTRY)
REQUESTS = Counter(
    "capl_proxy_requests_total", "All proxied requests", ["method", "status"], registry=REGISTRY)
UPSTREAM_ERRORS = Counter(
    "capl_proxy_upstream_errors_total", "Catalog unavailable (502/504 returned)", ["kind"],
    registry=REGISTRY)
HOLDS = Counter(
    "capl_holds_total", "Commit holds by outcome (released | deadline | ...)", ["outcome"],
    registry=REGISTRY)
HOLD_SECONDS = Histogram(
    "capl_hold_seconds", "Hold duration", buckets=_LAT_BUCKETS, registry=REGISTRY)
FAIL_OPEN = Counter(
    "capl_fail_open_total", "Advisor/internal errors bypassed by fail-open", ["stage", "reason"],
    registry=REGISTRY)
INTEGRITY = Counter(
    "capl_body_integrity_total", "Sampled comparisons of incoming and forwarded requests",
    ["result"], registry=REGISTRY)
PARSE_ERRORS = Counter(
    "capl_commit_parse_errors_total", "Commit bodies that could not be parsed", registry=REGISTRY)
CLIENT_REPORTS = Counter(
    "capl_client_reports_total", "Client metrics reports", ["report_type"], registry=REGISTRY)

ENRICH_QUEUE = Gauge("capl_enrich_queue_size", "Enricher queue length", registry=REGISTRY)
ENRICH_LAG = Histogram(
    "capl_enrich_lag_seconds", "Commit response → commit_event published", buckets=_LAT_BUCKETS,
    registry=REGISTRY)
ENRICH_TOTAL = Counter(
    "capl_enrich_total", "Enrichment jobs by result", ["result"], registry=REGISTRY)

TELEMETRY_ROWS = Counter(
    "capl_telemetry_rows_total", "Rows written to Postgres", ["table"], registry=REGISTRY)
TELEMETRY_DROPPED = Counter(
    "capl_telemetry_dropped_total", "Telemetry rows dropped", ["table", "reason"], registry=REGISTRY)
TELEMETRY_QUEUE = Gauge("capl_telemetry_queue_size", "Telemetry queue length", registry=REGISTRY)

UP = Gauge("capl_up", "Service up", registry=REGISTRY)
STAGE = Gauge("capl_stage", "Implementation stage", registry=REGISTRY)
UP.set(1)
STAGE.set(2)
