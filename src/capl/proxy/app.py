"""Catch-all reverse proxy in front of the Iceberg REST catalog (FR-PROXY-1…9).

Guarantees (AGENTS.md, constraints 1-2):
- request method, path, query, headers (except hop-by-hop) and body bytes are forwarded unchanged;
  the catalog response (status, headers except hop-by-hop, raw body bytes) is returned unchanged;
  the proxy answers by itself only with 502/504 when the catalog is unreachable;
- the only intervention is a hold of a commit, bounded by hold.max_ms;
- any error of the advisor or of the proxy's own bookkeeping → transparent forwarding (fail-open).
"""
from __future__ import annotations

import asyncio
import contextvars
import hashlib
import json
import random
import time
from collections import deque
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from typing import Any

import httpx
import structlog
from starlette.applications import Starlette
from starlette.background import BackgroundTask
from starlette.requests import Request
from starlette.responses import Response, StreamingResponse
from starlette.routing import Route

from capl import metrics
from capl.config import CaplConfig
from capl.digest import commit_digest
from capl.enricher import Enricher, EnrichJob
from capl.proxy.commit_parser import (
    CommitParseError,
    CommitPath,
    ParsedCommit,
    PathMatcher,
    parse_table_commit,
    parse_transaction_commit,
    response_error_type,
)
from capl.proxy.hold import NO_HOLD, HoldGates, HoldResult
from capl.state import StateStore
from capl.telemetry import TelemetrySink, ts

log = structlog.get_logger("capl.proxy")

# RFC 9110 §7.6.1 hop-by-hop headers (+ "trailer" spelling of tz/04 §1.1)
HOP_BY_HOP = frozenset([
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization", "proxy-connection",
    "te", "trailer", "trailers", "transfer-encoding", "upgrade",
])
METHODS = ["GET", "POST", "PUT", "DELETE", "HEAD", "PATCH", "OPTIONS"]


def _hop_by_hop(headers: list[tuple[bytes, bytes]]) -> frozenset[str]:
    """Static hop-by-hop names plus those listed in the Connection header."""
    extra = {tok.strip().lower() for k, v in headers if k.lower() == b"connection"
             for tok in v.decode("latin-1").split(",") if tok.strip()}
    return HOP_BY_HOP | frozenset(extra)


def raw_target(request: Request) -> str:
    """Path and query exactly as received (percent-encoding kept, no '?' added)."""
    raw_path = request.scope.get("raw_path") or request.url.path.encode("latin-1")
    path = raw_path.split(b"?", 1)[0]
    qs = request.scope.get("query_string", b"")
    return (path + (b"?" + qs if qs else b"")).decode("latin-1")


def end_to_end(headers: list[tuple[bytes, bytes]]) -> list[tuple[bytes, bytes]]:
    drop = _hop_by_hop(headers)
    return [(k, v) for k, v in headers if k.decode("latin-1").lower() not in drop]


# ---------------------------------------------------------------- integrity probe

@dataclass
class IntegrityProbe:
    """Incoming request as received vs. request as handed to the HTTP transport."""
    in_headers: list[tuple[bytes, bytes]]
    in_chunked: bool
    in_hash: Any = field(default_factory=hashlib.sha256)
    out_hash: Any = field(default_factory=hashlib.sha256)
    out_headers: list[tuple[bytes, bytes]] | None = None

    def verdict(self) -> tuple[bool, str]:
        if self.out_headers is None:
            return False, "not sent"
        if self.in_hash.digest() != self.out_hash.digest():
            return False, "body sha256 differs"

        def norm(hs: list[tuple[bytes, bytes]], drop_len: bool) -> list[tuple[str, bytes]]:
            out = sorted((k.decode("latin-1").lower(), v) for k, v in hs)
            return [h for h in out if not (drop_len and h[0] == "content-length")]

        # a chunked incoming body is re-framed by the transport (hop-by-hop framing)
        if norm(self.in_headers, self.in_chunked) != norm(self.out_headers, self.in_chunked):
            return False, "headers differ"
        return True, ""


_PROBE: contextvars.ContextVar[IntegrityProbe | None] = contextvars.ContextVar("capl_probe", default=None)


class IntegrityTransport(httpx.AsyncBaseTransport):
    """Records the bytes and headers actually handed to the network transport."""

    def __init__(self, inner: httpx.AsyncBaseTransport) -> None:
        self._inner = inner

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        found = _PROBE.get()
        if found is not None:
            probe = found
            probe.out_headers = list(request.headers.raw)
            original = request.stream

            class _Hashing(httpx.AsyncByteStream):
                async def __aiter__(self) -> AsyncIterator[bytes]:
                    async for chunk in original:  # type: ignore[union-attr]
                        probe.out_hash.update(chunk)
                        yield chunk

                async def aclose(self) -> None:
                    if isinstance(original, httpx.AsyncByteStream):
                        await original.aclose()

            request.stream = _Hashing()
        return await self._inner.handle_async_request(request)

    async def aclose(self) -> None:
        await self._inner.aclose()


def make_upstream_client(cfg: CaplConfig, transport: httpx.AsyncBaseTransport | None = None
                         ) -> httpx.AsyncClient:
    p = cfg.proxy
    inner = transport or httpx.AsyncHTTPTransport(
        limits=httpx.Limits(max_connections=p.max_connections,
                            max_keepalive_connections=p.max_keepalive_connections))
    client = httpx.AsyncClient(
        base_url=p.upstream_uri,
        timeout=httpx.Timeout(connect=p.connect_timeout_s, read=p.read_timeout_s,
                              write=p.write_timeout_s, pool=p.connect_timeout_s),
        follow_redirects=False,
        transport=IntegrityTransport(inner),
    )
    # httpx adds Accept, Accept-Encoding, Connection, User-Agent by default: a transparent proxy
    # must not add headers the client did not send
    for h in list(client.headers.keys()):
        del client.headers[h]
    return client


# ---------------------------------------------------------------- advisor interface

@dataclass(frozen=True)
class HoldDecision:
    hold: bool
    max_ms: int = 0
    reason: str = ""


@dataclass(frozen=True)
class CommitContext:
    slot: str
    writer: str
    run_id: str
    op_id: str | None
    commit: ParsedCommit | None


class HoldAdvisor:
    """Interface the proxy calls before forwarding a table commit (tz/02 §3.1)."""

    async def hold_decision(self, ctx: CommitContext) -> HoldDecision:  # pragma: no cover
        raise NotImplementedError


# ---------------------------------------------------------------- service

class ProxyService:
    def __init__(self, cfg: CaplConfig, client: httpx.AsyncClient, gates: HoldGates,
                 advisor: HoldAdvisor, telemetry: TelemetrySink, enricher: Enricher | None,
                 state: StateStore, rng: random.Random | None = None) -> None:
        self.cfg = cfg
        self.client = client
        self.gates = gates
        self.advisor = advisor
        self.telemetry = telemetry
        self.enricher = enricher
        self.state = state
        self.mode = cfg.proxy.mode
        self.integrity_sample_rate = cfg.proxy.integrity_sample_rate
        self.paths = PathMatcher(cfg.proxy.commit_path_regex, cfg.proxy.txn_commit_path_regex,
                                 cfg.proxy.metrics_report_path_regex)
        self.last_commits: deque[dict[str, Any]] = deque(maxlen=500)
        self.integrity_failures: deque[dict[str, Any]] = deque(maxlen=100)
        self._rng = rng or random.Random()
        hn = cfg.proxy.headers
        self._h_slot, self._h_writer, self._h_run = hn.slot.lower(), hn.writer.lower(), hn.run.lower()
        self._h_idem = hn.idempotency_key.lower()

    # ---- forwarding -------------------------------------------------------
    def _sampled(self) -> bool:
        return self.integrity_sample_rate > 0 and self._rng.random() < self.integrity_sample_rate

    async def _send(self, request: Request, headers: list[tuple[bytes, bytes]],
                    content: bytes | AsyncIterator[bytes] | None, stream: bool,
                    probe: IntegrityProbe | None) -> httpx.Response | Response:
        url = httpx.URL(raw_target(request))
        req = self.client.build_request(request.method, url, headers=headers, content=content)
        token = _PROBE.set(probe)
        try:
            resp = await self.client.send(req, stream=stream)
        except httpx.TimeoutException as exc:
            metrics.UPSTREAM_ERRORS.labels(kind="timeout").inc()
            log.warning("upstream_timeout", path=request.url.path, error=repr(exc))
            return _gateway_error(504, "catalog timeout")
        except httpx.TransportError as exc:
            metrics.UPSTREAM_ERRORS.labels(kind="unavailable").inc()
            log.warning("upstream_unavailable", path=request.url.path, error=repr(exc))
            return _gateway_error(502, "catalog unavailable")
        finally:
            _PROBE.reset(token)
        if probe is not None:
            ok, why = probe.verdict()
            metrics.INTEGRITY.labels(result="ok" if ok else "mismatch").inc()
            if not ok:
                self.integrity_failures.append({"path": request.url.path, "why": why})
                log.error("integrity_mismatch", path=request.url.path, why=why)
        return resp

    async def handle(self, request: Request) -> Response:
        path = request.url.path
        cpath = self.paths.match(request.method, path) if self.mode == "full" else None
        if cpath is None:
            return await self._forward_stream(request)
        if cpath.kind == "metrics":
            return await self._forward_metrics_report(request, cpath)
        return await self._forward_commit(request, cpath)

    async def _forward_stream(self, request: Request) -> Response:
        raw = list(request.headers.raw)
        headers = end_to_end(raw)
        chunked = any(k.lower() == b"transfer-encoding" for k, _ in raw)
        has_body = chunked or any(k.lower() == b"content-length" for k, _ in raw)
        probe = IntegrityProbe(headers, chunked) if self._sampled() else None

        async def body() -> AsyncIterator[bytes]:
            async for chunk in request.stream():
                if chunk:
                    if probe is not None:
                        probe.in_hash.update(chunk)
                    yield chunk

        resp = await self._send(request, headers, body() if has_body else None, True, probe)
        if isinstance(resp, Response):
            return resp
        metrics.REQUESTS.labels(method=request.method, status=str(resp.status_code)).inc()
        return _streaming_response(resp)

    async def _forward_buffered(self, request: Request, body: bytes,
                                headers: list[tuple[bytes, bytes]]
                                ) -> tuple[httpx.Response | Response, bytes]:
        probe = None
        if self._sampled():
            probe = IntegrityProbe(headers, any(k.lower() == b"transfer-encoding"
                                                for k, _ in request.headers.raw))
            probe.in_hash.update(body)
        has_body = bool(body) or any(k.lower() in (b"content-length", b"transfer-encoding")
                                     for k, _ in request.headers.raw)
        resp = await self._send(request, headers, body if has_body else None, True, probe)
        if isinstance(resp, Response):
            return resp, b""
        try:
            raw_body = b"".join([c async for c in resp.aiter_raw()])
        finally:
            await resp.aclose()
        metrics.REQUESTS.labels(method=request.method, status=str(resp.status_code)).inc()
        return resp, raw_body

    async def _forward_metrics_report(self, request: Request, cpath: CommitPath) -> Response:
        body = await request.body()
        resp, raw_body = await self._forward_buffered(request, body, end_to_end(list(request.headers.raw)))
        if isinstance(resp, Response):
            return resp
        try:
            report = json.loads(body)
            rtype = str(report.get("report-type", "unknown")) if isinstance(report, dict) else "unknown"
            self.telemetry.write("client_reports", {
                "run_id": request.headers.get(self._h_run) or None, "table_name": cpath.table_name,
                "received_at": ts(time.time() * 1000), "report_type": rtype, "report": report})
            metrics.CLIENT_REPORTS.labels(report_type=rtype).inc()
        except Exception as exc:            # fail-open: bookkeeping never affects the response
            metrics.FAIL_OPEN.labels(stage="client_report", reason=type(exc).__name__).inc()
        return _buffered_response(resp, raw_body)

    # ---- commits ----------------------------------------------------------
    def _parse(self, body: bytes, cpath: CommitPath) -> list[ParsedCommit]:
        try:
            if cpath.kind == "transaction":
                return parse_transaction_commit(body)
            return [parse_table_commit(body, cpath)]
        except CommitParseError as exc:
            metrics.PARSE_ERRORS.inc()
            log.warning("commit_parse_error", path=cpath, error=str(exc)[:300])
        except Exception as exc:
            metrics.FAIL_OPEN.labels(stage="parse", reason=type(exc).__name__).inc()
        return []

    async def _hold(self, ctx: CommitContext) -> HoldResult:
        if not self.cfg.hold.enabled or not ctx.slot:
            return NO_HOLD
        timeout_s = self.cfg.proxy.advisor_call_timeout_ms / 1000.0
        try:
            decision = await asyncio.wait_for(self.advisor.hold_decision(ctx), timeout=timeout_s)
        except Exception as exc:
            # fail-open (FR-PROXY-8): advisor unavailable, slow or broken → forward without a hold
            metrics.FAIL_OPEN.labels(stage="advisor", reason=type(exc).__name__).inc()
            log.warning("advisor_fail_open", slot=ctx.slot, error=repr(exc))
            return NO_HOLD
        if not decision.hold:
            return NO_HOLD
        max_ms = min(decision.max_ms or self.cfg.hold.max_ms, self.cfg.hold.max_ms)
        res = await self.gates.wait(ctx.slot, max_ms)
        if res.held:
            metrics.HOLDS.labels(outcome="deadline" if res.timed_out else "released").inc()
            metrics.HOLD_SECONDS.observe(res.hold_ms / 1000.0)
        return res

    async def _forward_commit(self, request: Request, cpath: CommitPath) -> Response:
        t_recv = time.perf_counter()
        received_ms = time.time() * 1000
        body = await request.body()
        raw = list(request.headers.raw)
        headers = end_to_end(raw)
        slot = request.headers.get(self._h_slot, "")
        writer = request.headers.get(self._h_writer, "")
        run_id = request.headers.get(self._h_run, "")
        idem = request.headers.get(self._h_idem)

        commits = self._parse(body, cpath)
        main_commit = commits[0] if cpath.kind == "table" and commits else None
        op_id = self.state.op_for_slot(slot) or (main_commit.op_id if main_commit else None)

        entry: dict[str, Any] = {
            "path": request.url.path, "kind": cpath.kind, "slot": slot, "writer": writer,
            "run": run_id, "status": None, "hold_ms": None, "held": False, "ts": round(received_ms),
            "ts_fwd": None, "ts_resp": None, "op_id": op_id,
            "op_class": main_commit.op_class if main_commit else None,
        }
        try:
            entry.update(commit_digest(body))
        except Exception as exc:
            metrics.FAIL_OPEN.labels(stage="digest", reason=type(exc).__name__).inc()
        self.last_commits.append(entry)

        hold = NO_HOLD
        if cpath.kind == "table":     # transactions: journal only, no hold (tz/04 §1.1)
            hold = await self._hold(CommitContext(slot, writer, run_id, op_id, main_commit))
        if hold.held:
            entry.update(held=True, hold_ms=hold.hold_ms, hold_max_ms=hold.max_ms,
                         hold_reason=hold.reason, released_by=hold.released_by)
            if hold.timed_out:
                entry["hold_timeout"] = True
        t_fwd = time.perf_counter()
        forwarded_ms = time.time() * 1000
        entry["ts_fwd"] = round(forwarded_ms)

        resp, raw_body = await self._forward_buffered(request, body, headers)
        t_resp = time.perf_counter()
        responded_ms = time.time() * 1000
        status = resp.status_code
        entry.update(status=status, ts_resp=round(responded_ms))
        out = resp if isinstance(resp, Response) else _buffered_response(resp, raw_body)
        try:
            self._after_commit(commits, slot, writer, run_id, op_id, idem, status, raw_body,
                               hold, received_ms, forwarded_ms, responded_ms)
            metrics.COMMIT_UPSTREAM.labels(op_class=entry["op_class"] or "unknown").observe(t_resp - t_fwd)
            metrics.PROXY_OVERHEAD.observe(max(0.0, (t_fwd - t_recv) - hold.hold_ms / 1000.0))
        except Exception as exc:
            metrics.FAIL_OPEN.labels(stage="after_commit", reason=type(exc).__name__).inc()
            log.warning("after_commit_failed", error=repr(exc))
        return out

    def _after_commit(self, commits: list[ParsedCommit], slot: str, writer: str, run_id: str,
                      op_id: str | None, idem: str | None, status: int, raw_body: bytes,
                      hold: HoldResult, received_ms: float, forwarded_ms: float,
                      responded_ms: float) -> None:
        error_type = response_error_type(raw_body) if status >= 400 else None
        if not commits:
            metrics.COMMIT_ATTEMPTS.labels(status=str(status), op_class="unparsed").inc()
        for c in commits:
            metrics.COMMIT_ATTEMPTS.labels(status=str(status), op_class=c.op_class).inc()
            self.telemetry.write("commit_attempts", {
                "run_id": run_id or None, "op_id": op_id or c.op_id, "slot": slot or None,
                "writer_class": writer or None, "table_name": c.table, "idempotency_key": idem,
                "received_at": ts(received_ms), "forwarded_at": ts(forwarded_ms),
                "responded_at": ts(responded_ms), "hold_ms": hold.hold_ms,
                "hold_reason": hold.reason if hold.held else None, "http_status": status,
                "base_snapshot_id": c.base_snapshot_id, "new_snapshot_id": c.new_snapshot_id,
                "sequence_number": c.sequence_number, "iceberg_op": c.iceberg_op,
                "op_class": c.op_class, "summary": c.summary or None, "error_type": error_type,
            })
            if status == 200 and self.enricher is not None and c.new_snapshot_id is not None \
                    and c.manifest_list:
                self.enricher.submit(EnrichJob(
                    run_id=run_id or None, table=c.table, snapshot_id=c.new_snapshot_id,
                    parent_snapshot_id=c.parent_snapshot_id, sequence_number=c.sequence_number,
                    manifest_list=c.manifest_list, iceberg_op=c.iceberg_op, op_class=c.op_class,
                    op_id=op_id or c.op_id, writer_class=writer or ("stream" if c.is_flink else None),
                    writer_id=c.flink_job_id or slot or c.summary.get("spark.app.id"),
                    committed_at_ms=responded_ms))
        if status == 200 and slot:
            self.gates.on_commit_succeeded(slot)


def _resp_headers(resp: httpx.Response) -> list[tuple[bytes, bytes]]:
    """Response headers without hop-by-hop ones, duplicates and order kept; content-encoding and
    content-length are kept because the body is passed on as raw (undecoded) bytes."""
    return end_to_end(list(resp.headers.raw))


def _buffered_response(resp: httpx.Response, raw_body: bytes) -> Response:
    out = Response(raw_body, status_code=resp.status_code)
    headers = _resp_headers(resp)
    if not any(k.lower() == b"content-length" for k, _ in headers) and resp.request.method != "HEAD":
        # upstream used chunked framing (hop-by-hop): the whole body is known, frame it by length
        headers.append((b"content-length", str(len(raw_body)).encode()))
    out.raw_headers = headers
    return out


def _streaming_response(resp: httpx.Response) -> StreamingResponse:
    out = StreamingResponse(resp.aiter_raw(), status_code=resp.status_code,
                            background=BackgroundTask(resp.aclose))
    out.raw_headers = _resp_headers(resp)
    return out


def _gateway_error(status: int, message: str) -> Response:
    """The only responses the proxy forms itself (FR-PROXY-7)."""
    body = json.dumps({"error": {"message": message, "type": "CaplGatewayError", "code": status}})
    return Response(body, status_code=status, media_type="application/json")


def build_proxy_app(svc: ProxyService) -> Starlette:
    async def endpoint(request: Request) -> Response:
        return await svc.handle(request)

    return Starlette(routes=[Route("/{path:path}", endpoint, methods=METHODS)])
