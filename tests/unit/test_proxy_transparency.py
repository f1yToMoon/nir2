"""The proxy forwards requests and returns responses unchanged (FR-PROXY-1, FR-PROXY-7)."""
from __future__ import annotations

import gzip
import hashlib
import json

import httpx
import pytest

from capl.config import CaplConfig
from tests.unit.conftest import Env

COMMIT = "/v1/warehouse/namespaces/bench/tables/events"
BODY = json.dumps({
    "identifier": {"namespace": ["bench"], "name": "events"},
    "requirements": [{"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": 1}],
    "updates": [{"action": "add-snapshot", "snapshot": {
        "snapshot-id": 2, "parent-snapshot-id": 1, "sequence-number": 2, "timestamp-ms": 1,
        "manifest-list": "s3://w/m.avro", "summary": {"operation": "append"}}}],
}, separators=(",", ":")).encode() + b"  \n"      # odd whitespace must survive byte-for-byte


def _hdrs(req: httpx.Request) -> list[tuple[str, bytes]]:
    return sorted((k.decode().lower(), v) for k, v in req.headers.raw)


@pytest.mark.parametrize("path,method", [
    (COMMIT, "POST"), ("/v1/config?warehouse=w&x=%2F", "GET"),
    ("/v1/namespaces/a%1Fb/tables/t", "GET"), ("/v1/oauth/tokens", "POST"),
    ("/v1/namespaces/bench/tables/events", "DELETE"), ("/v1/namespaces/bench", "HEAD"),
])
async def test_request_forwarded_unchanged(env: Env, path: str, method: str) -> None:
    body = BODY if method == "POST" else b""
    headers = [("X-CAPL-Slot", "s1"), ("Authorization", "Bearer t"), ("X-Dup", "1"), ("X-Dup", "2"),
               ("Idempotency-Key", "k-1"), ("Content-Type", "application/json"),
               ("Connection", "keep-alive, X-Hop"), ("X-Hop", "drop-me"), ("Keep-Alive", "timeout=5"),
               ("TE", "trailers"), ("Proxy-Authorization", "x"), ("Upgrade", "h2c")]
    r = await env.proxy.request(method, path, content=body or None, headers=headers)
    assert r.status_code == 200
    sent = env.upstream.requests[-1]
    assert sent.method == method
    assert sent.url.raw_path.decode() == path                 # path and query, encoding kept
    assert env.upstream.bodies[-1] == body
    names = {k for k, _ in _hdrs(sent)}
    assert not names & {"connection", "keep-alive", "te", "proxy-authorization", "upgrade", "x-hop"}
    assert [v for k, v in sent.headers.raw if k.lower() == b"x-dup"] == [b"1", b"2"]
    # nothing added: no httpx defaults (user-agent, accept-encoding ...)
    incoming = {"host", "accept", "accept-encoding", "connection", "user-agent", "x-capl-slot",
                "authorization", "x-dup", "idempotency-key", "content-type", "x-hop", "keep-alive",
                "te", "proxy-authorization", "upgrade", "content-length"}
    assert names <= incoming, names - incoming
    assert sent.headers["user-agent"].startswith("python-httpx")   # the *client's* UA, passed on
    assert list(env.service.proxy.integrity_failures) == []


@pytest.mark.parametrize("status", [200, 204, 400, 404, 409, 419, 500, 503])
async def test_response_returned_unchanged(env: Env, status: int) -> None:
    raw = gzip.compress(b'{"error": {"type": "CommitFailedException", "code": 409}}')
    env.upstream.handler = lambda req: httpx.Response(
        status, content=b"" if status == 204 else raw,
        headers=[("Content-Encoding", "gzip"), ("Content-Type", "application/json"),
                 ("Set-Cookie", "a=1"), ("Set-Cookie", "b=2"), ("ETag", '"v1"'),
                 ("Connection", "close"), ("X-Custom", "z")])
    for path in (COMMIT, "/v1/namespaces/bench/tables/events"):    # buffered and streamed path
        r = await env.proxy.request("POST" if path == COMMIT else "GET", path, content=BODY)
        assert r.status_code == status
        # raw (still gzip-compressed) bytes: the proxy must not decode the body
        assert r.num_bytes_downloaded == (0 if status == 204 else len(raw))
        assert r.headers.get_list("set-cookie") == ["a=1", "b=2"]
        assert r.headers["content-encoding"] == "gzip" and r.headers["etag"] == '"v1"'
        assert r.headers["x-custom"] == "z" and "connection" not in {k.lower() for k in r.headers}
        assert "server" not in r.headers
    entry = env.service.proxy.last_commits[-1]
    assert entry["status"] == status


async def test_large_streamed_response_sha(env: Env) -> None:
    big = bytes(range(256)) * 40000            # ~10 MB, arbitrary bytes
    env.upstream.handler = lambda req: httpx.Response(200, content=big)
    r = await env.proxy.get("/v1/namespaces/bench/tables/events")
    assert hashlib.sha256(r.content).digest() == hashlib.sha256(big).digest()


async def test_catalog_unavailable_gives_502(env: Env) -> None:
    env.upstream.fail = httpx.ConnectError("refused")
    r = await env.proxy.post(COMMIT, content=BODY)
    assert r.status_code == 502 and r.json()["error"]["type"] == "CaplGatewayError"
    env.upstream.fail = httpx.ReadTimeout("slow")
    r = await env.proxy.get("/v1/config")
    assert r.status_code == 504


async def test_commit_journal_row(env: Env) -> None:
    await env.proxy.post(COMMIT, content=BODY, headers={"X-CAPL-Slot": "s3", "X-CAPL-Writer": "batch_dml",
                                                        "X-CAPL-Run": "r1", "Idempotency-Key": "k"})
    [row] = env.telemetry.rows["commit_attempts"]
    assert row["table_name"] == "bench.events" and row["slot"] == "s3" and row["run_id"] == "r1"
    assert row["writer_class"] == "batch_dml" and row["idempotency_key"] == "k"
    assert row["base_snapshot_id"] == 1 and row["new_snapshot_id"] == 2 and row["op_class"] == "append"
    assert row["http_status"] == 200 and row["hold_ms"] == 0
    assert row["received_at"] <= row["forwarded_at"] <= row["responded_at"]


async def test_error_type_from_409(env: Env) -> None:
    env.upstream.handler = lambda req: httpx.Response(
        409, json={"error": {"type": "CommitFailedException", "message": "m", "code": 409}})
    await env.proxy.post(COMMIT, content=BODY)
    assert env.telemetry.rows["commit_attempts"][-1]["error_type"] == "CommitFailedException"


async def test_client_metrics_report_saved(env: Env) -> None:
    report = {"report-type": "commit-report", "table-name": "bench.events", "snapshot-id": 2,
              "metrics": {"attempts": {"unit": "count", "value": 3}}}
    raw = json.dumps(report).encode()
    r = await env.proxy.post("/v1/namespaces/bench/tables/events/metrics", content=raw)
    assert r.status_code == 200 and env.upstream.bodies[-1] == raw
    [row] = env.telemetry.rows["client_reports"]
    assert row["report_type"] == "commit-report" and row["report"] == report


async def test_unparseable_commit_still_forwarded(env: Env) -> None:
    r = await env.proxy.post(COMMIT, content=b"\x00not json")
    assert r.status_code == 200 and env.upstream.bodies[-1] == b"\x00not json"


@pytest.mark.parametrize("cfg", [CaplConfig.model_validate({"proxy": {"mode": "passthrough",
                                                                      "integrity_sample_rate": 1.0}})])
async def test_passthrough_mode_forwards_without_journal(env: Env, cfg: CaplConfig) -> None:
    await env.adv.post("/v1/debug/hold", json={"slot": "s1", "max_ms": 5000})
    r = await env.proxy.post(COMMIT, content=BODY, headers={"X-CAPL-Slot": "s1"})
    assert r.status_code == 200 and env.upstream.bodies[-1] == BODY
    assert env.telemetry.rows["commit_attempts"] == [] and len(env.service.proxy.last_commits) == 0


async def test_integrity_probe_detects_change(env: Env) -> None:
    """The check itself can fail: a transport that alters the body is reported."""
    from capl.proxy.app import IntegrityProbe

    probe = IntegrityProbe([(b"a", b"1")], False)
    probe.in_hash.update(b"x")
    probe.out_hash.update(b"y")
    probe.out_headers = [(b"a", b"1")]
    assert probe.verdict() == (False, "body sha256 differs")
    probe.out_hash = probe.in_hash
    probe.out_headers = [(b"a", b"2")]
    assert probe.verdict() == (False, "headers differ")
