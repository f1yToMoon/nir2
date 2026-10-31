"""Advisor component, stage 2: no policy yet.

Hold decisions come from gates armed by the test endpoint POST /v1/debug/hold (tz/06 step 2.6).
A fault flag emulates an unavailable / slow / failing advisor to check fail-open (step 2.7).
"""
from __future__ import annotations

import asyncio
import time
from typing import Any, Literal

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from capl.proxy.app import CommitContext, HoldAdvisor, HoldDecision, ProxyService
from capl.proxy.hold import HoldGates

Fault = Literal["none", "down", "error", "slow"]


class AdvisorUnavailable(RuntimeError):
    pass


class DebugHoldAdvisor(HoldAdvisor):
    def __init__(self, gates: HoldGates) -> None:
        self.gates = gates
        self.fault: Fault = "none"
        self.calls = 0

    async def hold_decision(self, ctx: CommitContext) -> HoldDecision:
        self.calls += 1
        if self.fault == "down":
            raise AdvisorUnavailable("advisor component stopped (debug flag)")
        if self.fault == "error":
            raise RuntimeError("advisor internal error (debug flag)")
        if self.fault == "slow":
            await asyncio.sleep(10)        # far above advisor_call_timeout_ms
        if self.gates.is_armed(ctx.slot):
            return HoldDecision(hold=True, max_ms=self.gates.max_ms_limit, reason="debug")
        return HoldDecision(hold=False)


class HoldRequest(BaseModel):
    slot: str
    max_ms: int = 30000
    release_on_commit_of: str | None = None


class FaultRequest(BaseModel):
    fault: Fault


class ModeRequest(BaseModel):
    mode: Literal["full", "passthrough"]


class SlotRequest(BaseModel):
    op_id: str


def build_advisor_app(svc: ProxyService, advisor: DebugHoldAdvisor, flush: Any = None) -> FastAPI:
    app = FastAPI(title="CAPL Advisor (stage-2 stub)", docs_url=None)
    gates = svc.gates

    @app.get("/healthz")
    async def healthz() -> dict[str, Any]:
        return {"status": "ok", "stage": "2", "upstream": svc.cfg.proxy.upstream_uri,
                "mode": svc.mode, "advisor_fault": advisor.fault}

    @app.get("/v1/state")
    async def state() -> dict[str, Any]:
        if advisor.fault == "down":
            raise HTTPException(status_code=503, detail="advisor down")
        snap = svc.state.snapshot(time.time())
        snap["holds"] = gates.armed()
        snap["enricher_queue"] = svc.enricher.qsize() if svc.enricher else None
        return snap

    @app.get("/_capl/debug/last-commits")
    async def last_commits() -> list[dict[str, Any]]:
        return list(svc.last_commits)

    @app.delete("/_capl/debug/last-commits")
    async def clear_commits() -> dict[str, Any]:
        n = len(svc.last_commits)
        svc.last_commits.clear()
        return {"cleared": n}

    @app.get("/_capl/debug/integrity")
    async def integrity() -> dict[str, Any]:
        return {"sample_rate": svc.integrity_sample_rate, "failures": list(svc.integrity_failures)}

    @app.put("/_capl/debug/integrity")
    async def set_integrity(rate: float) -> dict[str, Any]:
        if not 0.0 <= rate <= 1.0:
            raise HTTPException(status_code=422, detail="rate must be in [0, 1]")
        svc.integrity_sample_rate = rate
        return {"sample_rate": rate}

    @app.put("/_capl/debug/keep-bodies")
    async def keep_bodies(on: bool) -> dict[str, Any]:
        """Keep a read-only copy of each commit body in last-commits (fixture capture)."""
        svc.keep_bodies = on
        return {"keep_bodies": on}

    @app.post("/v1/debug/hold")
    async def hold_start(req: HoldRequest) -> dict[str, Any]:
        """Arm a hold on the slot: its next commit waits until released, until a successful
        commit of `release_on_commit_of`, or min(max_ms, hold.max_ms)."""
        if req.max_ms <= 0:
            raise HTTPException(status_code=422, detail="max_ms must be positive")
        eff = gates.register(req.slot, req.max_ms, "debug", req.release_on_commit_of)
        return {"status": "held", "slot": req.slot, "max_ms": eff}

    @app.delete("/v1/debug/hold/{slot}")
    async def hold_release(slot: str) -> dict[str, Any]:
        if not gates.release(slot):
            raise HTTPException(status_code=404, detail=f"slot '{slot}' not held")
        return {"status": "released", "slot": slot}

    @app.get("/v1/debug/holds")
    async def holds() -> dict[str, Any]:
        return {"holds": list(gates.armed())}

    @app.post("/v1/debug/run-stop")
    async def run_stop() -> dict[str, Any]:
        return {"released": gates.release_all("run_stop")}

    @app.put("/v1/debug/advisor")
    async def set_fault(req: FaultRequest) -> dict[str, Any]:
        advisor.fault = req.fault
        return {"fault": advisor.fault}

    @app.put("/v1/debug/mode")
    async def set_mode(req: ModeRequest) -> dict[str, Any]:
        svc.mode = req.mode
        return {"mode": svc.mode}

    @app.put("/v1/debug/slots/{slot}")
    async def assign_slot(slot: str, req: SlotRequest) -> dict[str, Any]:
        svc.state.assign_slot(slot, req.op_id)
        return {"slot": slot, "op_id": req.op_id}

    @app.delete("/v1/debug/slots/{slot}")
    async def release_slot(slot: str) -> dict[str, Any]:
        svc.state.release_slot(slot)
        return {"slot": slot}

    @app.post("/v1/debug/flush")
    async def flush_all() -> dict[str, Any]:
        """Wait for the enricher queue and write queued telemetry (tests, end of run)."""
        if svc.enricher is not None:
            await svc.enricher.join()
        if flush is not None:
            await flush()
        return {"enriched": svc.enricher.published if svc.enricher else 0,
                "enrich_failed": svc.enricher.failed if svc.enricher else 0}

    return app
