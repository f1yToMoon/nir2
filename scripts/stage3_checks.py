#!/usr/bin/env python3
"""Stage 3 [auto] checks over the finished runs (tz/06, stage 3; REVIEWER.md §4).

    make stage3-checks RUNS="stage3-S0-P0-mid-s1 stage3-S0-P0-mid-s1b stage3-S1-… …"

Every check asserts and can fail. Output: results/stage-3/checks.json; exit code 1 on failure.

 1. S0 run: status finished, invariants_ok (NFR-4).
 2. Determinism: two runs with the same scenario and seed have identical intents (op key, seq,
    type, time, seed of the op, seeded footprint); state-dependent footprints are reported.
 3. Cost: Σ cpu_ms of all operations of a run ≤ Σ executorCpuTime of the Spark application and
    ≤ CPU of the worker containers (cgroup, measured on the host around the run, if present).
 4. footprint_executed ⊆ footprint_requested for every committed controlled operation (> 0 checked).
 5. Scenarios S1-S5 (P0, mid): K2 + K1 > 0 and the class matches tz/05 §2 (S1: K2 = 0; S2-S5: K2 > 0).
 6. History: base_expired and history_unresolved reported; no operation whose D_hist (planning
    snapshot → last commit attempt) exceeds R, or it is counted and excluded from the loss metric.
 7. History size of the table ≤ the directly verified point of NFR-1 (1452 snapshots, 1.51 MB,
    stage 2 r9; review S2-R4-02); B = 1800 / 1.88 MB is reported with its uncertainty.
 8. Retention R re-check with measured durations: R_low = 2·D_max + (num_retries + 1)·hold_max
    (every attempt may be held, S2-R4-04); R_high from the verified point; D_max > 349 s → author.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from runner.db import DB
from runner.env import REPO, Env

RES = REPO / "results" / "stage-3"
VERIFIED_SNAPSHOTS = 1452
VERIFIED_METADATA_BYTES = 1_510_000
B_SNAPSHOTS, B_BYTES = 1800, 1_880_000
D_MAX_AUTHOR = 349.0
HOLD_MAX_S = 60.0
EXPECTED = {"S1": "K1", "S2": "K2", "S3": "K2", "S4": "K2", "S5": "K2"}


def _load(run_id: str) -> dict[str, Any]:
    p = RES / "runs" / f"{run_id}.json"
    return json.loads(p.read_text()) if p.exists() else {}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--runs", nargs="+", required=True)
    ap.add_argument("--determinism", nargs=2, metavar=("RUN_A", "RUN_B"))
    ap.add_argument("--out", default=str(RES / "checks.json"))
    args = ap.parse_args(argv)
    env = Env()
    db = DB(env.pg_dsn, env.pg_schema)
    out: dict[str, Any] = {"runs": {}, "checks": {}}
    fails: list[str] = []

    def check(name: str, ok: bool, **detail: Any) -> None:
        out["checks"][name] = {"ok": bool(ok), **detail}
        if not ok:
            fails.append(name)

    retries = 4
    d_hist_all: list[tuple[float, str, str]] = []
    rates: dict[str, float] = {}
    for rid in args.runs:
        run = db.query("SELECT * FROM {s}.runs WHERE run_id = %s", [rid])
        if not run:
            check(f"{rid}: exists", False)
            continue
        run = run[0]
        res = _load(rid)
        summ = db.query("SELECT * FROM {s}.run_summary WHERE run_id = %s", [rid])
        summ = summ[0] if summ else {}
        detail = summ.get("detail") or {}
        ops = db.query("SELECT * FROM {s}.ops WHERE run_id = %s", [rid])
        short = run["scenario"].split("_")[0]
        sc_params = run["params"].get("scenario", {})
        retries = int(sc_params.get("table", {}).get("properties", {}).get("commit.retry.num-retries", 4))
        R = float((sc_params.get("maintenance", {}).get("expire_snapshots") or {}).get("older_than_s") or 0)
        I_ = float((sc_params.get("maintenance", {}).get("expire_snapshots") or {}).get("interval_s") or 0)
        exp_on = bool((sc_params.get("maintenance", {}).get("expire_snapshots") or {}).get("enabled"))
        info: dict[str, Any] = {"scenario": run["scenario"], "status": run["status"],
                                "invariants_ok": run["invariants_ok"], "git_sha": run["git_sha"]}
        # 1. status and invariants
        check(f"{rid}: finished, invariants_ok", run["status"] == "finished" and run["invariants_ok"] is True,
              invariants={k: v.get("ok") if isinstance(v, dict) else v
                          for k, v in (res.get("invariants") or {}).items()})
        fresh = res.get("stream_freshness") or {}
        check(f"{rid}: stream freshness fully observed", fresh.get("complete") is True
              and fresh.get("n_samples", 0) > 0, freshness=fresh)
        # 3. cost
        s_cpu = sum(int(o["cpu_ms"] or 0) for o in ops)
        app = (res.get("spark_app_totals") or {}).get("cpu_ms")
        cg = RES / "runs" / f"{rid}.cpu.json"
        cg_ms = json.loads(cg.read_text()).get("workers_cpu_ms") if cg.exists() else None
        check(f"{rid}: sum cpu_ms <= app cpu", app is not None and s_cpu <= app and s_cpu > 0,
              ops_cpu_ms=s_cpu, app_cpu_ms=app, workers_cgroup_cpu_ms=cg_ms,
              share_of_app=round(s_cpu / app, 3) if app else None)
        if cg_ms is not None:
            check(f"{rid}: app cpu <= workers cgroup cpu", app is not None and app <= cg_ms,
                  app_cpu_ms=app, workers_cgroup_cpu_ms=cg_ms)
        incomplete = [o["op_id"] for o in ops if o["outcome"] != "noop"
                      and not (o.get("cost_detail") or {}).get("complete")]
        check(f"{rid}: complete cost for every executed operation", not incomplete,
              incomplete=incomplete)
        # 4. footprints
        ctl_commit = [o for o in ops if o["controlled"] and o["outcome"] == "committed"]
        viol = [o["op_id"] for o in ctl_commit
                if not set(o["footprint_executed"] or []) <= set(o["footprint_requested"] or [])]
        nonempty = sum(1 for o in ctl_commit if o["footprint_executed"])
        check(f"{rid}: footprint_executed ⊆ requested",
              not viol and nonempty == len(ctl_commit) and nonempty > 0,
              committed=len(ctl_commit), with_executed=nonempty, violations=viol[:10])
        # 5. conflict class
        in_m = [o for o in ops if o["controlled"] and o["in_metrics"]]
        k2 = sum(1 for o in in_m if o["outcome"] == "data_conflict")
        k1_fail = sum(1 for o in in_m if o["outcome"] == "retry_exhausted")
        k1_retry = sum(1 for o in in_m if 409 in ((o["outcome_detail"] or {}).get("http_statuses") or []))
        info.update(n_ops=len(in_m), k2=k2, k1_fail=k1_fail, k1_with_409=k1_retry,
                    outcomes=detail.get("outcomes"))
        if short in EXPECTED:
            n = max(1, len(in_m))
            share = (k2 + k1_retry) / n
            ok = share > 0 and ((k2 == 0) if EXPECTED[short] == "K1" else (k2 > 0))
            check(f"{rid}: K2+K1 > 0 and class {EXPECTED[short]}", ok, k2=k2, k1_with_409=k1_retry,
                  k1_fail=k1_fail, n_ops=len(in_m), share=round(share, 3))
        # 6. history labels and durations
        be = sum(1 for o in in_m if o["outcome"] == "base_expired")
        hu = sum(1 for o in in_m if o["outcome"] == "history_unresolved")
        unk = sum(1 for o in ops if o["outcome"] in ("error", "commit_unknown"))
        over_r = []
        for o in [x for x in ops if x["controlled"]]:
            end = o["last_attempt_at"] or o["finished_at"]
            if o["planning_snapshot_at"] and end:
                d = (end - o["planning_snapshot_at"]).total_seconds()
                d_hist_all.append((d, rid, o["op_id"]))
                if exp_on and R and d > R:
                    over_r.append({"op_id": o["op_id"], "d_hist_s": round(d, 1), "outcome": o["outcome"]})
        check(f"{rid}: no operation older than R (or counted)", not over_r or all(
            x["outcome"] not in ("data_conflict", "retry_exhausted", "history_unresolved") for x in over_r),
            R_s=R, over_r=over_r[:10], n_over_r=len(over_r), base_expired=be, history_unresolved=hu,
            errors=unk)
        errs = [(o["op_id"], o["error_class"]) for o in ops if o["outcome"] in ("error", "commit_unknown")]
        check(f"{rid}: no runner/infrastructure errors", not errs, errors=errs[:10])
        bgx = detail.get("background_expire") or {}
        info["background_expire"] = bgx
        # 7. history size
        ms, mb = detail.get("max_snapshots"), detail.get("max_metadata_bytes")
        check(f"{rid}: history <= verified point (1452 / 1.51 MB)",
              ms is not None and ms <= VERIFIED_SNAPSHOTS and mb <= VERIFIED_METADATA_BYTES,
              max_snapshots=ms, max_metadata_bytes=mb, B=(B_SNAPSHOTS, B_BYTES))
        # commit rate of the table (all successful commits / run time)
        rr = db.query("SELECT count(*) AS n, extract(epoch FROM max(responded_at) - min(responded_at)) AS s"
                      " FROM {s}.commit_attempts WHERE run_id = %s AND http_status BETWEEN 200 AND 299"
                      " AND (removed_snapshot_ids IS NULL)", [rid])[0]
        if rr["s"]:
            rates[short] = float(rr["n"]) / float(rr["s"])
        info["commit_rate_per_s"] = rates.get(short)
        info["R_s"], info["I_s"] = R, I_
        out["runs"][rid] = info
    # 2. determinism
    if args.determinism:
        a, b = (json.loads((REPO / "data" / "runs" / r / "intents.json").read_text())
                for r in args.determinism)
        def fp(x: list[dict[str, Any]]) -> list[Any]:
            return [(i["op_key"], i["seq"], i["op_type"], i["t_offset_s"], i["op_seed"],
                     json.dumps(i["footprint"], sort_keys=True)) for i in x]
        same = fp(a) == fp(b)
        ra = {"-".join(o["op_id"].rsplit("-", 2)[-2:]): set(o["footprint_requested"] or [])
              for o in db.query("SELECT op_id, footprint_requested FROM {s}.ops "
                                "WHERE run_id = %s AND controlled",
                                [args.determinism[0]])}
        rb = {"-".join(o["op_id"].rsplit("-", 2)[-2:]): set(o["footprint_requested"] or [])
              for o in db.query("SELECT op_id, footprint_requested FROM {s}.ops "
                                "WHERE run_id = %s AND controlled",
                                [args.determinism[1]])}
        jac = [len(ra[k] & rb[k]) / max(1, len(ra[k] | rb[k])) for k in ra if k in rb]
        check("determinism: identical intents", same and len(a) > 0, n_a=len(a), n_b=len(b),
              resolved_footprint_jaccard_mean=round(sum(jac) / len(jac), 3) if jac else None,
              resolved_identical=sum(1 for x in jac if x == 1.0), resolved_n=len(jac))
    # 8. retention re-check
    d_hist_all.sort(reverse=True)
    if d_hist_all:
        d_max, rid_max, op_max = d_hist_all[0]
        rate = rates.get("S1")
        r_low = 2 * d_max + (retries + 1) * HOLD_MAX_S
        r_low_single = 2 * d_max + HOLD_MAX_S
        I_s = 60.0
        r_high_verified = VERIFIED_SNAPSHOTS / rate - I_s if rate else None
        r_high_b = B_SNAPSHOTS / rate - I_s if rate else None
        out["retention"] = {
            "d_max_s": round(d_max, 1), "d_max_op": op_max, "d_max_run": rid_max,
            "top5": [(round(d, 1), o) for d, _, o in d_hist_all[:5]],
            "num_retries": retries, "R_low_s (2·D_max + (n+1)·hold)": round(r_low, 1),
            "R_low_single_hold_s": round(r_low_single, 1), "S1_commit_rate_per_s": rate,
            "R_high_verified_point_s": round(r_high_verified, 1) if r_high_verified else None,
            "R_high_B_s": round(r_high_b, 1) if r_high_b else None,
            "author_stop (D_max > 349 s)": d_max > D_MAX_AUTHOR,
        }
        configured_r = min((r["R_s"] for r in out["runs"].values() if r["R_s"]), default=0)
        check("retention: measured lower bound <= configured R", r_low <= configured_r,
              lower_s=round(r_low, 1), configured_R_s=configured_r)
        check("retention: D_max <= 349 s (else author decision)", d_max <= D_MAX_AUTHOR,
              d_max_s=round(d_max, 1))
    out["failed"] = fails
    Path(args.out).write_text(json.dumps(out, indent=1, default=str, ensure_ascii=False))
    for k, v in out["checks"].items():
        print(("PASS " if v["ok"] else "FAIL ") + k)
    if "retention" in out:
        print(json.dumps(out["retention"], ensure_ascii=False, indent=1))
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
