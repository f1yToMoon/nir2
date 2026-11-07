"""run_summary of a run (tz/05 §3, tz/06 step 3.9).

Losses (H2, `wasted_cpu_s`): CPU of controlled operations in the metrics window with outcome
data_conflict (K2), retry_exhausted (K1 failure) or history_unresolved (an unconfirmed
«history» failure is not excluded from the main metric, REVIEWER.md P5, author decision
2026-11-04). base_expired (confirmed by data) is reported separately
(`wasted_cpu_base_expired_s`). Background maintenance (expire, writer_class maintenance) is
not a controlled operation: its CPU is `background_cpu_s`, outside `wasted_share`.

Durations for the retention check (ADR-008, S2-R4-04): D_hist = last commit attempt (or the
end of the operation) - time of the planning snapshot; it includes every attempt and hold.
"""
from __future__ import annotations

from typing import Any

import numpy as np

WASTED = ("data_conflict", "retry_exhausted", "history_unresolved")


def _pct(xs: list[float], q: float) -> float | None:
    return float(np.percentile(xs, q)) if xs else None


def compute_summary(db: Any, run_id: str, extra: dict[str, Any] | None = None) -> dict[str, Any]:
    ops = db.query("SELECT * FROM {s}.ops WHERE run_id = %s", [run_id])
    run = db.query("SELECT * FROM {s}.runs WHERE run_id = %s", [run_id])[0]
    ctl = [o for o in ops if o["controlled"] and o.get("in_metrics")]
    bg = [o for o in ops if not o["controlled"]]

    def cpu(rows: list[dict[str, Any]]) -> float:
        return sum((o["cpu_ms"] or 0) for o in rows) / 1000.0

    by_outcome: dict[str, int] = {}
    for o in ctl:
        by_outcome[o["outcome"] or "none"] = by_outcome.get(o["outcome"] or "none", 0) + 1
    wasted = [o for o in ctl if o["outcome"] in WASTED]
    bexp = [o for o in ctl if o["outcome"] == "base_expired"]
    total_cpu = cpu(ctl)
    useful = cpu([o for o in ctl if o["outcome"] == "committed"])
    dur_s = float((extra or {}).get("duration_s") or 0) or 1.0
    committed = [o for o in ctl if o["outcome"] == "committed"]
    lat = [(o["finished_at"] - o["commit_requested_at"]).total_seconds() * 1000
           for o in committed if o["finished_at"] and o["commit_requested_at"]]
    delays = [(o["started_at"] - o["intent_at"]).total_seconds() for o in ctl if o["started_at"]]
    sla = sum(1 for o in ctl if o["started_at"] and o["sla_deadline"] and o["started_at"] > o["sla_deadline"])
    # debt: share of small data files over time
    hrows = db.query("SELECT at, sum(small_files) AS s, sum(data_files) AS d FROM {s}.table_health "
                     "WHERE run_id = %s GROUP BY at ORDER BY at", [run_id])
    debt = [float(r["s"]) / float(r["d"]) for r in hrows if r["d"]]
    orph = db.query("SELECT orphan_bytes FROM {s}.orphan_scan WHERE run_id = %s ORDER BY at DESC LIMIT 1",
                    [run_id])
    meta = db.query("SELECT max(snapshots) AS snaps, max(metadata_bytes) AS mb FROM {s}.table_meta "
                    "WHERE run_id = %s", [run_id])[0]

    def dhist(o: dict[str, Any]) -> float | None:
        end = o.get("last_attempt_at") or o.get("finished_at")
        if o.get("planning_snapshot_at") is None or end is None:
            return None
        return float((end - o["planning_snapshot_at"]).total_seconds())

    def dexec(o: dict[str, Any]) -> float | None:
        if o.get("started_at") is None or o.get("finished_at") is None:
            return None
        return float((o["finished_at"] - o["started_at"]).total_seconds())

    per_type: dict[str, dict[str, Any]] = {}
    for o in [x for x in ops if x["controlled"]]:
        d = per_type.setdefault(o["op_key"] or o["op_type"], {"op_type": o["op_type"], "n": 0,
                                                              "outcomes": {}, "d_exec_s": [],
                                                              "d_hist_s": [], "cpu_s": [], "attempts": []})
        d["n"] += 1
        d["outcomes"][o["outcome"]] = d["outcomes"].get(o["outcome"], 0) + 1
        for k, f in (("d_exec_s", dexec), ("d_hist_s", dhist)):
            v = f(o)
            if v is not None:
                d[k].append(round(v, 1))
        d["cpu_s"].append(round((o["cpu_ms"] or 0) / 1000, 1))
        d["attempts"].append(o["attempts"] or 0)
    for d in per_type.values():
        for k in ("d_exec_s", "d_hist_s", "cpu_s"):
            xs = d.pop(k)
            d[k] = {"n": len(xs), "max": max(xs) if xs else None, "p50": _pct(xs, 50),
                    "p95": _pct(xs, 95)}
        at = d.pop("attempts")
        d["attempts_max"] = max(at) if at else 0
    bg_d = [v for v in (dexec(o) for o in bg) if v is not None]
    bg_late = [float((o.get("outcome_detail") or {}).get("late_s", 0)) for o in bg]
    d_hist_all = [v for v in (dhist(o) for o in ops if o["controlled"]) if v is not None]
    row = {
        "run_id": run_id, "n_ops": len(ctl),
        "n_k2": by_outcome.get("data_conflict", 0), "n_k1_fail": by_outcome.get("retry_exhausted", 0),
        "n_base_expired": len(bexp), "n_history_unresolved": by_outcome.get("history_unresolved", 0),
        "n_errors": by_outcome.get("error", 0) + by_outcome.get("commit_unknown", 0),
        "wasted_cpu_s": cpu(wasted), "useful_cpu_s": useful,
        "wasted_cpu_base_expired_s": cpu(bexp),
        "wasted_share": cpu(wasted) / total_cpu if total_cpu else None,
        "background_cpu_s": cpu(bg),
        "wasted_bytes": int(sum((o["bytes_written"] or 0) for o in wasted)),
        "goodput_commits_per_min": len(committed) / (dur_s / 60.0),
        "commit_lat_p50_ms": _pct(lat, 50), "commit_lat_p95_ms": _pct(lat, 95),
        "commit_lat_p99_ms": _pct(lat, 99), "delay_p95_s": _pct(delays, 95), "sla_violations": sla,
        "debt_mean": float(np.mean(debt)) if debt else None, "debt_max": max(debt) if debt else None,
        "orphan_bytes": int(orph[0]["orphan_bytes"]) if orph else None,
        "stream_freshness_p95_s": ((extra or {}).get("stream_freshness", {}).get("p95_s")
                                   if (extra or {}).get("stream_freshness", {}).get("complete")
                                   else None),
        "detail": {"stream_freshness": (extra or {}).get("stream_freshness"),
                   "outcomes": by_outcome, "per_op": per_type,
                   "background_expire": {"n": len(bg), "d_exec_max_s": max(bg_d) if bg_d else None,
                                         "d_exec_p50_s": _pct(bg_d, 50),
                                         "late_max_s": max(bg_late) if bg_late else None,
                                         "outcomes": _count(o["outcome"] for o in bg),
                                         "cpu_s": cpu(bg)},
                   "d_hist_max_s": max(d_hist_all) if d_hist_all else None,
                   "max_snapshots": meta["snaps"], "max_metadata_bytes": meta["mb"],
                   "policy": run["policy"], "scenario": run["scenario"]},
    }
    db.execute("DELETE FROM {s}.run_summary WHERE run_id = %s", [run_id])
    cols = [*list(row), "computed_at"]
    import json
    vals = [json.dumps(v, default=str) if k == "detail" else v for k, v in row.items()]
    db.execute(f"INSERT INTO {{s}}.run_summary ({', '.join(cols)}) VALUES "
               f"({', '.join(['%s'] * len(row))}, now())", vals)
    return row


def _count(xs: Any) -> dict[str, int]:
    out: dict[str, int] = {}
    for x in xs:
        out[str(x)] = out.get(str(x), 0) + 1
    return out
