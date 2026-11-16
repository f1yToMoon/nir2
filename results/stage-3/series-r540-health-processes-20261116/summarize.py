"""Reproduce the numerical tables of the stage report from frozen exports."""
import gzip
import json
from datetime import datetime
from pathlib import Path

root = Path(__file__).resolve().parent
plan = json.loads((root / "plan.json").read_text())
verified = json.loads((root / "verification.json").read_text())
out = {}
for run in plan["runs"]:
    rid = run["run_id"]
    evidence = root / "evidence" / rid

    def load(name):
        return json.loads(gzip.decompress((evidence / f"{name}.json.gz").read_bytes()))

    result, ops = load("run-result"), load("ops")
    cpu = json.loads((root.parent / "runs" / f"{rid}.cpu.json").read_text())
    metadata = sorted(load("table_meta"), key=lambda r: r["at"])
    intervals = [(datetime.fromisoformat(b["at"]) - datetime.fromisoformat(a["at"])).total_seconds()
                 for a, b in zip(metadata, metadata[1:])]
    ctl = [o for o in ops if o["controlled"] and o["in_metrics"]]
    durations = [(datetime.fromisoformat(o["last_attempt_at"] or o["finished_at"]) -
                  datetime.fromisoformat(o["planning_snapshot_at"])).total_seconds()
                 for o in ctl if o["planning_snapshot_at"]]
    assert result["validation"]["ok"] is True and result["invariants"]["ok"] is True
    checks = result["invariants"]
    out[rid] = {
        "scenario": run["scenario"], "git_sha": result["git_sha"],
        "outcomes": result["summary"]["detail"]["outcomes"],
        "k1_with_409": sum(409 in ((o["outcome_detail"] or {}).get("http_statuses") or []) for o in ctl),
        "k2_without_http_attempt": sum(o["outcome"] == "data_conflict" and o["attempts"] == 0 for o in ctl),
        "total_cpu_s": sum(o["cpu_ms"] or 0 for o in ops) / 1000,
        "controlled_cpu_s": sum(o["cpu_ms"] or 0 for o in ops if o["controlled"]) / 1000,
        "background_cpu_s": sum(o["cpu_ms"] or 0 for o in ops if not o["controlled"]) / 1000,
        "all_spark_task_cpu_s": result["spark_app_totals"]["cpu_ms"] / 1000,
        "worker_cgroup_cpu_s": cpu["workers_cpu_ms"] / 1000,
        "live_rows": checks["ledger_row_counts"]["table_live_rows"],
        "rows_by_origin": checks["ledger_row_counts"]["rows_by_origin"],
        "versions_checked": checks["version_monotonic"]["rows_checked"],
        "rows_expected_updated": checks["version_monotonic"]["rows_expected_updated"],
        "aborted_rows_checked": checks["aborted_ops_absent"]["aborted_rows_checked"],
        "dense_peak": verified[rid]["history"]["upper"]["peak_snapshots"],
        "observed_max_snapshots": max(r["snapshots"] for r in metadata),
        "observed_max_metadata_bytes": max(r["metadata_bytes"] for r in metadata),
        "metadata_versions": verified[rid]["metadata_versions"],
        "metadata_bytes_upper_bound": verified[rid]["metadata_bytes_upper_bound"],
        "health_observations": len(metadata), "max_health_interval_s": max(intervals),
        "health_missed_intervals": verified[rid]["health_coverage"]["missed_intervals"],
        "health_invalid_timestamps": verified[rid]["health_coverage"]["invalid_timestamps"],
        "runner_errors": result["validation"]["runner_errors"],
        "incomplete_cost": result["validation"]["incomplete_cost"],
        "n_base_expired": sum(o['outcome'] == 'base_expired' for o in ops),
        "n_history_unresolved": sum(o['outcome'] == 'history_unresolved' for o in ops),
        "cooldown_overrun": result["overrun_after_cooldown"],
        "d_hist_max_s": max(durations), "freshness": result["stream_freshness"],
        "freshness_p99_s": verified[rid]["p99_from_full_csv_s"],
        "freshness_max_s": verified[rid]["max_from_full_csv_s"],
        "streams": result["streams"], "orphans": result["orphans"],
    }
(root / "summary-table.json").write_text(json.dumps(out, indent=2))
for rid, r in out.items():
    print(rid, r["outcomes"], "K1 retries", r["k1_with_409"], "dense", r["dense_peak"],
          "bytes", r["observed_max_metadata_bytes"], "D", r["d_hist_max_s"],
          "CPU", r["total_cpu_s"], r["all_spark_task_cpu_s"], r["worker_cgroup_cpu_s"],
          "rows", r["live_rows"])
