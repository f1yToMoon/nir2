"""Require all six fault TYPES and unchanged measured S5, with an explicit tiny control.

The original probe.py rejection remains preserved: the measured S5 had no successful
DELETE, so its five injected types must not be reported as six.
"""
import json
from pathlib import Path

from runner.db import DB
from runner.env import Env
from runner.ops.executor import load_table_state

root = Path(__file__).resolve().parent
rid = "stage3-S5-P0-mid-s11-r540-sessions-20261115"
output = root / "fault-coverage-with-control.json"
assert not output.exists()
found = []
for path in (root.parent / "integration").glob("*/invariants_fault_injection.json"):
    data = json.loads(path.read_text())
    if data["run_id"] == rid:
        found.append((path, data))
assert len(found) == 1, found
path, primary = found[0]
assert primary["clean"]["ok"] is True and primary["after_restore"]["ok"] is True
expected_primary = {"duplicate_row", "lost_row", "extra_update", "aborted_row", "lineage"}
assert set(primary["details"]) == expected_primary
assert all(row["ok"] is False for row in primary["details"].values())
control_path = root.parent / "retained-deleted-control-20261115/retained_deleted_fixture.json"
control = json.loads(control_path.read_text())
assert control["case"] == "retained_deleted_row"
assert control["clean"]["ok"] is True and control["after_restore"]["ok"] is True
assert control["injected"]["ok"] is False
row = control["injected"]["ledger_row_counts"]
assert row["ok"] is False and row["n_mismatches"] == 1
assert sum(x["actual"] - x["expected"] for x in row["mismatches"].values()) == 1
assert row["journal_physical_records"] == row["snapshot_total_records"] == 2
coverage = expected_primary | {control["case"]}
assert len(coverage) == 6
env = Env()
db = DB(env.pg_dsn, env.pg_schema)
try:
    baseline = {t: db.query("SELECT count(*) AS n FROM {s}." + t + " WHERE run_id=%s", [rid])[0]["n"]
                for t in ("commit_attempts", "commit_events", "ops")}
    baseline["current_snapshot_id"] = load_table_state(env, "bench.events").current_snapshot_id
    assert baseline == json.loads((root / "s5-before-injection.json").read_text())
    probe_requests = db.query("SELECT count(*) AS n FROM {s}.commit_attempts WHERE run_id=%s",
                              [primary["telemetry_run_id"]])[0]["n"]
    assert probe_requests > 0 and len(control["commit_attempts"]) >= 3
finally:
    db.close()
out = {"run_id": rid, "baseline_unchanged": True, "baseline_after": baseline,
       "primary_artifact": str(path), "primary_fault_types": sorted(expected_primary),
       "primary_probe_requests": probe_requests, "standalone_artifact": str(control_path),
       "standalone_fault_types": [control["case"]], "all_fault_types": sorted(coverage),
       "n_fault_types": len(coverage), "primary_successful_deletes": 0,
       "note": "Five types on measured S5, retained-deleted-row on a deterministic two-row control."}
output.write_text(json.dumps(out, indent=2))
print(json.dumps(out, indent=2))
