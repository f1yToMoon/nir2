"""Reproduce the stage-3 S2/P0 timeline from the exported, immutable observations.

Run with the project Python environment after exporting this series to evidence/.
No risk curve is drawn: prediction and the P6 comparison belong to later stages.
"""
import csv
import gzip
import json
from datetime import datetime
from pathlib import Path

import matplotlib
import numpy as np

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D

root = Path(__file__).resolve().parent
plan = json.loads((root / "plan.json").read_text())
rid = next(r["run_id"] for r in plan["runs"] if r["scenario"].startswith("S2_"))
result = json.loads((root.parent / "runs" / f"{rid}.json").read_text())
assert result["validation"]["ok"] is True


def load(name):
    return json.loads(gzip.decompress((root / "evidence" / rid / f"{name}.json.gz").read_bytes()))


t0 = datetime.fromisoformat(result["t0"])


def seconds(value):
    return (datetime.fromisoformat(value) - t0).total_seconds()


ops = [o for o in load("ops") if o["controlled"]]
attempts = load("commit_attempts")
meta = load("table_meta")
assert ops and meta
commits = {}
for row in sorted(attempts, key=lambda x: x["responded_at"] or ""):
    if row["new_snapshot_id"] is not None and 200 <= row["http_status"] < 300:
        commits.setdefault(row["new_snapshot_id"], seconds(row["responded_at"]))
commit_times = list(commits.values())
fail_times = [seconds(o["finished_at"]) for o in ops if o["outcome"] == "data_conflict"]
end = max(seconds(result["t_end"]), *commit_times, *(seconds(o["finished_at"]) for o in ops))
assert min(commit_times) >= 0
bins = np.arange(0, np.ceil(end / 30) * 30 + 31, 30)
counts, _ = np.histogram(commit_times, bins)
failures, _ = np.histogram(fail_times, bins)
assert int(counts.sum()) == len(commits)
assert int(failures.sum()) == len(fail_times) > 0
with (root / "timeline-S2-bins.csv").open("w", newline="") as fh:
    writer = csv.writer(fh)
    writer.writerow(["start_s", "end_s", "committed_snapshots", "data_conflicts"])
    writer.writerows(zip(bins[:-1], bins[1:], counts, failures))

fig, axes = plt.subplots(3, 1, figsize=(12, 8), sharex=True)
centres = (bins[:-1] + bins[1:]) / 120
axes[0].bar(centres, counts / 30, width=0.45, color="#426a92", label="Успешные снимки")
axes[0].step(centres, failures / 30, where="mid", color="#b33632", label="Отказы K2")
axes[0].set_ylabel("Событий/с\n(окна 30 с)")
axes[0].legend(loc="upper right", frameon=False)

keys = sorted({o["op_key"] for o in ops})
colours = {"committed": "#28734f", "data_conflict": "#b33632", "noop": "#777777"}
for op in ops:
    colour = colours[op["outcome"]]  # Unrecognised/error outcomes must not silently disappear.
    y = keys.index(op["op_key"])
    start, finish = seconds(op["started_at"]) / 60, seconds(op["finished_at"]) / 60
    axes[1].plot([start, finish], [y, y], color=colour, linewidth=3)
    axes[1].scatter([seconds(op["intent_at"]) / 60], [y], marker="|", color="#333333", s=55)
    axes[1].scatter([finish], [y], marker="x" if op["outcome"] == "data_conflict" else "o",
                    color=colour, s=22)
axes[1].set_yticks(range(len(keys)), keys)
axes[1].set_ylim(-0.6, len(keys) - 0.4)
axes[1].set_ylabel("Управляемые\nоперации")
axes[1].legend(handles=[Line2D([], [], color="#28734f", label="SQL: успех"),
                       Line2D([], [], color="#b33632", label="SQL: конфликт"),
                       Line2D([], [], color="#333333", marker="|", linestyle="", label="Intent P0")],
               loc="upper right", frameon=False)

meta.sort(key=lambda x: x["at"])
mx = [seconds(m["at"]) / 60 for m in meta]
axes[2].plot(mx, [m["snapshots"] for m in meta], ".-", color="#426a92", markersize=3)
axes[2].set_ylabel("Снимков\n(наблюдения)")
right = axes[2].twinx()
right.plot(mx, [m["metadata_bytes"] / 1e6 for m in meta], color="#a75b19", alpha=0.75)
right.set_ylabel("Метаданные, МБ", color="#a75b19")
warmup = result["warmup_s"] / 60
measurement_end = warmup + result["duration_s"] / 60
for axis in axes:
    axis.axvspan(0, warmup, color="#dddddd", alpha=0.5)
    axis.axvline(measurement_end, color="#777777", linestyle=":", linewidth=1)
    axis.grid(axis="y", alpha=0.2)
axes[-1].set_xlabel("Минуты от t0; серый фон — прогрев, пунктир — конец измеряемой части")
fig.suptitle("S2, P0, mid, seed=11: фиксации, операции и история таблицы")
fig.text(0.5, 0.01, "Прогноз риска и сравнение с P6 не входят в этап 3.", ha="center", fontsize=9)
fig.tight_layout(rect=(0, 0.035, 1, 0.96))
for suffix in ("png", "pdf"):
    fig.savefig(root / f"timeline-S2.{suffix}", dpi=180)
(root / "timeline-S2-facts.json").write_text(json.dumps({
    "run_id": rid, "git_sha": result["git_sha"], "unique_committed_snapshots": len(commits),
    "controlled_operations": len(ops), "data_conflicts": len(fail_times),
    "metadata_observations": len(meta), "bin_seconds": 30, "risk_prediction_available": False,
}, indent=2))
print(rid, len(commits), "commits;", len(ops), "operations;", len(fail_times), "K2")
