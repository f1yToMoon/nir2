#!/usr/bin/env python3
"""Stage 1: fill `verified` in conflict_matrix.yaml, draw heatmaps, evaluate G1.

G1 is evaluated against the ORIGINAL hypothesis (REVIEWER.md P8), not against the
corrected matrix:
  * `fd593a5` — matrix after iteration 1 (the version named by review stage-1-r1);
  * `c6e9a4e` — matrix before any stage-1 experiment (stricter; differs for expire).
Observed outcomes are compared per valid overlap trial (plan: "≥ 95% повторов") and
per pair. Ops that did not exist in the hypothesis are mapped to its op classes
(HYP_CLASS); pairs without a hypothesis rule are listed separately.

Usage:
    python3 scripts/update_matrix.py --results results/stage-1/iter3/v2/matrix_results.json \
        --results results/stage-1/iter3/v3/matrix_results.json --out results/stage-1/iter3
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from collections import Counter
from datetime import date
from pathlib import Path
from typing import Any

import yaml

REPO = Path(__file__).parent.parent
DEFAULT_MATRIX = REPO / "config/conflict_matrix.yaml"
HYPOTHESIS_REFS = ("fd593a5", "c6e9a4e")
MIN_VALID = 10

# op used in the experiment → op class of the original hypothesis
HYP_CLASS: dict[str, str] = {
    "mor_delete": "mor_dml", "mor_update": "mor_dml", "mor_merge": "mor_dml",
    "update_status": "mor_dml",
    "overwrite_opts": "overwrite",
    "merge_upsert": "cow_dml", "merge_upsert_unscoped": "cow_dml", "delete_by_keys": "cow_dml",
    "compaction_where": "rewrite_data",
}

# G1 groups (tz/06_implementation_plan.md, G1 criterion 1): pairs in hypothesis classes
G1_GROUPS: dict[str, set[tuple[str, str]]] = {
    "компакция × CoW DML": {("rewrite_data", "cow_dml"), ("cow_dml", "rewrite_data")},
    "компакция × MoR DML": {("rewrite_data", "mor_dml"), ("mor_dml", "rewrite_data")},
    "DML × append (serializable)": {("cow_dml", "append"), ("mor_dml", "append")},
    "DML × DML": {("cow_dml", "cow_dml"), ("cow_dml", "mor_dml"), ("mor_dml", "cow_dml"),
                  ("mor_dml", "mor_dml")},
}


def load_results(paths: list[Path]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for p in paths:
        with p.open() as fh:
            out.update(json.load(fh))
    return out


def hypothesis_rules(ref: str) -> list[dict[str, Any]]:
    text = subprocess.run(["git", "-C", str(REPO), "show", f"{ref}:config/conflict_matrix.yaml"],
                          check=True, capture_output=True, text=True).stdout
    rules: list[dict[str, Any]] = yaml.safe_load(text)["rules"]
    return rules


def hyp_expected(rules: list[dict[str, Any]], r: dict[str, Any]) -> str | None:
    p = HYP_CLASS.get(r["pending"], r["pending"])
    c = HYP_CLASS.get(r["committed"], r["committed"])
    startseq = r.get("variant") != "nostartseq"
    for rule in rules:
        if rule["pending"] != p:
            continue
        rc = rule["committed"]
        if rc != "*" and c not in (rc if isinstance(rc, list) else [rc]):
            continue
        when = rule.get("when", {})
        if "isolation" in when and when["isolation"] != r["isolation"]:
            continue
        if "format_version" in when and when["format_version"] != r["format_version"]:
            continue
        if when.get("rewrite_use_starting_sequence_number", True) != startseq:
            continue
        outcome: str = rule["outcome"]
        return outcome
    return None


def valid_overlap(r: dict[str, Any]) -> list[dict[str, Any]]:
    return [t for t in r["trials"] if t["overlap_trial"] and t["valid"]]


def g1_metric(results: dict[str, Any], ref: str) -> dict[str, Any]:
    rules = hypothesis_rules(ref)
    rows, no_hyp, not_concl = [], [], []
    trials_match = trials_total = 0
    for cid, r in sorted(results.items()):
        exp = hyp_expected(rules, r)
        vt = valid_overlap(r)
        if exp is None:
            no_hyp.append(cid)
            continue
        if len(vt) < MIN_VALID:
            not_concl.append(cid)
            continue
        m = sum(1 for t in vt if t["outcome"] == exp)
        trials_match += m
        trials_total += len(vt)
        rows.append({"case": cid, "hyp": exp, "observed": dict(Counter(t["outcome"] for t in vt)),
                     "valid": len(vt), "match": m})
    pairs_ok = sum(1 for x in rows if x["match"] / x["valid"] >= 0.95)
    return {"ref": ref, "trial_match": trials_match, "trial_total": trials_total,
            "trial_rate": trials_match / max(trials_total, 1),
            "pairs_match": pairs_ok, "pairs_total": len(rows),
            "mismatches": [x for x in rows if x["match"] / x["valid"] < 0.95],
            "no_hypothesis": no_hyp, "not_conclusive": not_concl}


def g1_groups(results: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for g, pairs in G1_GROUPS.items():
        members = []
        for cid, r in sorted(results.items()):
            key = (HYP_CLASS.get(r["pending"], r["pending"]),
                   HYP_CLASS.get(r["committed"], r["committed"]))
            if key not in pairs or r["isolation"] != "serializable" or r.get("variant"):
                continue
            vt = valid_overlap(r)
            k2 = sum(1 for t in vt if t["outcome"] == "K2")
            members.append({"case": cid, "k2": k2, "valid": len(vt),
                            "confirmed": len(vt) >= MIN_VALID and k2 / len(vt) >= 0.95})
        out[g] = {"members": members, "confirmed": any(m["confirmed"] for m in members)}
    return out


def build_verified(results: dict[str, Any]) -> list[dict[str, Any]]:
    today = date.today().isoformat()
    out = []
    for _cid, r in sorted(results.items()):
        vt = valid_overlap(r)
        e: dict[str, Any] = {
            "pending": r["pending"], "committed": r["committed"], "isolation": r["isolation"],
            "format_version": r["format_version"], "client": r.get("client", "1.12.0"),
            "catalog_server": r.get("catalog_server", "b0df3ca"),
            "valid_trials": len(vt), "trials": r["trials_overlap"],
            "observed": dict(Counter(t["outcome"] for t in vt)),
            "control": r["control_counts"], "expected": r["expected"],
            "matches": r["matches"], "date": today,
        }
        if r.get("variant"):
            e["variant"] = r["variant"]
        if r.get("scenario"):
            e["scenario"] = r["scenario"]
        out.append(e)
    return out


def write_verified(matrix_path: Path, verified: list[dict[str, Any]]) -> None:
    content = matrix_path.read_text()
    block = "verified:\n" + "".join(
        "  - " + json.dumps(e, ensure_ascii=False) + "\n" for e in verified)
    idx = content.find("\nverified:")
    content = content[:idx + 1] + block if idx != -1 else content.rstrip() + "\n" + block
    matrix_path.write_text(content)


def heatmap(results: dict[str, Any], fv: int, out: Path) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import numpy as np

    rs = [r for r in results.values() if r["format_version"] == fv
          and r["isolation"] == "serializable" and not r.get("variant") and not r.get("scenario")]
    pend = sorted({r["pending"] for r in rs})
    comm = sorted({r["committed"] for r in rs})
    colors = {"K2": 2.0, "K1": 1.0, "success": 0.0}
    mat = np.full((len(pend), len(comm)), np.nan)
    fig, ax = plt.subplots(figsize=(1.0 * len(comm) + 3, 0.6 * len(pend) + 2))
    for r in rs:
        i, j = pend.index(r["pending"]), comm.index(r["committed"])
        vt = valid_overlap(r)
        cnt = Counter(t["outcome"] for t in vt)
        top = cnt.most_common(1)[0][0] if cnt else "n/a"
        mat[i, j] = colors.get(top, 3.0) if len(vt) >= MIN_VALID else np.nan
        mark = "" if r["matches"] else " ≠"
        ax.text(j, i, f"{top}\n{cnt.get(top, 0)}/{len(vt)}{mark}", ha="center", va="center",
                fontsize=7)
    cmap = matplotlib.colors.ListedColormap(["#e0e0e0", "#91bfdb", "#fc8d59", "#999999"])
    ax.imshow(mat, cmap=cmap, vmin=-0.5, vmax=3.5, aspect="auto")
    ax.set_xticks(range(len(comm)), comm, rotation=45, ha="right", fontsize=8)
    ax.set_yticks(range(len(pend)), pend, fontsize=8)
    ax.set_xlabel("committed")
    ax.set_ylabel("pending")
    ax.set_title(f"Iceberg 1.12.0, format v{fv}, serializable: outcome (valid trials); "
                 "≠ — differs from verified matrix")
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    plt.close(fig)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--results", type=Path, action="append", required=True)
    ap.add_argument("--matrix", type=Path, default=DEFAULT_MATRIX)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--no-write", action="store_true", help="do not touch the YAML")
    args = ap.parse_args(argv)

    results = load_results(args.results)
    print(f"Loaded {len(results)} cases.")
    bad = [cid for cid, r in results.items() if not r["matches"]]
    print(f"Cases not matching the current matrix: {len(bad)}")
    for cid in bad:
        r = results[cid]
        print(f"  {cid}: expected={r['expected']} observed={r['overlap_counts']} "
              f"valid={r['valid_trials']} control={r['control_counts']}")

    if not args.no_write:
        write_verified(args.matrix, build_verified(results))
        print(f"verified: {len(results)} entries → {args.matrix}")

    args.out.mkdir(parents=True, exist_ok=True)
    for fv in sorted({r["format_version"] for r in results.values()}):
        heatmap(results, fv, args.out / f"heatmap-v{fv}.png")

    summary = {"groups": g1_groups(results),
               "hypothesis": {ref: g1_metric(results, ref) for ref in HYPOTHESIS_REFS}}
    (args.out / "g1.json").write_text(json.dumps(summary, indent=1, ensure_ascii=False))

    print("\nG1 criterion 1 (K2 in ≥ 3 of 4 groups):")
    n_conf = 0
    for g, d in summary["groups"].items():
        n_conf += d["confirmed"]
        mem = ", ".join(f"{m['case']} K2 {m['k2']}/{m['valid']}" for m in d["members"])
        print(f"  {'+' if d['confirmed'] else '-'} {g}: {mem}")
    print(f"  confirmed {n_conf}/4")
    print("\nG1 criterion 2 (observed = original hypothesis, ≥ 95% of valid trials):")
    for ref, m in summary["hypothesis"].items():
        print(f"  {ref}: trials {m['trial_match']}/{m['trial_total']} = {m['trial_rate']:.1%}; "
              f"pairs {m['pairs_match']}/{m['pairs_total']}; no hypothesis "
              f"{len(m['no_hypothesis'])}; not conclusive {len(m['not_conclusive'])}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
