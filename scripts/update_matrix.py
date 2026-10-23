#!/usr/bin/env python3
"""Fill conflict_matrix.yaml `verified` section from stage-1 test results.

Usage:
    python3 scripts/update_matrix.py [--results results/stage-1/matrix_results.json]
                                      [--matrix  config/conflict_matrix.yaml]
                                      [--heatmap results/stage-1/heatmap.png]
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import date
from pathlib import Path
from typing import Any

import yaml

REPO = Path(__file__).parent.parent
DEFAULT_RESULTS = REPO / "results/stage-1/matrix_results.json"
DEFAULT_MATRIX  = REPO / "config/conflict_matrix.yaml"
DEFAULT_HEATMAP = REPO / "results/stage-1/heatmap.png"


def _load(path: Path) -> Any:
    with path.open() as fh:
        return json.load(fh)


def _build_verified(results: dict) -> list[dict]:
    today = date.today().isoformat()
    verified = []
    for cid, r in results.items():
        entry: dict = {
            "pending":        r["pending"],
            "committed":      r["committed"],
            "isolation":      r["isolation"],
            "format_version": r["format_version"],
            "iceberg":        "b0df3ca (1.12.0-SNAPSHOT)",
            "trials":         r["trials_overlap"],
            "observed":       r["overlap_counts"],
            "matches":        r["matches"],
            "date":           today,
        }
        if not r["matches"]:
            entry["mismatch_note"] = (
                f"expected {r['expected']}, "
                f"got {r['overlap_counts']}, "
                f"match_rate={r['match_rate']:.2%}"
            )
        verified.append(entry)
    return verified


def _write_matrix(matrix_path: Path, verified: list[dict]) -> None:
    with matrix_path.open() as fh:
        content = fh.read()

    # Build new `verified:` block
    new_block = "verified:\n"
    for e in verified:
        new_block += "  - " + json.dumps(e, ensure_ascii=False) + "\n"

    # Replace everything from `verified:` to end-of-file
    idx = content.find("\nverified:")
    if idx != -1:
        content = content[:idx + 1] + new_block
    else:
        content = content.rstrip() + "\n" + new_block

    with matrix_path.open("w") as fh:
        fh.write(content)


def _build_heatmap(results: dict, out: Path) -> None:
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import matplotlib.colors as mcolors
        import numpy as np
    except ImportError:
        print("matplotlib not available — skipping heatmap", file=sys.stderr)
        return

    ops = ["append", "stream_upsert", "overwrite", "cow_dml",
           "mor_dml", "rewrite_data", "rewrite_deletes", "expire"]
    n = len(ops)
    op_idx = {o: i for i, o in enumerate(ops)}

    # Build matrix: value = match_rate for v2, serializable (representative)
    mat = np.full((n, n), np.nan)
    outcome_mat = [["" for _ in range(n)] for _ in range(n)]

    for r in results.values():
        if r["format_version"] != 2:
            continue
        if r["isolation"] not in ("serializable", "default"):
            continue
        p = r["pending"]
        c = r["committed"]
        if p not in op_idx or c not in op_idx:
            continue
        pi, ci = op_idx[p], op_idx[c]
        mat[pi, ci] = r["match_rate"]
        top = max(r["overlap_counts"], key=lambda k: r["overlap_counts"][k], default="?")
        outcome_mat[pi][ci] = top

    fig, ax = plt.subplots(figsize=(10, 8))
    cmap = mcolors.LinearSegmentedColormap.from_list(
        "rg", ["#d73027", "#fdae61", "#1a9850"]
    )
    im = ax.imshow(mat, cmap=cmap, vmin=0, vmax=1, aspect="auto")

    ax.set_xticks(range(n))
    ax.set_yticks(range(n))
    ax.set_xticklabels(ops, rotation=45, ha="right", fontsize=9)
    ax.set_yticklabels(ops, fontsize=9)
    ax.set_xlabel("committed operation")
    ax.set_ylabel("pending operation")
    ax.set_title(
        "Conflict matrix: match rate (green ≥ 95%)\n"
        "Iceberg b0df3ca, v2, serializable"
    )

    for i in range(n):
        for j in range(n):
            if not np.isnan(mat[i, j]):
                label = outcome_mat[i][j]
                ax.text(j, i, label, ha="center", va="center",
                        fontsize=7, color="black")

    fig.colorbar(im, ax=ax, label="match rate")
    fig.tight_layout()
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, dpi=150)
    plt.close(fig)
    print(f"Heatmap saved: {out}")


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--results", type=Path, default=DEFAULT_RESULTS)
    ap.add_argument("--matrix",  type=Path, default=DEFAULT_MATRIX)
    ap.add_argument("--heatmap", type=Path, default=DEFAULT_HEATMAP)
    args = ap.parse_args(argv)

    if not args.results.exists():
        print(f"Results file not found: {args.results}", file=sys.stderr)
        sys.exit(1)

    results = _load(args.results)
    print(f"Loaded {len(results)} test-case results.")

    mismatches = [cid for cid, r in results.items() if not r["matches"]]
    if mismatches:
        print(f"WARNING: {len(mismatches)} mismatches:")
        for m in mismatches:
            r = results[m]
            print(f"  {m}: expected={r['expected']}, "
                  f"got={r['overlap_counts']}, rate={r['match_rate']:.2%}")
    else:
        print("All cases match expected outcomes.")

    verified = _build_verified(results)
    _write_matrix(args.matrix, verified)
    print(f"Updated verified section in {args.matrix} ({len(verified)} entries).")

    _build_heatmap(results, args.heatmap)

    # Print G1 summary
    _print_g1_summary(results)


def _print_g1_summary(results: dict) -> None:
    """Evaluate G1 gate criteria and print summary."""
    groups = {
        "compaction×CoW_DML":   lambda r: r["pending"] == "rewrite_data" and r["committed"] == "cow_dml",
        "compaction×MoR_DML":   lambda r: r["pending"] == "rewrite_data" and r["committed"] == "mor_dml",
        "DML×append_serial":    lambda r: r["pending"] in ("cow_dml", "mor_dml") and r["committed"] == "append" and r["isolation"] == "serializable",
        "DML×DML":              lambda r: r["pending"] in ("cow_dml", "mor_dml") and r["committed"] in ("cow_dml", "mor_dml"),
    }

    print("\n── G1 Criteria ──")
    k2_confirmed = 0
    for gname, pred in groups.items():
        group_results = [r for r in results.values() if pred(r) and r["format_version"] == 2]
        if not group_results:
            print(f"  {gname}: NO DATA")
            continue
        all_match = all(r["matches"] for r in group_results)
        any_k2    = any(r["overlap_counts"].get("K2", 0) > 0 for r in group_results)
        confirmed = all_match and any_k2
        if confirmed:
            k2_confirmed += 1
        symbol = "✓" if confirmed else "✗"
        rates = [f"{r['match_rate']:.0%}" for r in group_results]
        print(f"  {symbol} {gname}: K2 confirmed={confirmed}, match_rates={rates}")

    total_cases = len(results)
    matched     = sum(1 for r in results.values() if r["matches"])
    overall_rate = matched / max(total_cases, 1)
    g1_crit1 = k2_confirmed >= 3
    g1_crit2 = overall_rate >= 0.95
    print(f"\n  K2 groups confirmed: {k2_confirmed}/4 (need ≥3) → {'PASS' if g1_crit1 else 'FAIL'}")
    print(f"  Overall match rate: {overall_rate:.1%} (need ≥95%) → {'PASS' if g1_crit2 else 'FAIL'}")
    g1 = g1_crit1 and g1_crit2
    print(f"\n  G1 gate: {'GO' if g1 else 'CORRECT/PIVOT'}")


if __name__ == "__main__":
    main()
