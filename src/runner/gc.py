"""Removal of the files of past runs (tz/05 §1.1 p. 3): everything under the table location that
the golden versions do not reference (data/golden/_data*.keep.txt). Only between runs.

    python3 -m runner.gc [--size full|lite]
"""
from __future__ import annotations

import argparse
import json
import sys


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--size", default="full")
    args = ap.parse_args(argv)
    from runner.env import Env
    from runner.main import data_golden_path, gc_table_files
    dpath = data_golden_path(args.size)
    dg = json.loads(dpath.read_text())
    keep = set(dpath.with_suffix(".keep.txt").read_text().split())
    print(gc_table_files(Env(), dg["location"], keep))
    return 0


if __name__ == "__main__":
    sys.exit(main())
