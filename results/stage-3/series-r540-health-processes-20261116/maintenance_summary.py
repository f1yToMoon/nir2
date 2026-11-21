"""Summarize frozen operation intervals; no live Spark jobs are started."""
import gzip
import json
import sys
from datetime import datetime
from pathlib import Path

root = Path(sys.argv[1])
plan = json.loads((root / 'plan-amendment-20261121.json').read_text())
out = {}


def at(row, key):
    return datetime.fromisoformat(row[key])


for run in plan['runs']:
    rid = run['run_id']
    ops = json.loads(gzip.decompress((root / 'evidence' / rid / 'ops.json.gz').read_bytes()))
    bg = sorted([o for o in ops if not o['controlled']], key=lambda o: o['started_at'])
    ctl = [o for o in ops if o['controlled']]
    assert bg and all(o['op_type'] == 'expire' for o in bg)
    for op in ops:
        if op['cpu_ms'] is None:
            assert op['outcome'] == 'noop' and not op['footprint_requested'], op['op_id']
    duration = [(at(o, 'finished_at') - at(o, 'started_at')).total_seconds() for o in bg]
    start_lag = [(at(o, 'started_at') - at(o, 'intent_at')).total_seconds() for o in bg]
    end_lag = [(at(o, 'finished_at') - at(o, 'intent_at')).total_seconds() for o in bg]
    overlaps = []
    for b in bg:
        for c in ctl:
            seconds = (min(at(b, 'finished_at'), at(c, 'finished_at')) -
                       max(at(b, 'started_at'), at(c, 'started_at'))).total_seconds()
            if seconds > 0:
                overlaps.append({'background_op': b['op_id'], 'controlled_op': c['op_id'],
                                 'controlled_type': c['op_type'], 'seconds': seconds})
    bg_overlap = sum(at(b, 'started_at') < at(a, 'finished_at') for a, b in zip(bg, bg[1:]))
    out[rid] = {
        'n_background': len(bg), 'background_cpu_s': sum(o['cpu_ms'] for o in bg) / 1000,
        'controlled_cpu_s': sum(o['cpu_ms'] or 0 for o in ctl) / 1000,
        'unexecuted_empty_footprint_noops': sum(o['cpu_ms'] is None for o in ctl),
        'duration_max_s': max(duration), 'start_lag_max_s': max(start_lag),
        'finish_lag_max_s': max(end_lag), 'overlapping_background_pairs': bg_overlap,
        'background_controlled_overlap_pairs': overlaps,
        'pairwise_overlap_seconds': sum(o['seconds'] for o in overlaps),
        'note': 'Pairwise intersections may overlap each other; no causal slowdown is inferred.'}
(root / 'maintenance-summary.json').write_text(json.dumps(out, indent=2))
print(json.dumps({rid: {k: v for k, v in value.items() if k != 'background_controlled_overlap_pairs'}
                  for rid, value in out.items()}, indent=2))
