"""Read-only diagnostic after S1 coverage failed; never changes measured data."""
import cProfile
import gzip
import hashlib
import json
import pstats
import time
from datetime import datetime, timezone
from pathlib import Path

from runner.db import DB
from runner.env import Env
from runner.health import inspect_partition_files
from pyiceberg.table import StaticTable
from runner.health import _aws

root = Path('/workspace/results/stage-3/series-r540-health-avro-resumed-20261116/s1-delay-diagnostic')
root.mkdir(exist_ok=False)
env = Env()
db = DB(env.pg_dsn, env.pg_schema)
rid = 'stage3-S1-P0-mid-s11-r540-health-avro-20261116'
try:
    rows = db.query('SELECT * FROM {s}.table_meta WHERE run_id=%s ORDER BY at', [rid])
finally:
    db.close()
index = next(i for i in range(1, len(rows)) if (rows[i]['at'] - rows[i-1]['at']).total_seconds() > 31)
(root/'source-samples.json').write_text(json.dumps(rows, default=str, indent=2))
cache = {}
observations = []
objects = {}


def archive(location, io):
    if location not in objects:
        with io.new_input(location).open() as f:
            raw = f.read()
        sha = hashlib.sha256(raw).hexdigest()
        (root/(sha+'.gz')).write_bytes(gzip.compress(raw, mtime=0))
        objects[location] = dict(sha256=sha, bytes=len(raw), archive=sha+'.gz')


for i in (index-1, index):
    location = rows[i]['metadata_location']
    props = {'s3.endpoint': env.s3_endpoint, 's3.access-key-id': _aws('AWS_ACCESS_KEY_ID'),
             's3.secret-access-key': _aws('AWS_SECRET_ACCESS_KEY'), 's3.region': _aws('AWS_REGION', 'us-east-1')}
    table = StaticTable.from_metadata(location, properties=props)
    archive(location, table.io)
    snapshot = table.current_snapshot()
    archive(snapshot.manifest_list, table.io)
    manifests = snapshot.manifests(table.io)
    missing = [m for m in manifests if m.manifest_path not in cache]
    for manifest in manifests:
        archive(manifest.manifest_path, table.io)
    profiler = cProfile.Profile()
    started = time.perf_counter()
    cpu = time.process_time()
    profiler.enable()
    stats = inspect_partition_files(env, location, cache)
    profiler.disable()
    elapsed = time.perf_counter()-started
    cpu = time.process_time()-cpu
    profiler.dump_stats(str(root/f'sample-{i}.pstats'))
    with (root/f'sample-{i}-profile.txt').open('w') as f:
        pstats.Stats(profiler, stream=f).sort_stats('cumulative').print_stats()
    observations.append(dict(index=i, metadata_location=location, snapshot_id=snapshot.snapshot_id,
                             manifests=len(manifests), new_manifests=len(missing),
                             new_manifest_bytes=sum(objects[m.manifest_path]['bytes'] for m in missing),
                             elapsed_s=elapsed, process_cpu_s=cpu, partitions=len(stats)))
    (root/'observations.json').write_text(json.dumps(observations, indent=2))
(root/'objects.json').write_text(json.dumps(objects, indent=2))
print(json.dumps(dict(at=datetime.now(timezone.utc).isoformat(), observations=observations), indent=2))
