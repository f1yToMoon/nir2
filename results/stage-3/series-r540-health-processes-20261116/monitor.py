"""Read-only progress monitor; does not poll the exec session or change run data."""
import json
import subprocess
from datetime import datetime,timezone
from pathlib import Path

base = Path('results/stage-3')
root = base/'series-r540-health-processes-20261116'
logs = list(base.glob('stage3-S*-r540-health-processes-20261116.log'))
path = max(logs,key=lambda p:p.stat().st_mtime) if logs else None
progress = {'at':datetime.now(timezone.utc).isoformat(), 'run':path.stem if path else None}
if path:
    with path.open('rb') as f:
        f.seek(max(0,path.stat().st_size-12000))
        lines=f.read().decode(errors='replace').splitlines()
    progress['tail']=[l for l in lines if l.startswith('[')][-3:]
    rid=path.stem
    sql="SELECT count(*),coalesce(max(gap),0),max(metadata_bytes) FROM (SELECT metadata_bytes,extract(epoch from at-lag(at)over(order by at)) AS gap FROM stand.table_meta WHERE run_id='"+rid+"')q"
    p=subprocess.run(['docker','exec','nir-postgres-1','psql','-U','capl','-d','telemetry','-Atc',sql],text=True,capture_output=True)
    progress['health_samples_maxgap_maxbytes']=p.stdout.strip() if p.returncode==0 else 'query error (see monitor stderr)'
    if p.returncode:print(p.stderr)
progress['finished_results']=[p.stem for p in (base/'runs').glob('stage3-S*-r540-health-processes-20261116.json') if not p.name.endswith('.cpu.json')]
with (root/'progress.jsonl').open('a') as f:f.write(json.dumps(progress)+'\n')
print(progress["at"], path.stem.split("-P0")[0] if path else "pending", "health(n/maxgap_s/maxbytes):", progress.get("health_samples_maxgap_maxbytes"), "results:", len(progress["finished_results"]), "/7")
