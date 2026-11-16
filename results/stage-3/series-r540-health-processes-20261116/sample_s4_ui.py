"""Three live UI comparisons during S4, without submitting Spark work."""
import json
import subprocess
import time
from pathlib import Path

root = Path(__file__).resolve().parent
rid = 'stage3-S4-P0-mid-s11-r540-health-processes-20261116'
started = time.monotonic()
statuses = []
for label, offset in [('early',400),('mid',900),('late',1500)]:
    time.sleep(max(0, started + offset - time.monotonic()))
    out = root / ('cost-ui-S4-' + label + '.json')
    assert not out.exists()
    with out.open('w') as stdout, out.with_suffix('.stderr').open('w') as stderr:
        r = subprocess.run(['docker','compose','--env-file','infra/.env','-f','infra/docker-compose.yml',
            'exec','-T','runner','python3','/workspace/' + str(root.relative_to(Path('/home/an/nir')) / 'cost_ui_sample.py'),rid],stdout=stdout,stderr=stderr)
    statuses.append(dict(label=label,offset_s=offset,exit=r.returncode))
    (root/'ui-sampling-status.json').write_text(json.dumps(statuses,indent=2))
    print(label,'exit',r.returncode,flush=True)
    assert r.returncode == 0
