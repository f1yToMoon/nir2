"""Offline decoding comparison over archived immutable S1 manifest bytes."""
import copy
import gzip
import io
import json
import time
from pathlib import Path

import fastavro

root = Path(__file__).resolve().parent / 's1-delay-diagnostic'
objects = json.loads((root / 'objects.json').read_text())
payloads = [(location, gzip.decompress((root / row['archive']).read_bytes()))
            for location, row in objects.items()
            if location.endswith('.avro') and '/snap-' not in location]


def projection(schema):
    schema = copy.deepcopy(schema)
    schema['fields'] = [f for f in schema['fields'] if f['name'] in ('status', 'data_file')]
    file = next(f['type'] for f in schema['fields'] if f['name'] == 'data_file')
    file['fields'] = [f for f in file['fields'] if f['name'] in ('content', 'partition', 'file_size_in_bytes')]
    part = next(f['type'] for f in file['fields'] if f['name'] == 'partition')
    part['fields'] = [f for f in part['fields'] if f['name'] in ('dt', 'key_bucket')]
    return schema


def aggregate(payload, projected):
    source = io.BytesIO(payload)
    reader = fastavro.reader(source)
    if projected:
        schema = projection(reader.writer_schema)
        source.seek(0)
        reader = fastavro.reader(source, reader_schema=schema)
    stats = {}
    records = 0
    for entry in reader:
        assert entry['status'] in (0, 1, 2)
        if entry['status'] == 2:
            continue
        f = entry['data_file']
        assert f['content'] in (0, 1, 2)
        p = f['partition']
        key = (str(p['dt']), p['key_bucket'])
        v = stats.setdefault(key, [0, 0, 0, 0])
        size = f['file_size_in_bytes']
        v[0] += f['content'] == 0
        v[1] += f['content'] == 0 and size < 32*1024*1024
        v[2] += f['content'] != 0
        v[3] += size
        records += 1
    return stats, records


output = []
for location, payload in sorted(payloads, key=lambda p: -len(p[1])):
    item = {'location': location, 'bytes': len(payload)}
    expected = None
    for name, projected in [('full', False), ('projected', True)]:
        measurements = []
        for _ in range(3):
            start = time.perf_counter()
            result = aggregate(payload, projected)
            measurements.append(time.perf_counter() - start)
            if expected is None:
                expected = result
            assert result == expected, location
        item[name+'_s'] = measurements
    item['live_records'] = expected[1]
    output.append(item)
(root/'projection-benchmark.json').write_text(json.dumps(output, indent=2))
print(json.dumps({'objects': len(output), 'equal': len(output),
                  'full_total_s': sum(min(r['full_s']) for r in output),
                  'projected_total_s': sum(min(r['projected_s']) for r in output),
                  'largest': output[:5]}, indent=2))
