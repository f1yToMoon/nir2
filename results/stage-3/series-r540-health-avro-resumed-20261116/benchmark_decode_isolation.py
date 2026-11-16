"""Separate CPU/GIL contention from S3 IO using preserved production Avro bytes."""
import gzip
import io
import json
import multiprocessing
import threading
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from pathlib import Path

import fastavro
import pyarrow as pa


def aggregate(payload):
    stats = {}
    labels = {}
    for e in fastavro.reader(io.BytesIO(payload)):
        assert e['status'] in (0, 1, 2)
        if e['status'] == 2:
            continue
        f = e['data_file']
        assert f['content'] in (0, 1, 2)
        p = f['partition']
        key = (p['dt'], p['key_bucket'])
        if key not in labels:
            labels[key] = f"dt={key[0]}/key_bucket={key[1]}"
        row = stats.setdefault(labels[key], [0, 0, 0, 0])
        row[0] += f['content'] == 0
        row[1] += f['content'] == 0 and f['file_size_in_bytes'] < 32*1024*1024
        row[2] += f['content'] != 0
        row[3] += f['file_size_in_bytes']
    return stats


def stress(payload, started, stop):
    started.set()
    while not stop.is_set():
        # Same decoder/stream type as the existing freshness observer; no Spark or IO.
        for _ in fastavro.reader(pa.BufferReader(payload)):
            pass


def main():
    root = Path(__file__).resolve().parent / 's1-delay-diagnostic'
    objects = json.loads((root/'objects.json').read_text())
    payloads = [gzip.decompress((root/row['archive']).read_bytes())
                for path, row in objects.items() if path.endswith('.avro') and '/snap-' not in path]
    largest = max(payloads, key=len)
    expected = [aggregate(p) for p in payloads]
    rows = []
    with ProcessPoolExecutor(2, mp_context=multiprocessing.get_context('spawn')) as processes:
        assert processes.submit(aggregate, largest).result() == aggregate(largest)
        for with_stress in (False, True):
            for variant in ('threads', 'processes'):
                for trial in range(3):
                    stop, started = threading.Event(), threading.Event()
                    thread = None
                    if with_stress:
                        thread = threading.Thread(target=stress, args=(largest, started, stop))
                        thread.start()
                        started.wait()
                    try:
                        begin = time.perf_counter()
                        if variant == 'threads':
                            with ThreadPoolExecutor(8) as threads:
                                actual = list(threads.map(aggregate, payloads))
                        else:
                            actual = list(processes.map(aggregate, payloads))
                        elapsed = time.perf_counter()-begin
                        assert actual == expected
                        row = dict(with_stress=with_stress, variant=variant, trial=trial,
                                   elapsed_s=elapsed, equal=len(actual))
                        rows.append(row)
                        print(json.dumps(row), flush=True)
                    finally:
                        stop.set()
                        if thread:
                            thread.join()
    (root/'decode-isolation-benchmark.json').write_text(json.dumps(rows, indent=2))


if __name__ == '__main__':
    main()
