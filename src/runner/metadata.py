"""Conservative byte bound over all retained metadata versions, not periodic samples.

Iceberg retains committed metadata files when delete-after-commit is false (the
default). Expire removes snapshots/manifests/data, not these metadata.json files.
Run this after writers stop and before the *next* run's explicit file GC.
Uncommitted retained candidates are included too: they can only increase the bound.
"""
from __future__ import annotations

import gzip
import hashlib
import json
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from typing import Any

from runner.env import Env


def metadata_bound(files: list[dict[str, Any]], required: set[str], listed: set[str],
                   *, deletion_enabled: bool) -> dict[str, Any]:
    recorded = {f['location'] for f in files}
    complete = (bool(files) and recorded == listed and required <= recorded
                and not deletion_enabled and all(f['bytes'] > 0 for f in files))
    return {'complete': complete, 'files': files, 'listed': len(listed),
            'required_locations': sorted(required), 'missing': sorted(required - recorded),
            'delete_after_commit_enabled': deletion_enabled,
            'max_metadata_bytes': max((f['bytes'] for f in files), default=None)}


def capture_metadata_versions(env: Env, state: Any, started: datetime, target: Path,
                              observed_locations: set[str]) -> dict[str, Any]:
    import pyarrow.fs as pafs

    from runner.health import _aws
    fs = pafs.S3FileSystem(endpoint_override=env.s3_endpoint, scheme='http',
                          access_key=_aws('AWS_ACCESS_KEY_ID'),
                          secret_key=_aws('AWS_SECRET_ACCESS_KEY'),
                          region=_aws('AWS_REGION', 'us-east-1'))
    root = state.metadata['location'].split('://', 1)[1].rstrip('/') + '/metadata'
    objects = fs.get_file_info(pafs.FileSelector(root, recursive=True))
    locations = {'s3://' + i.path for i in objects if i.type == pafs.FileType.File
                 and i.path.endswith('.metadata.json')}
    locations |= observed_locations | {state.metadata_location}
    required = observed_locations | {state.metadata_location}
    required |= {e['metadata-file'] for e in state.metadata.get('metadata-log', [])
                 if int(e['timestamp-ms']) >= started.timestamp() * 1000}
    # Include required locations even if not under the usual table prefix. Missing
    # objects must raise, rather than silently dropping a failed read from the bound.
    locations |= required
    target.mkdir(exist_ok=False)

    def read(location: str) -> dict[str, Any]:
        with fs.open_input_file(location.split('://', 1)[1]) as f:
            raw = f.read()
        parsed = json.loads(raw)
        sha = hashlib.sha256(raw).hexdigest()
        archive = sha + '.json.gz'
        (target / archive).write_bytes(gzip.compress(raw, mtime=0))
        return {'location': location, 'bytes': len(raw), 'sha256': sha, 'archive': archive,
                'current_snapshot_id': parsed.get('current-snapshot-id'),
                'delete_after_commit_enabled': str(parsed.get('properties', {}).get(
                    'write.metadata.delete-after-commit.enabled', 'false')).lower() != 'false'}

    with ThreadPoolExecutor(8) as pool:
        files = list(pool.map(read, sorted(locations)))
    bound = metadata_bound(files, required, locations,
                           deletion_enabled=any(f['delete_after_commit_enabled'] for f in files))
    bound['method'] = 'all_retained_metadata_versions_including_uncommitted_candidates'
    (target.parent / 'metadata-inventory.json').write_text(json.dumps(bound, indent=2))
    return bound


def verify_metadata_inventory(inventory: dict[str, Any], archives: Path) -> dict[str, Any]:
    """Re-read original bytes; a boolean or sampled maximum cannot certify this bound."""
    files = []
    for entry in inventory['files']:
        archive = entry['archive']
        if Path(archive).name != archive:
            raise ValueError('invalid metadata archive path')
        raw = gzip.decompress((archives / archive).read_bytes())
        if len(raw) != entry['bytes'] or hashlib.sha256(raw).hexdigest() != entry['sha256']:
            raise ValueError(f"metadata bytes/hash mismatch: {entry['location']}")
        parsed = json.loads(raw)
        if str(parsed.get('properties', {}).get('write.metadata.delete-after-commit.enabled',
                                             'false')).lower() != 'false':
            raise ValueError('metadata deletion enabled: complete enumeration is not established')
        files.append(entry)
    result = metadata_bound(files, set(inventory['required_locations']),
                            {e['location'] for e in inventory['files']}, deletion_enabled=False)
    result['complete'] &= (len(files) == inventory['listed'] and inventory['complete'] is True)
    return result
