"""Read-only digest of an Iceberg REST commit body for the proxy debug log."""
from __future__ import annotations

import json
from typing import Any


def commit_digest(body: bytes) -> dict[str, Any]:
    """Read-only copy of the commit's requirements/updates for the debug log.

    The forwarded body is the original bytes; this parses a separate copy.
    add-snapshot keeps the full snapshot (summary, manifest-list, parent),
    other updates keep only their action and snapshot ids.
    """
    try:
        doc = json.loads(body)
    except ValueError:
        return {"body_error": "not json"}
    if not isinstance(doc, dict):
        return {"body_error": "not an object"}
    updates: list[dict[str, Any]] = []
    for u in doc.get("updates", []):
        action = u.get("action")
        if action == "add-snapshot":
            updates.append({"action": action, "snapshot": u.get("snapshot")})
        elif action == "remove-snapshots":
            updates.append({"action": action, "snapshot-ids": u.get("snapshot-ids", [])})
        elif action == "set-snapshot-ref":
            updates.append({"action": action, "ref-name": u.get("ref-name"),
                            "snapshot-id": u.get("snapshot-id")})
        else:
            updates.append({"action": action})
    return {"requirements": doc.get("requirements", []), "updates": updates}
