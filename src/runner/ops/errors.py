"""Exception text classification for Iceberg OCC outcomes (stage 1+).

The class is taken from fully qualified Iceberg exception names in the text (the Java
stack trace with its "Caused by:" chain), deepest first. Message words are not used:
Spark's rewrite actions wrap both outcomes into one RuntimeException whose message
mentions "ValidationException or CommitFailedException"
(RewriteDataFilesSparkAction.java:306-314, RewritePositionDeleteFilesSparkAction.java:238-248).
"""
from __future__ import annotations

import re

_ICEBERG_EXC = re.compile(r"org\.apache\.iceberg\.exceptions\.(\w+)")

_CLASS: dict[str, str] = {
    "ValidationException": "K2",            # data conflict found by validateXxx()
    "CommitFailedException": "K1",          # catalog CAS failed, retries exhausted
    "CommitStateUnknownException": "unknown",
}


# History of the operation removed by a concurrent expire_snapshots: work is lost as with K2,
# but it is not a data conflict (AGENTS.md, constraint 6; ADR-008, «Последствия»). Two code paths:
# - RowDelta (MoR DML) whose base snapshot itself was expired: IllegalArgumentException without
#   a retry (BaseRowDelta.java:134-139, SnapshotUtil.isAncestorOf stops at the missing snapshot);
# - any MergingSnapshotProducer that validates history (CoW OverwriteFiles, ReplacePartitions /
#   OverwriteByFilter with validate-from-snapshot-id, RewriteFiles, and RowDelta past the ancestor
#   check) whose first snapshot after the base was expired: validationHistory cannot walk back to
#   the base and throws ValidationException (MergingSnapshotProducer.java:967-994). The exception
#   class is the one of a data conflict, so the message decides; it must win over the class.
_EXPIRED: dict[str, re.Pattern[str]] = {
    "base_snapshot": re.compile(r"Snapshot -?\d+ is not an ancestor of -?\d+"),
    "validation_history": re.compile(
        r"Cannot determine history between starting snapshot -?\d+ and the last known ancestor"),
}


def expire_cause(exc_text: str) -> str | None:
    """'base_snapshot' | 'validation_history' when the failure is due to expired history, else None."""
    for cause, rx in _EXPIRED.items():
        if rx.search(exc_text):
            return cause
    return None


def classify(exc_text: str) -> str:
    """Return 'K2', 'K1', 'base_expired' or 'unknown' for an exception text (with stack trace).

    'base_expired' covers both paths of expired history (`expire_cause`), never 'K2'."""
    if expire_cause(exc_text) is not None:
        return "base_expired"
    names = _ICEBERG_EXC.findall(exc_text)
    for name in reversed(names):            # deepest cause is printed last
        if name in _CLASS:
            return _CLASS[name]
    return "unknown"


def root_iceberg_exception(exc_text: str) -> str | None:
    names = [n for n in _ICEBERG_EXC.findall(exc_text) if n in _CLASS]
    return names[-1] if names else None


def is_retriable(exc_text: str) -> bool:
    return classify(exc_text) == "K1"
