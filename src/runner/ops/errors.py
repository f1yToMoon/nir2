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


def classify(exc_text: str) -> str:
    """Return 'K2', 'K1', or 'unknown' for an exception text (with stack trace)."""
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
