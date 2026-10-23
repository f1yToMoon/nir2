"""Exception text classification for Iceberg OCC outcomes (stage 1+)."""
from __future__ import annotations

# Patterns that indicate a data-level conflict (K2 — ValidationException).
# Iceberg raises these from validateXxx() methods on the commit operation.
_K2_PATTERNS: tuple[str, ...] = (
    "ValidationException",
    "Found conflicting",
    "Found new conflicting",
    "Found new delete",
    "data files that were deleted",
    "Cannot commit",
    "Conflicting files",
    "found new position delete",
)

# Patterns that indicate a catalog-level CAS failure (K1 — CommitFailedException).
_K1_PATTERNS: tuple[str, ...] = (
    "CommitFailedException",
    "RetryableCommitFailedException",
    "conflict detected with a concurrent writer",
)


def classify(exc_text: str) -> str:
    """Return 'K2', 'K1', or 'unknown' for an exception message."""
    for p in _K2_PATTERNS:
        if p in exc_text:
            return "K2"
    for p in _K1_PATTERNS:
        if p in exc_text:
            return "K1"
    return "unknown"


def is_retriable(exc_text: str) -> bool:
    return classify(exc_text) == "K1"
