"""General utility functions for gcsfs."""

from __future__ import annotations


def is_empty_range(start: int | None, end: int | None, size: int | None = None) -> bool:
    """Returns True if the requested byte range represents zero bytes or falls beyond EOF."""
    if size is not None:
        start_idx, end_idx, _ = slice(start, end).indices(size)
        return start_idx >= end_idx

    # A slice ending at index 0 is always empty (e.g. [:0] or [0:0])
    if end == 0:
        return True

    # Reading to EOF without known size cannot be proven empty
    if end is None:
        return False

    # In Python slicing, omitting start (start is None) defaults to index 0 (e.g. [:end])
    start = start or 0

    # Negative offsets without known size cannot be evaluated yet
    if start < 0 or end < 0:
        return False

    return start >= end
