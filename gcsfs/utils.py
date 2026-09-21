"""General utility functions for gcsfs."""

from __future__ import annotations


def is_empty_range(start: int | None, end: int | None, size: int | None = None) -> bool:
    """Returns True if the requested byte range represents zero bytes or falls beyond EOF.

    Handles negative offsets following Python slice semantics when size is known.
    """
    if size is not None:
        start = max(0, size + start) if (start is not None and start < 0) else start
        end = max(0, size + end) if (end is not None and end < 0) else end
        if start is not None and start >= size:
            return True

    if start is None or end is None or start < 0 or end < 0:
        return False

    return start >= end
