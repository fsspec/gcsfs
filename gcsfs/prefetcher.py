"""Compatibility re-exports for prefetch classes.

Prefetch logic now lives in filesystem_spec/fsspec.
"""

from fsspec.prefetch import (  # noqa: F401
    HAS_CPYTHON_API,
    BackgroundPrefetcher,
    PrefetchConsumer,
    PrefetchProducer,
    RunningAverageTracker,
    _fast_slice,
)

__all__ = [
    "HAS_CPYTHON_API",
    "BackgroundPrefetcher",
    "PrefetchConsumer",
    "PrefetchProducer",
    "RunningAverageTracker",
    "_fast_slice",
]
