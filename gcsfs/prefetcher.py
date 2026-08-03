"""Compatibility re-exports for prefetch classes.

Prefetch logic now lives in filesystem_spec/fsspec.
"""

from fsspec.prefetcher import (  # noqa: F401
    BackgroundPrefetcher,
    PrefetchConsumer,
    PrefetchProducer,
    RunningAverageTracker,
    _fast_slice,
)
from gcsfs.zb_hns_utils import HAS_CPYTHON_API  # noqa: F401

__all__ = [
    "HAS_CPYTHON_API",
    "BackgroundPrefetcher",
    "PrefetchConsumer",
    "PrefetchProducer",
    "RunningAverageTracker",
    "_fast_slice",
]
