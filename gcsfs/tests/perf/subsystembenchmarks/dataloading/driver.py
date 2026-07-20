"""Read-driver interface and helper functions for gcsfs validation and rank reduction.

Drivers execute engine-specific reads through gcsfs. All drivers must route through fsspec/gcsfs.
"""

import dataclasses
import time
from typing import Protocol, runtime_checkable


@dataclasses.dataclass
class ReadResult:
    """Container for driver run metrics returned to read_case runner."""

    durations: list  # per-epoch wall seconds
    rows_per_epoch: list
    ttfb_seconds: float
    build_seconds: float  # dataset construction duration (excluded from rounds)
    extra_columns: dict = dataclasses.field(default_factory=dict)


@runtime_checkable
class ReadDriver(Protocol):
    formats: tuple

    def run_read(self, prefix, params) -> ReadResult:
        """Read per-case corpus for params.rounds epochs."""
        ...


def timestamp():
    """Monotonic clock specified system-wide for cross-process timestamp comparisons."""
    return time.clock_gettime(time.CLOCK_MONOTONIC)


def reduce_split(results, rounds):
    """Reduce per-rank results into per-epoch duration, rows, and max TTFB across ranks."""
    durations, rows_list = [], []
    for e in range(rounds):
        begins = [res[0][e][0] for res in results]
        ends = [res[0][e][1] for res in results]
        durations.append(max(ends) - min(begins))
        rows_list.append(sum(res[0][e][2] for res in results))
    ttfb = max(res[1] for res in results)
    return durations, rows_list, ttfb


def assert_fsspec_gcsfs(prefix):
    """Verify that gs:// URLs route through gcsfs via fsspec."""
    if not str(prefix).startswith("gs://"):
        return
    import fsspec

    import gcsfs

    fs, _ = fsspec.core.url_to_fs(prefix)
    if not isinstance(fs, gcsfs.GCSFileSystem):
        raise AssertionError(f"gs:// prefix not routed to gcsfs: {type(fs)!r}")


def gcsfs_pyarrow_fs():
    """Return PyFileSystem wrapping gcsfs to prevent default pyarrow native GCS fallback."""
    import pyarrow.fs as pafs

    import gcsfs

    return pafs.PyFileSystem(pafs.FSSpecHandler(gcsfs.GCSFileSystem()))


def assert_gcsfs_backed(fs):
    """Verify that a pyarrow filesystem is backed by gcsfs."""
    import pyarrow.fs as pafs

    import gcsfs

    if not isinstance(fs, pafs.PyFileSystem):
        raise AssertionError(
            f"expected PyFileSystem(FSSpecHandler(gcsfs)), got {type(fs)!r}"
        )
    underlying = getattr(getattr(fs, "handler", None), "fs", None)
    if underlying is not None and not isinstance(underlying, gcsfs.GCSFileSystem):
        raise AssertionError(f"filesystem is not gcsfs-backed: {underlying!r}")
