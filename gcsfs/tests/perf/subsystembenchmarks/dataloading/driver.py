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

    def run_read(self, prefix, params, manifest) -> ReadResult:
        """Read corpus at prefix for params.rounds epochs using ingestion manifest."""
        ...


def timestamp():
    """Monotonic clock specified system-wide for cross-process timestamp comparisons."""
    return time.clock_gettime(time.CLOCK_MONOTONIC)


# Timeout in seconds for rank synchronization at the round barrier.
ROUND_BARRIER_TIMEOUT_SECONDS = 3600


def round_barrier(ctx, world_size):
    """Return a Barrier for multi-rank synchronization, or None if single-rank."""
    return ctx.Barrier(world_size) if world_size > 1 else None


def await_round(barrier):
    """Synchronize ranks at the barrier before starting a round."""
    if barrier is not None:
        barrier.wait(timeout=ROUND_BARRIER_TIMEOUT_SECONDS)


def measure_epochs(
    loader,
    rounds,
    rows_in_batch,
    *,
    barrier=None,
    on_epoch=None,
    target=None,
):
    """Return per-epoch timestamps, row counts, and first-epoch TTFB."""
    per_epoch = []
    ttfb = None
    for epoch in range(rounds):
        if on_epoch is not None:
            on_epoch(epoch)
        await_round(barrier)
        begin = timestamp()
        rows = 0
        if target is not None and target <= 0:
            # Skip reading if target budget is zero to avoid uncounted batches.
            per_epoch.append((begin, timestamp(), 0))
            continue
        for batch in loader:
            if epoch == 0 and ttfb is None:
                ttfb = timestamp() - begin
            batch_rows = rows_in_batch(batch)
            if target is not None:
                batch_rows = min(batch_rows, target - rows)
            rows += batch_rows
            if target is not None and rows >= target:
                break
        per_epoch.append((begin, timestamp(), rows))
    return per_epoch, ttfb if ttfb is not None else float("inf")


def reduce_split(results, rounds):
    """Reduce per-rank results into wall duration, rows, and global TTFB."""
    durations, rows_list = [], []
    for e in range(rounds):
        begins = [res[0][e][0] for res in results]
        ends = [res[0][e][1] for res in results]
        durations.append(max(ends) - min(begins))
        rows_list.append(sum(res[0][e][2] for res in results))
    first_begins = [res[0][0][0] for res in results]
    first_batches = [res[0][0][0] + res[1] for res in results]
    ttfb = max(first_batches) - min(first_begins)
    return durations, rows_list, ttfb


def spawn_rank_epochs(rank_entry, prefix, params, *rank_args):
    """Spawn rank processes and reduce epoch metrics and dataset build time."""
    import torch.multiprocessing as mp

    ctx = mp.get_context("spawn")
    with ctx.Manager() as manager:
        queue = manager.Queue()
        mp.spawn(
            rank_entry,
            args=(
                params.world_size,
                prefix,
                params,
                round_barrier(ctx, params.world_size),
                queue,
                *rank_args,
            ),
            nprocs=params.world_size,
            join=True,
        )
        results = [queue.get() for _ in range(params.world_size)]
    durations, rows, ttfb = reduce_split(results, params.rounds)
    return durations, rows, ttfb, max(result[2] for result in results)


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
