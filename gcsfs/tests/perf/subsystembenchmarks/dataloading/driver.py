"""Read-driver interface and helper functions for gcsfs validation and rank reduction.

Drivers execute engine-specific reads through gcsfs. All drivers must route through fsspec/gcsfs.
"""

from __future__ import annotations

import dataclasses
import json
import os
import tempfile
import time
import uuid
from typing import Protocol, runtime_checkable

from gcsfs.tests.perf.subsystembenchmarks._common import launcher

_WORKER_MODULE = "gcsfs.tests.perf.subsystembenchmarks.dataloading._worker"


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


def _serialize_params(params) -> dict:
    if dataclasses.is_dataclass(params) and not isinstance(params, type):
        return dataclasses.asdict(params)
    return dict(vars(params))


def _write_worker_spec(spec_file: str, prefix: str, params, extra: dict | None) -> None:
    payload = {
        "prefix": str(prefix),
        "params": _serialize_params(params),
        "extra": dict(extra) if extra is not None else {},
    }
    with open(spec_file, "w", encoding="utf-8") as f:
        json.dump(payload, f)


def _read_worker_result(
    result_file: str,
) -> tuple[list[float], list[int], float, float]:
    with open(result_file, encoding="utf-8") as f:
        data = json.load(f)
    return (
        list(data["durations"]),
        list(data["rows"]),
        float(data["ttfb"]),
        float(data["build_seconds"]),
    )


def run_rank_epochs_distributed(
    worker_kind: str,
    prefix: str,
    params,
    *,
    extra: dict | None = None,
    prepare_tpu_env: bool = True,
) -> tuple[list[float], list[int], float, float]:
    """Run multi-rank epochs via torchrun and return (durations, rows, ttfb, build_s)."""
    with tempfile.TemporaryDirectory(prefix="gcsfs_dl_worker_") as tmpdir:
        spec_file = os.path.join(tmpdir, "spec.json")
        result_file = os.path.join(tmpdir, "result.json")
        _write_worker_spec(spec_file, prefix, params, extra)
        args = [
            "--kind",
            worker_kind,
            "--spec-file",
            spec_file,
            "--result-file",
            result_file,
            "--distributed",
        ]
        launcher.run_torchrun(
            _WORKER_MODULE,
            args,
            world_size=params.world_size,
            rdzv_id=f"dl-{worker_kind}-{uuid.uuid4().hex[:8]}",
            prepare_tpu_env=prepare_tpu_env,
        )
        return _read_worker_result(result_file)


def run_single_rank_isolated(
    worker_kind: str,
    prefix: str,
    params,
    *,
    extra: dict | None = None,
) -> tuple[list[float], list[int], float, float]:
    """Run single-rank epochs in an isolated subprocess and return metrics."""
    with tempfile.TemporaryDirectory(prefix="gcsfs_dl_worker_") as tmpdir:
        spec_file = os.path.join(tmpdir, "spec.json")
        result_file = os.path.join(tmpdir, "result.json")
        _write_worker_spec(spec_file, prefix, params, extra)
        args = [
            "--kind",
            worker_kind,
            "--spec-file",
            spec_file,
            "--result-file",
            result_file,
        ]
        launcher.run_subprocess(_WORKER_MODULE, args)
        return _read_worker_result(result_file)


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
