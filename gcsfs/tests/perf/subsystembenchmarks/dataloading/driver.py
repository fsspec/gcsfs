"""Read-driver interface and helper functions for gcsfs validation and rank reduction.

Drivers execute engine-specific reads through gcsfs. All drivers must route through fsspec/gcsfs.
"""

from __future__ import annotations

import contextlib
import dataclasses
import importlib
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


def qualified_name(obj) -> str:
    """Return the ``module:qualname`` import path of a module-level class or function."""
    name = f"{obj.__module__}:{obj.__qualname__}"
    if "<locals>" in name:
        raise ValueError(f"{name} is not importable; define it at module level")
    return name


def resolve_qualified_name(name: str):
    """Import and return the object named by a ``module:qualname`` path."""
    module_name, _, attr_path = name.partition(":")
    if not module_name or not attr_path:
        raise ValueError(f"expected 'module:qualname', got {name!r}")
    obj = importlib.import_module(module_name)
    for attr in attr_path.split("."):
        obj = getattr(obj, attr)
    return obj


def write_worker_spec(
    path: str, prefix: str, params, extra: dict | None = None
) -> None:
    """Write a rank worker's inputs; ``params`` must be a dataclass instance."""
    if not dataclasses.is_dataclass(params) or isinstance(params, type):
        raise TypeError(f"params must be a dataclass instance, got {type(params)!r}")
    payload = {
        "prefix": str(prefix),
        "params_cls": qualified_name(type(params)),
        "params": dataclasses.asdict(params),
        "extra": dict(extra) if extra is not None else {},
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f)


def read_worker_spec(path: str) -> tuple[str, object, dict]:
    """Return ``(prefix, params, extra)``, rebuilding the params dataclass."""
    with open(path, encoding="utf-8") as f:
        spec = json.load(f)
    params_cls = resolve_qualified_name(spec["params_cls"])
    if not dataclasses.is_dataclass(params_cls):
        raise TypeError(f"{spec['params_cls']} is not a dataclass")
    return spec["prefix"], params_cls(**spec["params"]), spec["extra"]


def write_worker_result(
    path: str,
    durations: list[float],
    rows: list[int],
    ttfb: float,
    build_seconds: float,
) -> None:
    """Write a worker's reduced metrics for the parent to read back."""
    payload = {
        "durations": list(durations),
        "rows": list(rows),
        "ttfb": ttfb,
        "build_seconds": build_seconds,
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f)


def read_worker_result(path: str) -> tuple[list[float], list[int], float, float]:
    """Return ``(durations, rows, ttfb, build_seconds)`` written by a worker."""
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    return (
        list(data["durations"]),
        list(data["rows"]),
        float(data["ttfb"]),
        float(data["build_seconds"]),
    )


@contextlib.contextmanager
def worker_files(prefix: str, params, extra: dict | None = None):
    """Yield ``(spec_file, result_file)`` in a private temp dir with the spec written."""
    with tempfile.TemporaryDirectory(prefix="gcsfs_dl_worker_") as tmpdir:
        spec_file = os.path.join(tmpdir, "spec.json")
        result_file = os.path.join(tmpdir, "result.json")
        write_worker_spec(spec_file, prefix, params, extra)
        yield spec_file, result_file


def run_rank_epochs_distributed(
    entry,
    prefix: str,
    params,
    *,
    extra: dict | None = None,
    cpu_only: bool = False,
) -> tuple[list[float], list[int], float, float]:
    """Run ``entry`` on ``params.world_size`` torchrun ranks.

    ``entry`` is a module-level ``(rank, world_size, prefix, params, barrier,
    extra) -> (per_epoch, ttfb, build_seconds)`` function; ``cpu_only`` runs
    the ranks on CPU for work that never touches an accelerator. Returns
    ``(durations, rows, ttfb, build_seconds)``.
    """
    with worker_files(prefix, params, extra) as (spec_file, result_file):
        launcher.run_torchrun(
            _WORKER_MODULE,
            [
                "--entry",
                qualified_name(entry),
                "--spec-file",
                spec_file,
                "--result-file",
                result_file,
                "--distributed",
            ],
            world_size=params.world_size,
            rdzv_id=f"dl-{uuid.uuid4().hex[:8]}",
            cpu_only=cpu_only,
        )
        return read_worker_result(result_file)


def run_single_rank_isolated(
    entry,
    prefix: str,
    params,
    *,
    extra: dict | None = None,
) -> tuple[list[float], list[int], float, float]:
    """Run ``entry`` as rank 0 of 1 in an isolated subprocess and return metrics."""
    with worker_files(prefix, params, extra) as (spec_file, result_file):
        launcher.run_subprocess(
            _WORKER_MODULE,
            [
                "--entry",
                qualified_name(entry),
                "--spec-file",
                spec_file,
                "--result-file",
                result_file,
            ],
        )
        return read_worker_result(result_file)


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
