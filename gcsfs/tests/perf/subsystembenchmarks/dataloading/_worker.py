"""Subprocess / torchrun worker entry point for data-loading read drivers."""

from __future__ import annotations

import argparse
import datetime
import importlib
import json
import os
from collections.abc import Callable, Sequence
from types import SimpleNamespace
from typing import Any

from gcsfs.tests.perf.subsystembenchmarks.dataloading import driver

WorkerFn = Callable[
    [int, int, str, SimpleNamespace, object, dict[str, Any]],
    tuple[list[tuple[float, float, int]], float, float],
]

# Explicit registry populated by tests or drivers.
_WORKERS: dict[str, WorkerFn] = {}

# Lazy "module:callable" entry points for built-in worker kinds.
_BUILTIN_WORKERS: dict[str, str] = {
    "hf": "gcsfs.tests.perf.subsystembenchmarks.dataloading.huggingface_datasets.read.driver:worker_entry",
    "wds": "gcsfs.tests.perf.subsystembenchmarks.dataloading.webdataset.read.driver:worker_entry",
}


def register_worker(kind: str, fn: WorkerFn) -> None:
    """Register a worker entry point for ``kind``."""
    _WORKERS[kind] = fn


def _resolve_worker(kind: str) -> WorkerFn:
    if kind in _WORKERS:
        return _WORKERS[kind]
    target = _BUILTIN_WORKERS.get(kind)
    if target is None:
        raise ValueError(f"Unknown worker kind: {kind!r}")
    module_name, attr_name = target.split(":", 1)
    module = importlib.import_module(module_name)
    fn = getattr(module, attr_name)
    _WORKERS[kind] = fn
    return fn


class _DistBarrier:
    """Adapter matching ``driver.await_round``'s ``.wait(timeout=...)`` interface."""

    def wait(self, timeout=None) -> None:
        del timeout  # Timeout is configured on init_process_group.
        import torch.distributed as dist

        dist.barrier()


def _write_result(
    result_file: str,
    durations: list[float],
    rows: list[int],
    ttfb: float,
    build_seconds: float,
) -> None:
    payload = {
        "durations": list(durations),
        "rows": list(rows),
        "ttfb": ttfb,
        "build_seconds": build_seconds,
    }
    with open(result_file, "w", encoding="utf-8") as f:
        json.dump(payload, f)


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Dataloading benchmark rank worker.")
    parser.add_argument("--kind", required=True, help="Registered worker kind.")
    parser.add_argument("--spec-file", required=True, help="Path to JSON spec file.")
    parser.add_argument(
        "--result-file", required=True, help="Path to JSON result output file."
    )
    parser.add_argument(
        "--distributed",
        action="store_true",
        help="Coordinate ranks via a gloo torch.distributed process group.",
    )
    return parser.parse_args(argv)


_TPU_ENV_PREFIX = "TORCH_TPU_"


def main(argv: Sequence[str] | None = None) -> None:
    args = _parse_args(argv)
    worker_fn = _resolve_worker(args.kind)
    with open(args.spec_file, encoding="utf-8") as f:
        spec = json.load(f)

    prefix = spec["prefix"]
    params = SimpleNamespace(**spec["params"])
    extra = spec.get("extra", {})

    if not args.distributed:
        for key in [k for k in os.environ if k.startswith(_TPU_ENV_PREFIX)]:
            os.environ.pop(key, None)
        per_epoch, ttfb, build_seconds = worker_fn(0, 1, prefix, params, None, extra)
        durations = [end - begin for begin, end, _ in per_epoch]
        rows = [count for _, _, count in per_epoch]
        _write_result(args.result_file, durations, rows, ttfb, build_seconds)
        return

    import torch.distributed as dist

    dist.init_process_group(
        backend="gloo",
        timeout=datetime.timedelta(seconds=driver.ROUND_BARRIER_TIMEOUT_SECONDS),
    )
    try:
        rank = dist.get_rank()
        world_size = dist.get_world_size()
        result = worker_fn(rank, world_size, prefix, params, _DistBarrier(), extra)
        gathered = [None] * world_size if rank == 0 else None
        dist.gather_object(result, gathered, dst=0)
        if rank == 0:
            durations, rows, ttfb = driver.reduce_split(gathered, params.rounds)
            build_seconds = max(item[2] for item in gathered)
            _write_result(args.result_file, durations, rows, ttfb, build_seconds)
    finally:
        dist.destroy_process_group()


if __name__ == "__main__":
    main()
