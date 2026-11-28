"""Isolated subprocess worker for Ray Data TPU read benchmarks."""

from __future__ import annotations

import argparse
import json
import os
from collections.abc import Sequence

from gcsfs.tests.perf.subsystembenchmarks._common import accelerator
from gcsfs.tests.perf.subsystembenchmarks.dataloading.ray_data.parameters import (
    RayDataReadParameters,
)
from gcsfs.tests.perf.subsystembenchmarks.dataloading.ray_data.read import driver

_TPU_ENV_PREFIX = "TORCH_TPU_"
_RAY_NOSET_TPU_ENV = "RAY_EXPERIMENTAL_NOSET_TPU_VISIBLE_CHIPS"


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Isolated Ray Data read benchmark worker."
    )
    parser.add_argument("--spec-file", required=True, help="Path to JSON spec file.")
    parser.add_argument(
        "--result-file", required=True, help="Path to JSON result output file."
    )
    return parser.parse_args(argv)


def _prepare_tpu_env(effective_ws: int) -> None:
    count = accelerator.local_device_count()
    if effective_ws > count:
        raise ValueError(
            f"world_size {effective_ws} exceeds {count} "
            f"{accelerator.accelerator_type()} devices"
        )
    for key in [k for k in os.environ if k.startswith(_TPU_ENV_PREFIX)]:
        os.environ.pop(key, None)
    if effective_ws > 1:
        os.environ[_RAY_NOSET_TPU_ENV] = "1"
        from torch.tpu import distributed as tpu_distributed

        tpu_distributed.set_tpu_launch_env(nproc_per_node=effective_ws)


def _write_result(result_file: str, result: driver.ReadResult) -> None:
    payload = {
        "durations": list(result.durations),
        "rows": list(result.rows_per_epoch),
        "ttfb": result.ttfb_seconds,
        "build_seconds": result.build_seconds,
    }
    with open(result_file, "w", encoding="utf-8") as f:
        json.dump(payload, f)


def main(argv: Sequence[str] | None = None) -> None:
    args = _parse_args(argv)
    with open(args.spec_file, encoding="utf-8") as f:
        spec = json.load(f)

    prefix = spec["prefix"]
    params = RayDataReadParameters(**spec["params"])

    if driver._uses_tpu_actors(params):
        effective_ws = (
            params.world_size if (params.split_by_node and params.world_size > 1) else 1
        )
        _prepare_tpu_env(effective_ws)

    try:
        driver.ensure_ray_initialized()
        result = driver._run_read_in_process(prefix, params)
    finally:
        driver.shutdown_ray()

    _write_result(args.result_file, result)


if __name__ == "__main__":
    main()
