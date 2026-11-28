"""Isolated subprocess worker for Ray Data TPU read benchmarks.

``launcher.run_subprocess`` starts this module with a clean ``TORCH_TPU_*``
env; for multi-rank cases it sets the TorchTPU launch env before Ray starts so
every rank-pinned actor inherits it.
"""

from __future__ import annotations

import argparse
import os
from collections.abc import Sequence

from gcsfs.tests.perf.subsystembenchmarks._common import accelerator
from gcsfs.tests.perf.subsystembenchmarks.dataloading import driver as dl_driver
from gcsfs.tests.perf.subsystembenchmarks.dataloading.ray_data.read import driver

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


def _configure_tpu_env(world_size: int) -> None:
    count = accelerator.local_device_count()
    if world_size > count:
        raise ValueError(
            f"world_size {world_size} exceeds {count} "
            f"{accelerator.accelerator_type()} devices"
        )
    if world_size > 1:
        os.environ[_RAY_NOSET_TPU_ENV] = "1"
        from torch.tpu import distributed as tpu_distributed

        tpu_distributed.set_tpu_launch_env(nproc_per_node=world_size)
    else:
        os.environ.pop(_RAY_NOSET_TPU_ENV, None)


def main(argv: Sequence[str] | None = None) -> None:
    args = _parse_args(argv)
    prefix, params, _ = dl_driver.read_worker_spec(args.spec_file)

    if driver._uses_tpu_actors(params):
        multi_rank = params.split_by_node and params.world_size > 1
        _configure_tpu_env(params.world_size if multi_rank else 1)

    try:
        driver.ensure_ray_initialized()
        result = driver._run_read_in_process(prefix, params)
    finally:
        driver.shutdown_ray()

    dl_driver.write_worker_result(
        args.result_file,
        result.durations,
        result.rows_per_epoch,
        result.ttfb_seconds,
        result.build_seconds,
    )


if __name__ == "__main__":
    main()
