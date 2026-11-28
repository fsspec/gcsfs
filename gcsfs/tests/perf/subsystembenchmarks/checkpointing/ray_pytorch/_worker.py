"""Isolated subprocess worker for Ray PyTorch checkpoint benchmarks."""

from __future__ import annotations

import argparse
import dataclasses
import json
from collections.abc import Sequence

from gcsfs.tests.perf.subsystembenchmarks.checkpointing.ray_pytorch.common import (
    run_ray_workers,
    shutdown_ray,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.ray_pytorch.parameters import (
    RayCheckpointParameters,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.ray_pytorch.read.driver import (
    RayCheckpointSetupWorker,
    run_ray_load,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.ray_pytorch.write.driver import (
    run_ray_save,
)


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Isolated subprocess worker for Ray PyTorch checkpoint benchmarks."
    )
    parser.add_argument(
        "--op",
        required=True,
        choices=("setup_load", "load", "save"),
    )
    parser.add_argument("--spec-file", required=True)
    parser.add_argument("--result-file", default=None)
    args = parser.parse_args(argv)

    with open(args.spec_file, encoding="utf-8") as f:
        spec = json.load(f)
    prefix = spec["prefix"]
    params = RayCheckpointParameters(**spec["params"])

    try:
        if args.op == "setup_load":
            setup_world_size = params.setup_world_size
            setup_params = dataclasses.replace(
                params,
                world_size=setup_world_size,
                tensor_parallel_size=params.setup_tensor_parallel_size,
                data_parallel_size=params.setup_data_parallel_size,
            )
            run_ray_workers(
                RayCheckpointSetupWorker,
                setup_world_size,
                prefix,
                setup_params,
                run_method="setup_and_save",
            )
        elif args.op == "load":
            durations = run_ray_load(prefix, params)
            assert args.result_file is not None
            with open(args.result_file, "w", encoding="utf-8") as f:
                json.dump({"durations": durations}, f)
        elif args.op == "save":
            durations = run_ray_save(prefix, params)
            assert args.result_file is not None
            with open(args.result_file, "w", encoding="utf-8") as f:
                json.dump({"durations": durations}, f)
    finally:
        shutdown_ray()


if __name__ == "__main__":
    main()
