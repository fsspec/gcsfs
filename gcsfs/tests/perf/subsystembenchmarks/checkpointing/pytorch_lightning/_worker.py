"""torchrun worker entrypoint for PyTorch Lightning checkpoint benchmarks."""

from __future__ import annotations

import argparse
import json
import os
import time
from collections.abc import Sequence

import torch.distributed as dist

from gcsfs.tests.perf.subsystembenchmarks._common import accelerator
from gcsfs.tests.perf.subsystembenchmarks.checkpointing._dist import (
    reduce_durations,
    setup_distributed_env,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch_lightning.common import (
    get_strategy,
    trainer_device_args,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch_lightning.parameters import (
    PLCheckpointParameters,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch_lightning.read.driver import (
    _load_step,
    _setup_read_trainer,
    setup_checkpoint_for_load,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch_lightning.write.driver import (
    _fit_and_time_saves,
)


def _gather_and_write(
    durations: list[tuple[float, float]],
    rounds: int,
    result_file: str | None,
) -> None:
    rank = dist.get_rank()
    world_size = dist.get_world_size()
    gathered = [None] * world_size if rank == 0 else None
    dist.gather_object(durations, gathered, dst=0)
    if rank == 0:
        assert result_file is not None
        reduced = reduce_durations(gathered, world_size, rounds)
        with open(result_file, "w", encoding="utf-8") as f:
            json.dump({"durations": reduced}, f)
    dist.barrier()


def _run_setup_load(prefix: str, params: PLCheckpointParameters) -> None:
    world_size = dist.get_world_size()
    trainer_args = {
        "default_root_dir": prefix,
        **trainer_device_args(world_size),
        "max_steps": 1,
        "precision": "bf16-mixed",
        "enable_checkpointing": False,
        "logger": False,
    }
    setup_checkpoint_for_load(prefix, params, trainer_args, is_distributed=True)
    dist.barrier()


def _run_load(
    prefix: str,
    params: PLCheckpointParameters,
    *,
    device,
    result_file: str | None,
) -> None:
    world_size = dist.get_world_size()
    ckpt_path = os.path.join(prefix, "model.ckpt")
    trainer = _setup_read_trainer(
        prefix, params, world_size=world_size, strategy=get_strategy(params)
    )
    durations: list[tuple[float, float]] = []
    for _ in range(params.rounds):
        dist.barrier()
        t_start = time.perf_counter()
        _load_step(trainer, ckpt_path)
        accelerator.synchronize(device)
        dist.barrier()
        t_end = time.perf_counter()
        durations.append((t_start, t_end))
    _gather_and_write(durations, params.rounds, result_file)


def _run_save(
    prefix: str,
    params: PLCheckpointParameters,
    *,
    device,
    result_file: str | None,
) -> None:
    world_size = dist.get_world_size()
    durations = _fit_and_time_saves(
        prefix, params, world_size, device, distributed=True
    )
    _gather_and_write(durations, params.rounds, result_file)


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="torchrun worker for PyTorch Lightning checkpoint benchmarks."
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
    params = PLCheckpointParameters(**spec["params"])

    device = setup_distributed_env()
    try:
        if args.op == "setup_load":
            _run_setup_load(prefix, params)
        elif args.op == "load":
            _run_load(prefix, params, device=device, result_file=args.result_file)
        elif args.op == "save":
            _run_save(prefix, params, device=device, result_file=args.result_file)
    finally:
        dist.destroy_process_group()


if __name__ == "__main__":
    main()
