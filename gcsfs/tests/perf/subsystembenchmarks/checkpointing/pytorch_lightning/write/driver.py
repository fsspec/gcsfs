import os
import time

import lightning.pytorch as L
import torch
import torch.distributed as dist
from torch.utils.data import DataLoader

from gcsfs.tests.perf.subsystembenchmarks.checkpointing.driver import (
    CheckpointDriver,
    CheckpointResult,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch_lightning.common import (
    DummyDataset,
    DummyModel,
    get_strategy,
    run_split,
    setup_distributed_env,
)


def _rank_save(rank, world_size, port, prefix, params, q):
    # This runs in a subprocess. We use "gloo" for CPU distributed run.
    setup_distributed_env(rank, world_size, port)

    strategy = get_strategy(params)
    trainer_args = {
        "default_root_dir": prefix,
        "accelerator": "cpu",
        "devices": world_size,
        "strategy": strategy,
        "max_steps": 1,
        "precision": "bf16-mixed",
        "enable_checkpointing": False,
    }

    trainer = L.Trainer(**trainer_args)

    dataset = DummyDataset(in_features=4096)
    dataloader = DataLoader(dataset, batch_size=2)
    model = DummyModel(params)
    model = model.to(torch.bfloat16)

    trainer.fit(model, train_dataloaders=dataloader)

    ckpt_path = os.path.join(prefix, "model.ckpt")
    durations = []
    for _ in range(params.rounds):
        # Ensure all ranks are synchronized before starting the timer
        dist.barrier()
        t_start = time.perf_counter()
        trainer.save_checkpoint(ckpt_path)
        # Ensure all ranks finished writing before stopping the timer
        dist.barrier()
        t_end = time.perf_counter()
        durations.append((t_start, t_end))

    dist.destroy_process_group()

    # Report durations from all ranks to aggregate
    q.put(durations)


class PLCheckpointWriteDriver(CheckpointDriver):
    """Driver for PyTorch Lightning checkpoint save benchmarks."""

    def setup(self, prefix: str, params):
        pass

    def run(self, prefix, params):
        if params.strategy in (
            "ddp",
            "fsdp_sharded",
            "fsdp_full",
            "model_parallel_full",
            "model_parallel_sharded",
        ):
            durations = run_split(prefix, params, _rank_save)
            return CheckpointResult(durations=durations)

        # Single device save
        model = DummyModel(params)
        model = model.to(torch.bfloat16)

        trainer_args = {
            "default_root_dir": prefix,
            "accelerator": "cpu",
            "devices": 1,
            "max_steps": 1,
            "precision": "bf16-mixed",
            "enable_checkpointing": False,
        }

        trainer = L.Trainer(**trainer_args)

        dataset = DummyDataset(in_features=4096)
        dataloader = DataLoader(dataset, batch_size=1)
        trainer.fit(model, train_dataloaders=dataloader)

        filepath = os.path.join(prefix, "model.ckpt")
        durations = []
        for _ in range(params.rounds):
            begin = time.perf_counter()
            trainer.save_checkpoint(filepath)
            end = time.perf_counter()
            durations.append(end - begin)

        return CheckpointResult(durations=durations)
