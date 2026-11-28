import os
import time

import lightning.pytorch as L
import torch
import torch.distributed as dist
from torch.utils.data import DataLoader

from gcsfs.tests.perf.subsystembenchmarks._common import accelerator
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.driver import (
    CheckpointDriver,
    CheckpointResult,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.model_staging import (
    stage_gcs_model,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch_lightning.common import (
    DummyDataset,
    DummyModel,
    get_strategy,
    is_distributed_strategy,
    run_worker,
    trainer_device_args,
)


class _TimedSaves(L.Callback):
    """Times ``trainer.save_checkpoint()`` rounds at the end of training.

    ``fit()`` tears the strategy down before it returns, which moves the module and
    optimizer state to CPU, so saves timed after ``fit()`` would skip the
    device-to-host copy a real training-time checkpoint pays.
    """

    def __init__(self, ckpt_path, rounds, device, distributed):
        super().__init__()
        self.ckpt_path = ckpt_path
        self.rounds = rounds
        self.device = device
        self.distributed = distributed
        self.durations = []

    def on_train_end(self, trainer, pl_module):
        for _ in range(self.rounds):
            if self.distributed:
                dist.barrier()
            begin = time.perf_counter()
            trainer.save_checkpoint(self.ckpt_path)
            accelerator.synchronize(self.device)
            if self.distributed:
                dist.barrier()
            end = time.perf_counter()
            self.durations.append((begin, end) if self.distributed else end - begin)


def _fit_and_time_saves(prefix, params, world_size, device, distributed):
    """Fits one step and returns the timed save rounds (per-rank pairs if distributed)."""
    timed_saves = _TimedSaves(
        os.path.join(prefix, "model.ckpt"), params.rounds, device, distributed
    )
    trainer_args = {
        "default_root_dir": prefix,
        **trainer_device_args(world_size),
        "max_steps": 1,
        "precision": "bf16-mixed",
        "enable_checkpointing": False,
        "logger": False,
        "callbacks": [timed_saves],
    }
    if distributed:
        trainer_args["strategy"] = get_strategy(params)
    trainer = L.Trainer(**trainer_args)
    model = DummyModel(params).to(torch.bfloat16)
    dataloader = DataLoader(
        DummyDataset(in_features=4096), batch_size=2 if distributed else 1
    )
    trainer.fit(model, train_dataloaders=dataloader)
    return timed_saves.durations


class PLCheckpointWriteDriver(CheckpointDriver):
    """Driver for PyTorch Lightning checkpoint save benchmarks."""

    def setup(self, prefix: str, params):
        stage_gcs_model(getattr(params, "model_id", ""))

    def run(self, prefix, params):
        stage_gcs_model(getattr(params, "model_id", ""))
        if is_distributed_strategy(params.strategy):
            data = run_worker(
                "save",
                prefix,
                params,
                world_size=params.world_size,
                rdzv_id=f"{params.name}-save",
                expect_result=True,
            )
            return CheckpointResult(durations=data["durations"])

        device = accelerator.bind_rank(0)
        durations = _fit_and_time_saves(prefix, params, 1, device, distributed=False)
        return CheckpointResult(durations=durations)
