import os
import time

import lightning.pytorch as L
import torch
from lightning.pytorch.callbacks import ModelCheckpoint
from lightning.pytorch.trainer.call import _call_configure_model
from lightning.pytorch.trainer.states import TrainerFn
from torch.utils.data import DataLoader

from gcsfs.tests.perf.subsystembenchmarks._common import accelerator
from gcsfs.tests.perf.subsystembenchmarks.checkpointing._dist import MAX_RANKS
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


def setup_checkpoint_for_load(prefix, params, trainer_args, is_distributed=False):

    args = trainer_args.copy()
    checkpoint_callback = ModelCheckpoint(
        dirpath=prefix,
        filename="model",
        save_top_k=1,
    )
    args["callbacks"] = [checkpoint_callback]
    args["enable_checkpointing"] = True

    if is_distributed:
        args["strategy"] = get_strategy(params, setup=True)

    trainer = L.Trainer(**args)

    dataset = DummyDataset(in_features=4096)
    dataloader = DataLoader(dataset, batch_size=2 if is_distributed else 1)
    model = DummyModel(params)
    model = model.to(torch.bfloat16)

    trainer.fit(model, train_dataloaders=dataloader)

    return checkpoint_callback.best_model_path


def _setup_read_trainer(prefix, params, world_size=1, strategy=None):
    trainer_args = {
        "default_root_dir": prefix,
        **trainer_device_args(world_size),
        "max_steps": 1,
        "precision": "bf16-mixed",
        "enable_checkpointing": False,
        "logger": False,
    }
    if strategy is not None:
        trainer_args["strategy"] = strategy

    model = DummyModel(params)
    model = model.to(torch.bfloat16)
    trainer = L.Trainer(**trainer_args)
    trainer.strategy.connect(model)
    model.trainer = trainer
    trainer.state.fn = TrainerFn.FITTING
    trainer.strategy.setup_environment()
    _call_configure_model(trainer)
    if trainer.strategy.restore_checkpoint_after_setup:
        trainer.strategy.setup(trainer)

    return trainer


def _load_step(trainer, ckpt_path):
    trainer.strategy.load_checkpoint(ckpt_path)


class PLCheckpointReadDriver(CheckpointDriver):
    def setup(self, prefix, params):
        stage_gcs_model(getattr(params, "model_id", ""))
        if is_distributed_strategy(params.strategy):
            setup_world_size = (
                getattr(params, "setup_world_size", None) or params.world_size
            )
            run_worker(
                "setup_load",
                prefix,
                params,
                world_size=setup_world_size,
                rdzv_id=f"{params.name}-setup",
            )
        else:
            trainer_args = {
                "default_root_dir": prefix,
                **trainer_device_args(1),
                "max_steps": 1,
                "precision": "bf16-mixed",
                "enable_checkpointing": False,
                "logger": False,
            }
            setup_checkpoint_for_load(
                prefix, params, trainer_args, is_distributed=False
            )

    def run(self, prefix, params):
        stage_gcs_model(getattr(params, "model_id", ""))
        if is_distributed_strategy(params.strategy):
            data = run_worker(
                "load",
                prefix,
                params,
                world_size=params.world_size,
                rdzv_id=f"{params.name}-load",
                expect_result=True,
            )
            return CheckpointResult(durations=data["durations"])

        device = accelerator.bind_rank(0)
        ckpt_path = os.path.join(prefix, "model.ckpt")
        trainer = _setup_read_trainer(prefix, params, world_size=1)

        durations = []
        for _ in range(params.rounds):
            begin = time.perf_counter()
            _load_step(trainer, ckpt_path)
            accelerator.synchronize(device)
            end = time.perf_counter()
            durations.append(end - begin)

        return CheckpointResult(durations=durations)

    def read_count(self, params) -> int:
        if params.strategy in ("ddp", "fsdp_full", "model_parallel_full"):
            if accelerator.device_kind() == "cpu":
                return min(params.world_size, MAX_RANKS)
            return params.world_size
        return 1
