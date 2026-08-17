import os
import time

import lightning.pytorch as L
import torch
import torch.distributed as dist
from lightning.pytorch.callbacks import ModelCheckpoint
from lightning.pytorch.trainer.call import _call_configure_model
from lightning.pytorch.trainer.states import TrainerFn
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


def _rank_setup_checkpoint(rank, world_size, port, prefix, params, q):
    setup_distributed_env(rank, world_size, port)

    trainer_args = {
        "default_root_dir": prefix,
        "accelerator": "cpu",
        "devices": world_size,
        "max_steps": 1,
        "precision": "bf16-mixed",
        "enable_checkpointing": False,
    }

    setup_checkpoint_for_load(prefix, params, trainer_args, is_distributed=True)

    dist.destroy_process_group()
    q.put(None)


def _rank_load(rank, world_size, port, prefix, params, q):
    # This runs in a subprocess. We use "gloo" for CPU distributed run.
    setup_distributed_env(rank, world_size, port)

    trainer_args = {
        "default_root_dir": prefix,
        "accelerator": "cpu",
        "devices": world_size,
        "max_steps": 1,
        "precision": "bf16-mixed",
        "enable_checkpointing": False,
    }

    # The checkpoint path is deterministic based on ModelCheckpoint config
    ckpt_path = os.path.join(prefix, "model.ckpt")

    model2 = DummyModel(params)
    model2 = model2.to(torch.bfloat16)
    trainer2 = L.Trainer(strategy=get_strategy(params), **trainer_args)
    trainer2.strategy.connect(model2)
    model2.trainer = trainer2
    trainer2.state.fn = TrainerFn.FITTING
    trainer2.strategy.setup_environment()
    _call_configure_model(trainer2)
    trainer2.strategy.setup(trainer2)
    trainer2.optimizers = [model2.configure_optimizers()]

    durations = []
    for _ in range(params.rounds):
        dist.barrier()
        t_start = time.perf_counter()
        trainer2.strategy.load_checkpoint(ckpt_path)
        dist.barrier()
        t_end = time.perf_counter()
        durations.append((t_start, t_end))

    dist.destroy_process_group()
    q.put(durations)


class PLCheckpointReadDriver(CheckpointDriver):
    def setup(self, prefix, params):
        if params.strategy in (
            "ddp",
            "fsdp_sharded",
            "fsdp_full",
            "model_parallel_full",
            "model_parallel_sharded",
        ):
            setup_world_size = (
                getattr(params, "setup_world_size", None) or params.world_size
            )
            run_split(
                prefix,
                params,
                _rank_setup_checkpoint,
                world_size_override=setup_world_size,
            )
        else:
            trainer_args = {
                "default_root_dir": prefix,
                "accelerator": "cpu",
                "devices": 1,
                "max_steps": 1,
                "precision": "bf16-mixed",
                "enable_checkpointing": False,
            }
            setup_checkpoint_for_load(
                prefix, params, trainer_args, is_distributed=False
            )

    def run(self, prefix, params):
        if params.strategy in (
            "ddp",
            "fsdp_sharded",
            "fsdp_full",
            "model_parallel_full",
            "model_parallel_sharded",
        ):
            durations = run_split(prefix, params, _rank_load)
            return CheckpointResult(durations=durations)

        trainer_args = {
            "default_root_dir": prefix,
            "accelerator": "cpu",
            "devices": 1,
            "max_steps": 1,
            "precision": "bf16-mixed",
            "enable_checkpointing": False,
        }

        ckpt_path = os.path.join(prefix, "model.ckpt")

        model2 = DummyModel(params)
        model2 = model2.to(torch.bfloat16)
        trainer2 = L.Trainer(**trainer_args)
        trainer2.strategy.connect(model2)
        model2.trainer = trainer2
        trainer2.state.fn = TrainerFn.FITTING
        trainer2.strategy.setup_environment()
        _call_configure_model(trainer2)
        trainer2.strategy.setup(trainer2)
        trainer2.optimizers = [model2.configure_optimizers()]

        durations = []
        for _ in range(params.rounds):
            begin = time.perf_counter()
            trainer2.strategy.load_checkpoint(ckpt_path)
            end = time.perf_counter()
            durations.append(end - begin)

        return CheckpointResult(durations=durations)
