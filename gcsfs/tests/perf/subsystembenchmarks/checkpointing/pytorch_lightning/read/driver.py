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
    is_distributed_strategy,
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


def _setup_read_trainer(prefix, params, world_size=1, strategy=None):
    trainer_args = {
        "default_root_dir": prefix,
        "accelerator": "cpu",
        "devices": world_size,
        "max_steps": 1,
        "precision": "bf16-mixed",
        "enable_checkpointing": False,
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
    trainer.strategy.setup(trainer)
    trainer.optimizers = [model.configure_optimizers()]
    return trainer


def _load_step(trainer, ckpt_path):
    ckpt = trainer.strategy.load_checkpoint(ckpt_path)
    if isinstance(ckpt, dict) and "state_dict" in ckpt:
        trainer.strategy.load_model_state_dict(ckpt)
        if trainer.optimizers and "optimizer_states" in ckpt:
            trainer.strategy.load_optimizer_state_dict(ckpt)


def _rank_load(rank, world_size, port, prefix, params, q):
    # This runs in a subprocess. We use "gloo" for CPU distributed run.
    setup_distributed_env(rank, world_size, port)

    # The checkpoint path is deterministic based on ModelCheckpoint config
    ckpt_path = os.path.join(prefix, "model.ckpt")
    trainer = _setup_read_trainer(
        prefix, params, world_size=world_size, strategy=get_strategy(params)
    )

    durations = []
    for _ in range(params.rounds):
        dist.barrier()
        t_start = time.perf_counter()
        _load_step(trainer, ckpt_path)
        dist.barrier()
        t_end = time.perf_counter()
        durations.append((t_start, t_end))

    dist.destroy_process_group()
    q.put(durations)


class PLCheckpointReadDriver(CheckpointDriver):
    def setup(self, prefix, params):
        if is_distributed_strategy(params.strategy):
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
        if is_distributed_strategy(params.strategy):
            durations = run_split(prefix, params, _rank_load)
            return CheckpointResult(durations=durations)

        ckpt_path = os.path.join(prefix, "model.ckpt")
        trainer = _setup_read_trainer(prefix, params, world_size=1)

        durations = []
        for _ in range(params.rounds):
            begin = time.perf_counter()
            _load_step(trainer, ckpt_path)
            end = time.perf_counter()
            durations.append(end - begin)

        return CheckpointResult(durations=durations)
