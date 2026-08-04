import dataclasses
import os
import time

import lightning.pytorch as L
import torch
import torch.distributed as dist
import torch.nn as nn
from torch.utils.data import DataLoader, Dataset


class DummyDataset(Dataset):
    """A dummy dataset to feed the trainer."""

    def __init__(self, size=10, in_features=4096):
        self.size = size
        self.in_features = in_features

    def __len__(self):
        return self.size

    def __getitem__(self, idx):
        return torch.randn(self.in_features)


class DummyModel(L.LightningModule):
    """A dummy LightningModule of a configurable size in Megabytes using a Linear layer."""

    def __init__(self, params):
        super().__init__()
        self.params = params
        size_mb = params.model_size_mb
        tp_size = getattr(params, "tensor_parallel_size", 1)

        total_params = (size_mb * 1024 * 1024) // 2
        self.in_features = 4096
        out_features = max(1, total_params // self.in_features)
        # Ensure out_features is divisible by tp_size
        if out_features % tp_size != 0:
            out_features = ((out_features // tp_size) + 1) * tp_size
        self.out_features = out_features

        self.layer = nn.Linear(
            self.in_features, self.out_features, bias=False, dtype=torch.bfloat16
        )
        self.layer.weight.requires_grad = True

    def forward(self, x):
        return self.layer(x)

    def training_step(self, batch, batch_idx):
        loss = self(batch).sum()
        return loss

    def configure_optimizers(self):
        return torch.optim.AdamW(self.parameters(), lr=1e-3)

    def configure_model(self):
        from lightning.pytorch.strategies import ModelParallelStrategy

        if isinstance(self.trainer.strategy, ModelParallelStrategy):
            mesh = self.trainer.strategy.device_mesh

            # Apply TP if tensor_parallel_size > 1
            if (
                "tensor_parallel" in mesh.mesh_dim_names
                and mesh["tensor_parallel"].size() > 1
            ):
                from torch.distributed.tensor.parallel import (
                    ColwiseParallel,
                    parallelize_module,
                )

                tp_mesh = mesh["tensor_parallel"]
                parallelize_module(self.layer, tp_mesh, ColwiseParallel())

            # Apply FSDP2 (DP) to the sharded module
            if (
                "data_parallel" in mesh.mesh_dim_names
                and mesh["data_parallel"].size() > 1
            ):
                from torch.distributed.fsdp import fully_shard

                dp_mesh = mesh["data_parallel"]
                fully_shard(self.layer, mesh=dp_mesh)
                fully_shard(self, mesh=dp_mesh)


@dataclasses.dataclass
class WriteResult:
    durations: list
    extra_columns: dict = dataclasses.field(default_factory=dict)


import socket


def find_free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


from lightning.pytorch.strategies import ModelParallelStrategy


class CPUModelParallelStrategy(ModelParallelStrategy):
    @property
    def root_device(self) -> torch.device:
        if not torch.cuda.is_available():
            return torch.device("cpu")
        return super().root_device


def _rank_save(rank, world_size, port, prefix, params, q):
    # This runs in a subprocess. We use "gloo" for CPU distributed run.
    os.environ["MASTER_ADDR"] = "localhost"
    os.environ["MASTER_PORT"] = str(port)
    os.environ["GLOO_SOCKET_IFNAME"] = "lo"
    os.environ["WORLD_SIZE"] = str(world_size)
    os.environ["RANK"] = str(rank)
    os.environ["LOCAL_RANK"] = str(rank)

    dist.init_process_group("gloo", rank=rank, world_size=world_size)

    # Setup strategy
    if params.strategy in ("fsdp_sharded", "fsdp_full"):
        from lightning.pytorch.strategies import FSDPStrategy
        from torch.distributed.fsdp.fully_sharded_data_parallel import MixedPrecision

        state_dict_type = "sharded" if params.strategy == "fsdp_sharded" else "full"
        strategy_args = {
            "process_group_backend": "gloo",
            "state_dict_type": state_dict_type,
            "mixed_precision": MixedPrecision(
                param_dtype=torch.bfloat16,
                reduce_dtype=torch.bfloat16,
                buffer_dtype=torch.bfloat16,
            ),
        }
        strategy = FSDPStrategy(**strategy_args)
    elif params.strategy in ("model_parallel_full", "model_parallel_sharded"):
        strategy = CPUModelParallelStrategy(
            tensor_parallel_size=params.tensor_parallel_size,
            data_parallel_size=params.data_parallel_size,
            save_distributed_checkpoint=(params.strategy == "model_parallel_sharded"),
            process_group_backend="gloo",
        )
    elif params.strategy == "ddp":
        from lightning.pytorch.strategies import DDPStrategy

        strategy = DDPStrategy(
            find_unused_parameters=False,
            process_group_backend="gloo",
        )
    else:
        raise ValueError(f"Unknown strategy: {params.strategy}")

    trainer_args = {
        "default_root_dir": prefix,
        "accelerator": "cpu",
        "devices": world_size,
        "strategy": strategy,
        "max_steps": 1,
        "precision": "bf16-mixed",
    }

    trainer = L.Trainer(**trainer_args)

    dataset = DummyDataset(in_features=4096)
    dataloader = DataLoader(dataset, batch_size=2)
    model = DummyModel(params)
    model = model.to(torch.bfloat16)

    # Trigger save checkpoint
    trainer.fit(model, train_dataloaders=dataloader)

    # Save checkpoint manually
    # We want to measure the save_checkpoint time.
    # In distributed strategies, we must save on all ranks (it is a collective call).
    ckpt_path = os.path.join(prefix, "model.ckpt")

    # Ensure all ranks are synchronized before starting the timer
    dist.barrier()
    t_start = time.perf_counter()
    trainer.save_checkpoint(ckpt_path)
    # Ensure all ranks finished writing before stopping the timer
    dist.barrier()
    t_end = time.perf_counter()

    dist.destroy_process_group()

    # Report durations from all ranks to aggregate
    q.put([(t_start, t_end)])


def run_split_save(prefix, params):
    """Spawns processes to run the distributed benchmark and gathers timing."""
    import torch.multiprocessing as mp

    ctx = mp.get_context("spawn")
    world_size = 2 if not hasattr(params, "world_size") else params.world_size
    world_size = min(world_size, 8)  # Increased cap to 8 to support TP=4, DP=2
    port = find_free_port()

    with ctx.Manager() as manager:
        q = manager.Queue()
        mp.spawn(
            _rank_save,
            args=(world_size, port, prefix, params, q),
            nprocs=world_size,
            join=True,
        )
        results = [q.get() for _ in range(world_size)]

    # We reduce across ranks.
    # durations = max(end) - min(begin) per epoch.
    begins = [res[0][0] for res in results]
    ends = [res[0][1] for res in results]
    durations = [max(ends) - min(begins)]
    return durations


class PLCheckpointDriver:
    """Driver for PyTorch Lightning checkpoint save benchmarks."""

    def run_save(self, prefix, params):
        if params.strategy in (
            "ddp",
            "fsdp_sharded",
            "fsdp_full",
            "model_parallel_full",
            "model_parallel_sharded",
        ):
            durations = run_split_save(prefix, params)
            return WriteResult(durations=durations)

        # Single device save
        model = DummyModel(params)
        model = model.to(torch.bfloat16)

        trainer_args = {
            "default_root_dir": prefix,
            "accelerator": "cpu",
            "devices": 1,
            "max_steps": 1,
            "precision": "bf16-mixed",
        }

        trainer = L.Trainer(**trainer_args)

        dataset = DummyDataset(in_features=4096)
        dataloader = DataLoader(dataset, batch_size=1)
        trainer.fit(model, train_dataloaders=dataloader)

        filepath = os.path.join(prefix, "model.ckpt")
        begin = time.perf_counter()
        trainer.save_checkpoint(filepath)
        end = time.perf_counter()
        durations = [end - begin]

        return WriteResult(durations=durations)
