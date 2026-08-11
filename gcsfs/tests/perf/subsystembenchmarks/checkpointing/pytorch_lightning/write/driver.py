import dataclasses
import os
import time

import lightning.pytorch as L
import torch
import torch.distributed as dist
import torch.nn as nn
import transformers
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


def _llama_tp_plan():
    from torch.distributed.tensor.parallel import ColwiseParallel, RowwiseParallel

    return {
        "self_attn.q_proj": ColwiseParallel(),
        "self_attn.k_proj": ColwiseParallel(),
        "self_attn.v_proj": ColwiseParallel(),
        "self_attn.o_proj": RowwiseParallel(),
        "mlp.gate_proj": ColwiseParallel(),
        "mlp.up_proj": ColwiseParallel(),
        "mlp.down_proj": RowwiseParallel(),
    }


class DummyModel(L.LightningModule):
    """A dummy LightningModule."""

    def __init__(self, params):
        super().__init__()
        self.params = params

        model_id = params.model_id
        use_local_files_only = False
        if model_id.startswith("gs://"):
            use_local_files_only = True
            dir_name = os.path.basename(model_id.rstrip("/"))
            model_id = os.path.join("/tmp", dir_name)

        self.llama = transformers.AutoModelForCausalLM.from_pretrained(
            model_id,
            torch_dtype=torch.bfloat16,
            local_files_only=use_local_files_only,
            use_cache=False,
        )

        # Freeze the giant model's parameters to avoid CPU compute and OOMs.
        for p in self.llama.parameters():
            p.requires_grad = False

        # A tiny trainable layer to satisfy Lightning/PyTorch optimizer and backward requirements.
        self.trainable = nn.Linear(8, 8, dtype=torch.bfloat16)
        self.trainable.weight.requires_grad = True

    def forward(self, x):
        # We don't forward the giant model on CPU.
        return x

    def training_step(self, batch, batch_idx):
        # Discard batch to avoid CPU compute.
        del batch
        # Real loss with a real grad path so backward + DDP all-reduce run on the trainable layer.
        zeros = torch.zeros(1, 8, dtype=self.trainable.weight.dtype)
        return (self.trainable(zeros) ** 2).sum()

    @staticmethod
    def _materialize_adamw_state(optimizer):
        """Eagerly allocate AdamW moments so checkpoint size is realistic."""
        for group in optimizer.param_groups:
            for p in group["params"]:
                state = optimizer.state[p]
                if state:
                    continue
                state["step"] = torch.zeros((), dtype=torch.float32)
                state["exp_avg"] = torch.randn_like(
                    p, memory_format=torch.preserve_format
                )
                state["exp_avg_sq"] = torch.rand_like(
                    p, memory_format=torch.preserve_format
                )
                if group["amsgrad"]:
                    state["max_exp_avg_sq"] = torch.rand_like(
                        p, memory_format=torch.preserve_format
                    )

    def configure_optimizers(self):
        # Pass all parameters (including the frozen giant model) to the optimizer
        # so that it generates states for all of them, making the checkpoint realistic.
        optimizer = torch.optim.AdamW(self.parameters(), lr=1e-3)
        # Eagerly materialize states on CPU
        self._materialize_adamw_state(optimizer)
        return optimizer

    def configure_model(self):
        # Apply FSDP1 wrapping block-by-block
        if self.params.strategy in ("fsdp_sharded", "fsdp_full"):
            from torch.distributed.fsdp import FullyShardedDataParallel as FSDP
            from torch.distributed.fsdp.wrap import wrap

            if not isinstance(self.llama.model, FSDP):
                for index, layer in enumerate(self.llama.model.layers):
                    self.llama.model.layers[index] = wrap(layer)
                self.llama = wrap(self.llama)
            if not isinstance(self.trainable, FSDP):
                self.trainable = wrap(self.trainable)

        # Apply FSDP2 (ModelParallelStrategy) wrapping block-by-block + TP
        elif self.params.strategy in ("model_parallel_sharded", "model_parallel_full"):
            from torch.distributed.fsdp import fully_shard
            from torch.distributed.tensor.parallel import parallelize_module

            mesh = self.trainer.strategy.device_mesh
            tp_size = self.params.tensor_parallel_size

            # Apply TP to layers
            if tp_size > 1 and mesh["tensor_parallel"].size() > 1:
                tp_mesh = mesh["tensor_parallel"]
                for layer in self.llama.model.layers:
                    parallelize_module(layer, tp_mesh, _llama_tp_plan())

            # Apply FSDP2 (DP)
            dp_mesh = mesh["data_parallel"]
            for layer in self.llama.model.layers:
                fully_shard(layer, mesh=dp_mesh)
            fully_shard(self.llama, mesh=dp_mesh)


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

    # We reduce across ranks for each round.
    durations = []
    for r in range(params.rounds):
        begins = [results[rank][r][0] for rank in range(world_size)]
        ends = [results[rank][r][1] for rank in range(world_size)]
        durations.append(max(ends) - min(begins))
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

        return WriteResult(durations=durations)
