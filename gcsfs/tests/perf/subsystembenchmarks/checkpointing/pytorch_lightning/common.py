from __future__ import annotations

import dataclasses
import json
import os
import tempfile

import lightning.pytorch as L
import torch
import torch.nn as nn
import transformers
from lightning.pytorch.strategies import FSDPStrategy, ModelParallelStrategy
from torch.utils.data import Dataset

from gcsfs.tests.perf.subsystembenchmarks._common import accelerator, launcher
from gcsfs.tests.perf.subsystembenchmarks.checkpointing._dist import MAX_RANKS
from gcsfs.tests.perf.subsystembenchmarks.checkpointing._llama_tp import apply_llama_tp
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.model_staging import model_dir

_WORKER_MODULE = (
    "gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch_lightning._worker"
)


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
    """A dummy LightningModule."""

    def __init__(self, params):
        super().__init__()
        self.params = params

        model_id = params.model_id
        use_local_files_only = model_id.startswith("gs://")
        resolved_model = model_dir(model_id)

        self.llama = transformers.AutoModelForCausalLM.from_pretrained(
            resolved_model,
            dtype=torch.bfloat16,
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
        # Discard batch to avoid compute.
        del batch
        # Real loss with a real grad path so backward + DDP all-reduce run on the trainable layer.
        zeros = torch.zeros(
            1,
            8,
            dtype=self.trainable.weight.dtype,
            device=self.trainable.weight.device,
        )
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
        # Eagerly materialize states
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

            mesh = self.trainer.strategy.device_mesh

            # Apply TP to layers
            if (
                "tensor_parallel" in mesh.mesh_dim_names
                and mesh["tensor_parallel"].size() > 1
            ):
                apply_llama_tp(self.llama, mesh["tensor_parallel"])

            # Apply FSDP2 (DP)
            if "data_parallel" in mesh.mesh_dim_names:
                dp_mesh = mesh["data_parallel"]
                for layer in self.llama.model.layers:
                    fully_shard(layer, mesh=dp_mesh)
                fully_shard(self.llama, mesh=dp_mesh)


class HostAwareModelParallelStrategy(ModelParallelStrategy):
    @property
    def root_device(self) -> torch.device:
        if accelerator.device_kind() == "cpu":
            return torch.device("cpu")
        return super().root_device


class HostAwareFSDPStrategy(FSDPStrategy):
    @property
    def root_device(self) -> torch.device:
        if accelerator.device_kind() == "cpu":
            return torch.device("cpu")
        return super().root_device


def trainer_device_args(world_size: int) -> dict:
    """Return Lightning Trainer device kwargs for CPU or CUDA GPU."""
    kind = accelerator.device_kind()
    if kind == "cpu":
        return {"accelerator": "cpu", "devices": world_size}
    if kind == "cuda":
        return {"accelerator": "gpu", "devices": world_size}
    raise ValueError(f"PyTorch Lightning does not support device kind {kind!r}")


def is_distributed_strategy(strategy: str) -> bool:
    return strategy != "single"


def get_strategy(params, setup=False):
    strategy_name = params.strategy
    tp_size = params.tensor_parallel_size
    dp_size = params.data_parallel_size

    if setup:
        tp_size = getattr(params, "setup_tensor_parallel_size", None) or tp_size
        dp_size = getattr(params, "setup_data_parallel_size", None) or dp_size

    pg_backend = accelerator.backend_name()

    if strategy_name in ("fsdp_sharded", "fsdp_full"):
        from torch.distributed.fsdp.fully_sharded_data_parallel import MixedPrecision

        state_dict_type = "sharded" if strategy_name == "fsdp_sharded" else "full"
        return HostAwareFSDPStrategy(
            process_group_backend=pg_backend,
            state_dict_type=state_dict_type,
            mixed_precision=MixedPrecision(
                param_dtype=torch.bfloat16,
                reduce_dtype=torch.bfloat16,
                buffer_dtype=torch.bfloat16,
            ),
        )
    elif strategy_name in ("model_parallel_full", "model_parallel_sharded"):
        return HostAwareModelParallelStrategy(
            tensor_parallel_size=tp_size,
            data_parallel_size=dp_size,
            save_distributed_checkpoint=(strategy_name == "model_parallel_sharded"),
            process_group_backend=pg_backend,
        )
    elif strategy_name == "ddp":
        from lightning.pytorch.strategies import DDPStrategy

        return DDPStrategy(
            find_unused_parameters=False,
            process_group_backend=pg_backend,
        )
    else:
        raise ValueError(f"Unknown strategy: {strategy_name}")


def run_worker(
    op: str,
    prefix: str,
    params,
    *,
    world_size: int,
    rdzv_id: str,
    expect_result: bool = False,
) -> dict | None:
    """Launch ``pytorch_lightning._worker`` via single-node ``torchrun``."""
    effective_ws = (
        min(world_size, MAX_RANKS) if accelerator.device_kind() == "cpu" else world_size
    )
    with tempfile.TemporaryDirectory(prefix="ssb-pl-") as tmp_dir:
        spec_file = os.path.join(tmp_dir, "spec.json")
        result_file = os.path.join(tmp_dir, "result.json")
        with open(spec_file, "w", encoding="utf-8") as f:
            json.dump({"prefix": prefix, "params": dataclasses.asdict(params)}, f)
        args = ["--op", op, "--spec-file", spec_file]
        if expect_result:
            args.extend(["--result-file", result_file])
        launcher.run_torchrun(
            _WORKER_MODULE,
            args,
            world_size=effective_ws,
            rdzv_id=rdzv_id,
        )
        if not expect_result:
            return None
        with open(result_file, encoding="utf-8") as f:
            return json.load(f)
