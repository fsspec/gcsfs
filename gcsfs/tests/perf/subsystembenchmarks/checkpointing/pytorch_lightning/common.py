import os

import lightning.pytorch as L
import torch
import torch.nn as nn
import transformers
from lightning.pytorch.strategies import FSDPStrategy, ModelParallelStrategy
from torch.utils.data import Dataset

from gcsfs.tests.perf.subsystembenchmarks.checkpointing._llama_tp import apply_llama_tp


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


class CPUModelParallelStrategy(ModelParallelStrategy):
    @property
    def root_device(self) -> torch.device:
        if not torch.cuda.is_available():
            return torch.device("cpu")
        return super().root_device


class CPUFSDPStrategy(FSDPStrategy):
    @property
    def root_device(self) -> torch.device:
        if not torch.cuda.is_available():
            return torch.device("cpu")
        return super().root_device


def is_distributed_strategy(strategy: str) -> bool:
    return strategy != "single"


def get_strategy(params, setup=False):
    import torch

    strategy_name = params.strategy
    tp_size = params.tensor_parallel_size
    dp_size = params.data_parallel_size

    if setup:
        tp_size = getattr(params, "setup_tensor_parallel_size", None) or tp_size
        dp_size = getattr(params, "setup_data_parallel_size", None) or dp_size

    if strategy_name in ("fsdp_sharded", "fsdp_full"):
        from torch.distributed.fsdp.fully_sharded_data_parallel import MixedPrecision

        state_dict_type = "sharded" if strategy_name == "fsdp_sharded" else "full"
        return CPUFSDPStrategy(
            process_group_backend="gloo",
            state_dict_type=state_dict_type,
            mixed_precision=MixedPrecision(
                param_dtype=torch.bfloat16,
                reduce_dtype=torch.bfloat16,
                buffer_dtype=torch.bfloat16,
            ),
        )
    elif strategy_name in ("model_parallel_full", "model_parallel_sharded"):
        return CPUModelParallelStrategy(
            tensor_parallel_size=tp_size,
            data_parallel_size=dp_size,
            save_distributed_checkpoint=(strategy_name == "model_parallel_sharded"),
            process_group_backend="gloo",
        )
    elif strategy_name == "ddp":
        from lightning.pytorch.strategies import DDPStrategy

        return DDPStrategy(
            find_unused_parameters=False,
            process_group_backend="gloo",
        )
    else:
        raise ValueError(f"Unknown strategy: {strategy_name}")
