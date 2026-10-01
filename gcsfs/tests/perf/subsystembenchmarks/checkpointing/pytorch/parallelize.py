import torch.nn as nn
from torch.distributed.device_mesh import DeviceMesh, init_device_mesh
from torch.distributed.fsdp import fully_shard

from gcsfs.tests.perf.subsystembenchmarks.checkpointing._llama_tp import apply_llama_tp
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.parameters import (
    STRATEGY_MESH_DIMS,
)


def build_mesh(params) -> DeviceMesh:
    dims = params.mesh_dims()
    return init_device_mesh(
        "cpu",
        tuple(size for _, size in dims),
        mesh_dim_names=tuple(name for name, _ in dims),
    )


def _keep(modules: nn.ModuleList, begin: int, count: int) -> nn.ModuleDict:
    # Keyed by the original index so FQNs stay canonical, e.g. model.layers.16.
    return nn.ModuleDict({str(i): modules[i] for i in range(begin, begin + count)})


def split_stage(model: nn.Module, stage: int, num_stages: int) -> None:
    """Keeps only this stage's modules. State-dict layout only: the ModuleDict
    below breaks HF forward, which iterates layers as a list."""
    if model.config.tie_word_embeddings:
        raise ValueError("split_stage does not support tie_word_embeddings")
    layers = model.model.layers
    if len(layers) % num_stages:
        raise ValueError(f"{len(layers)} layers do not split into {num_stages} stages")
    per_stage = len(layers) // num_stages
    model.model.layers = _keep(layers, stage * per_stage, per_stage)
    if stage != 0:
        model.model.embed_tokens = None
    if stage != num_stages - 1:
        model.model.norm = None
        model.lm_head = None


def keep_local_experts(model: nn.Module, rank: int, num_ranks: int) -> None:
    """Keeps this rank's contiguous expert slice per layer (assumes per-expert
    nn.ModuleList, i.e. transformers<5). State-dict layout only: breaks HF forward."""
    for layer in model.model.layers.children():
        experts = layer.mlp.experts
        if len(experts) % num_ranks:
            raise ValueError(
                f"{len(experts)} experts do not split into {num_ranks} ranks"
            )
        per_rank = len(experts) // num_ranks
        layer.mlp.experts = _keep(experts, rank * per_rank, per_rank)


def parallelize(model: nn.Module, mesh: DeviceMesh, params) -> nn.Module:
    if params.strategy not in STRATEGY_MESH_DIMS:
        raise ValueError(f"Unsupported strategy {params.strategy!r}")
    dims = STRATEGY_MESH_DIMS[params.strategy]
    if "pp" in dims:
        split_stage(model, mesh["pp"].get_local_rank(), mesh["pp"].size())
    if "tp" in dims:
        apply_llama_tp(model, mesh["tp"])
    if params.strategy == "ep":
        # EP reuses the dp_shard ranks; local experts stay whole plain tensors.
        dp_shard = mesh["dp_shard"]
        keep_local_experts(model, dp_shard.get_local_rank(), dp_shard.size())

    dp_dims = tuple(d for d in dims if d.startswith("dp_"))
    if dp_dims:
        dp_mesh = mesh[dp_dims]
        for layer in model.model.layers.children():
            ignored = set()
            if params.strategy == "ep":
                ignored = set(layer.mlp.experts.parameters())
            fully_shard(layer, mesh=dp_mesh, ignored_params=ignored)
        fully_shard(model, mesh=dp_mesh)
    return model
