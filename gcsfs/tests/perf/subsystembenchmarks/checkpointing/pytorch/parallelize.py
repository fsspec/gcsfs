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


def split_stage(model: nn.Module, stage: int, num_stages: int) -> None:
    if model.config.tie_word_embeddings:
        raise ValueError("split_stage does not support tie_word_embeddings")
    layers = model.model.layers
    if len(layers) % num_stages:
        raise ValueError(f"{len(layers)} layers do not split into {num_stages} stages")
    per_stage = len(layers) // num_stages
    begin = stage * per_stage
    # Keyed by the original index so FQNs stay canonical, e.g. model.layers.16.
    model.model.layers = nn.ModuleDict(
        {str(i): layers[i] for i in range(begin, begin + per_stage)}
    )
    if stage != 0:
        model.model.embed_tokens = None
    if stage != num_stages - 1:
        model.model.norm = None
        model.lm_head = None


def parallelize(model: nn.Module, mesh: DeviceMesh, params) -> nn.Module:
    if params.strategy not in STRATEGY_MESH_DIMS:
        raise ValueError(f"Unsupported strategy {params.strategy!r}")
    dims = STRATEGY_MESH_DIMS[params.strategy]
    if "pp" in dims:
        split_stage(model, mesh["pp"].get_local_rank(), mesh["pp"].size())
    if "tp" in dims:
        apply_llama_tp(model, mesh["tp"])

    dp_dims = tuple(d for d in dims if d.startswith("dp_"))
    if dp_dims:
        dp_mesh = mesh[dp_dims]
        for layer in model.model.layers.children():
            fully_shard(layer, mesh=dp_mesh)
        fully_shard(model, mesh=dp_mesh)
    return model
