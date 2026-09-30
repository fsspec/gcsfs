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


def parallelize(model: nn.Module, mesh: DeviceMesh, params) -> nn.Module:
    if params.strategy not in STRATEGY_MESH_DIMS:
        raise ValueError(f"Unsupported strategy {params.strategy!r}")
    dims = STRATEGY_MESH_DIMS[params.strategy]
    if "tp" in dims:
        apply_llama_tp(model, mesh["tp"])

    dp_dims = tuple(d for d in dims if d.startswith("dp_"))
    if dp_dims:
        dp_mesh = mesh[dp_dims]
        for layer in model.model.layers:
            fully_shard(layer, mesh=dp_mesh)
        fully_shard(model, mesh=dp_mesh)
    return model
