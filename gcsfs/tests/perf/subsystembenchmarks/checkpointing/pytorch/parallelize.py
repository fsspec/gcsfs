import torch.nn as nn
from torch.distributed.device_mesh import DeviceMesh, init_device_mesh
from torch.distributed.fsdp import fully_shard
from torch.distributed.tensor import Shard

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


def _experts_modules(model: nn.Module) -> list[nn.Module]:
    """Each MoE layer's experts module (transformers>=5 OlmoeExperts and the like),
    which holds every expert of the layer in 3-D [num_experts, ...] weights."""
    return [
        layer.mlp.experts
        for layer in model.model.layers.children()
        if hasattr(getattr(layer, "mlp", None), "experts")
    ]


def check_experts_split(model: nn.Module, num_ranks: int) -> None:
    """EP gives each rank a contiguous run of whole experts, so the expert count
    must divide evenly across the ranks."""
    for experts in _experts_modules(model):
        if experts.num_experts % num_ranks:
            raise ValueError(
                f"{experts.num_experts} experts do not split into {num_ranks} ranks"
            )


def _expert_placement_fn(model: nn.Module, strategy: str):
    """FSDP placement for the fused expert weights; None keeps FSDP's Shard(0).

    Dim 0 of a fused expert weight indexes the experts. EP keeps Shard(0), so each
    rank owns whole experts; every other strategy shards dim 1 instead, slicing
    each expert across the dp ranks as FSDP did with transformers<5's per-expert
    2-D weights.
    """
    if strategy == "ep":
        return None
    expert_params = {p for m in _experts_modules(model) for p in m.parameters()}
    if not expert_params:
        return None
    return lambda p: Shard(1) if p in expert_params else None


def parallelize(model: nn.Module, mesh: DeviceMesh, params) -> nn.Module:
    if params.strategy not in STRATEGY_MESH_DIMS:
        raise ValueError(f"Unsupported strategy {params.strategy!r}")
    dims = STRATEGY_MESH_DIMS[params.strategy]
    if "pp" in dims:
        split_stage(model, mesh["pp"].get_local_rank(), mesh["pp"].size())
    if "tp" in dims:
        apply_llama_tp(model, mesh["tp"])
    if params.strategy == "ep":
        # EP reuses the dp_shard ranks.
        check_experts_split(model, mesh["dp_shard"].size())

    dp_dims = tuple(d for d in dims if d.startswith("dp_"))
    if dp_dims:
        dp_mesh = mesh[dp_dims]
        placement_fn = _expert_placement_fn(model, params.strategy)
        for layer in model.model.layers.children():
            fully_shard(layer, mesh=dp_mesh, shard_placement_fn=placement_fn)
        fully_shard(model, mesh=dp_mesh)
    return model
