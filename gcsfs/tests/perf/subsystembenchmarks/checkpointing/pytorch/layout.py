import dataclasses
from collections.abc import Mapping
from typing import Any

from gcsfs.tests.perf.subsystembenchmarks.checkpointing.configurator import _STRATEGY

# Non-unit mesh dims each strategy uses, in mesh order; every other size must be 1.
STRATEGY_MESH_DIMS: dict[str, tuple[str, ...]] = {
    "fsdp": ("dp_shard",),
    "hsdp": ("dp_replicate", "dp_shard"),
    "tp": ("tp",),
    "fsdp_tp": ("dp_shard", "tp"),
    "hsdp_tp": ("dp_replicate", "dp_shard", "tp"),
    "pp": ("pp",),
    "pp_fsdp": ("pp", "dp_shard"),
    "pp_fsdp_tp": ("pp", "dp_shard", "tp"),
    "pp_hsdp_tp": ("pp", "dp_replicate", "dp_shard", "tp"),
    "ep": ("dp_shard",),
}

_SIZE_FIELDS = (
    "world_size",
    "data_parallel_replicate_size",
    "data_parallel_size",
    "tensor_parallel_size",
    "pipeline_parallel_size",
    "expert_parallel_size",
)


@dataclasses.dataclass(frozen=True)
class Layout:
    strategy: str
    world_size: int
    data_parallel_replicate_size: int = 1
    data_parallel_size: int = 1
    tensor_parallel_size: int = 1
    pipeline_parallel_size: int = 1
    expert_parallel_size: int = 1

    @classmethod
    def from_mapping(cls, m: Mapping[str, Any]) -> "Layout":
        if not isinstance(m, Mapping):
            raise ValueError(f"layout must be a mapping, got {type(m).__name__}")
        unknown = sorted(set(m) - {f.name for f in dataclasses.fields(cls)}, key=str)
        if unknown:
            raise ValueError(f"Unknown layout keys: {unknown}")
        missing = sorted({"strategy", "world_size"} - set(m))
        if missing:
            raise ValueError(f"Missing required layout keys: {missing}")
        return cls(**m)

    def mesh_dims(self) -> list[tuple[str, int]]:
        dims = [
            ("pp", self.pipeline_parallel_size),
            ("dp_replicate", self.data_parallel_replicate_size),
            ("dp_shard", self.data_parallel_size),
            ("tp", self.tensor_parallel_size),
        ]
        return [(name, size) for name, size in dims if size > 1]

    def validate(self) -> None:
        if (
            not isinstance(self.strategy, str)
            or self.strategy not in STRATEGY_MESH_DIMS
        ):
            raise ValueError(
                f"Unsupported strategy {self.strategy!r} for framework 'pytorch'. "
                f"Allowed strategies: {sorted(STRATEGY_MESH_DIMS)}"
            )
        for field_name in _SIZE_FIELDS:
            val = getattr(self, field_name)
            if isinstance(val, bool) or not isinstance(val, int) or val < 1:
                raise ValueError(f"{field_name} must be an int >= 1, got {val!r}")
        dims = tuple(name for name, _ in self.mesh_dims())
        if dims != STRATEGY_MESH_DIMS[self.strategy]:
            raise ValueError(
                f"Strategy {self.strategy!r} needs non-unit sizes for exactly "
                f"{STRATEGY_MESH_DIMS[self.strategy]}, got {dims}"
            )
        mesh_product = (
            self.data_parallel_replicate_size
            * self.data_parallel_size
            * self.tensor_parallel_size
            * self.pipeline_parallel_size
        )
        if mesh_product != self.world_size:
            raise ValueError(
                f"Mesh product (dpr={self.data_parallel_replicate_size} * "
                f"dp={self.data_parallel_size} * tp={self.tensor_parallel_size} * "
                f"pp={self.pipeline_parallel_size}) = {mesh_product} "
                f"must equal world_size={self.world_size}"
            )
        # EP is not a mesh factor: it reuses the dp_shard ranks.
        ep_size = self.data_parallel_size if self.strategy == "ep" else 1
        if self.expert_parallel_size != ep_size:
            raise ValueError(
                f"expert_parallel_size={self.expert_parallel_size} must be {ep_size} "
                f"for strategy {self.strategy!r}"
            )

    def token(self) -> str:
        parts = [_STRATEGY[self.strategy]]
        if self.strategy != "fsdp":
            size_tokens = []
            if self.pipeline_parallel_size > 1:
                size_tokens.append(f"pp{self.pipeline_parallel_size}")
            if self.data_parallel_replicate_size > 1:
                size_tokens.append(f"dpr{self.data_parallel_replicate_size}")
            if self.data_parallel_size > 1:
                size_tokens.append(f"dp{self.data_parallel_size}")
            if self.tensor_parallel_size > 1:
                size_tokens.append(f"tp{self.tensor_parallel_size}")
            if self.expert_parallel_size > 1:
                size_tokens.append(f"ep{self.expert_parallel_size}")
            if size_tokens:
                parts.append("".join(size_tokens))
        if self.world_size > 1:
            parts.append(f"ws{self.world_size}")
        return "-".join(parts)
