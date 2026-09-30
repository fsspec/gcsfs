import dataclasses

from gcsfs.tests.perf.subsystembenchmarks.checkpointing.configurator import (
    _BUCKET,
    _STRATEGY,
    CheckpointParameters,
    _model_id_slug,
)

# Non-unit mesh dims each strategy uses, in mesh order; every other size must be 1.
STRATEGY_MESH_DIMS = {
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


@dataclasses.dataclass
class PyTorchCheckpointParameters(CheckpointParameters):
    """Parameters for a native PyTorch checkpoint load benchmark case."""

    data_parallel_replicate_size: int = 1
    pipeline_parallel_size: int = 1
    expert_parallel_size: int = 1

    def extra_columns(self):
        cols = super().extra_columns()
        cols.update(
            {
                "data_parallel_replicate_size": self.data_parallel_replicate_size,
                "pipeline_parallel_size": self.pipeline_parallel_size,
                "expert_parallel_size": self.expert_parallel_size,
            }
        )
        return cols

    def mesh_dims(self) -> list[tuple[str, int]]:
        dims = [
            ("pp", self.pipeline_parallel_size),
            ("dp_replicate", self.data_parallel_replicate_size),
            ("dp_shard", self.data_parallel_size),
            ("tp", self.tensor_parallel_size),
        ]
        return [(name, size) for name, size in dims if size > 1]

    def benchmark_name(self):
        op = "load" if "read" in self.scenario else "save"
        parts = [
            op,
            _model_id_slug(self.model_id),
            _STRATEGY[self.strategy],
        ]

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

        parts.append(_BUCKET[self.bucket_type])
        return "-".join(parts)
