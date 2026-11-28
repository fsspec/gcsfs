import dataclasses
from typing import Optional

from gcsfs.tests.perf.subsystembenchmarks._common import accelerator
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.configurator import (
    _BUCKET,
    CheckpointParameters,
    _model_id_slug,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.layout import (
    STRATEGY_MESH_DIMS,
    Layout,
)

__all__ = ["STRATEGY_MESH_DIMS", "PyTorchCheckpointParameters"]


@dataclasses.dataclass
class PyTorchCheckpointParameters(CheckpointParameters):
    """Parameters for a native PyTorch checkpoint benchmark case."""

    # DCP cases wider than one node run across `ceil(world_size / devices_per_node)`
    # nodes with --nodes; other groups are single-node only.
    MULTI_NODE = True

    data_parallel_replicate_size: int = 1
    pipeline_parallel_size: int = 1
    expert_parallel_size: int = 1
    source: Optional[dict] = None

    def __post_init__(self):
        self._has_setup_keys = any(
            v is not None
            for v in (
                self.setup_world_size,
                self.setup_tensor_parallel_size,
                self.setup_data_parallel_size,
            )
        )
        super().__post_init__()

    def extra_columns(self):
        cols = super().extra_columns()
        src = self.source_layout()
        cols.update(
            {
                "device": accelerator.device_kind(),
                "data_parallel_replicate_size": self.data_parallel_replicate_size,
                "pipeline_parallel_size": self.pipeline_parallel_size,
                "expert_parallel_size": self.expert_parallel_size,
                "source_strategy": src.strategy,
                "setup_world_size": src.world_size,
                "setup_tensor_parallel_size": src.tensor_parallel_size,
                "setup_data_parallel_size": src.data_parallel_size,
                "setup_data_parallel_replicate_size": src.data_parallel_replicate_size,
                "setup_pipeline_parallel_size": src.pipeline_parallel_size,
                "setup_expert_parallel_size": src.expert_parallel_size,
            }
        )
        return cols

    def target_layout(self) -> Layout:
        return Layout(
            strategy=self.strategy,
            world_size=self.world_size,
            data_parallel_replicate_size=self.data_parallel_replicate_size,
            data_parallel_size=self.data_parallel_size,
            tensor_parallel_size=self.tensor_parallel_size,
            pipeline_parallel_size=self.pipeline_parallel_size,
            expert_parallel_size=self.expert_parallel_size,
        )

    def source_layout(self) -> Layout:
        if self.source is None:
            return self.target_layout()
        return Layout.from_mapping(self.source)

    def max_world_size(self) -> int:
        return max(self.target_layout().world_size, self.source_layout().world_size)

    def benchmark_name(self):
        op = "load" if "read" in self.scenario else "save"
        target = self.target_layout()
        source = self.source_layout()
        parts = [
            op,
            _model_id_slug(self.model_id),
            target.token(),
        ]
        if source != target:
            parts.append(f"from-{source.token()}")
        parts.append(_BUCKET[self.bucket_type])
        return "-".join(parts)
