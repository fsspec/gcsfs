import dataclasses
import os

from gcsfs.tests.perf.subsystembenchmarks._common.config_loader import (
    OneFactorConfigurator,
)

_BUCKET = {"regional": "reg", "zonal": "zon", "hns": "hns"}
_STRATEGY = {
    "single": "sgl",
    "ddp": "ddp",
    "fsdp_sharded": "fsdp-shd",
    "fsdp_full": "fsdp-full",
    "model_parallel_full": "mp-full",
    "model_parallel_sharded": "mp-shd",
}

_RUN_LEVEL_KEYS = ("bucket_type",)


def _model_id_slug(model_id: str) -> str:
    name = os.path.basename(model_id.rstrip("/"))
    return name.lower().replace("-", "_").replace(".", "_")


@dataclasses.dataclass
class CheckpointParameters:
    """Parameters for a checkpoint save benchmark case."""

    name: str
    bucket_name: str
    bucket_type: str
    rounds: int
    scenario: str
    framework: str

    model_id: str
    strategy: str  # single, ddp, fsdp, model_parallel_*
    world_size: int = 2
    tensor_parallel_size: int = 1
    data_parallel_size: int = 1
    sweep_axis: str = "baseline"

    def extra_columns(self):
        """Loader-specific CSV columns."""
        return {}

    def benchmark_name(self):
        """Stable, param-encoding pytest-benchmark id using swept values."""
        parts = [
            "save",
            _model_id_slug(self.model_id),
            _STRATEGY[self.strategy],
        ]
        if self.strategy in ("model_parallel_full", "model_parallel_sharded"):
            parts.append(f"tp{self.tensor_parallel_size}dp{self.data_parallel_size}")
        parts.append(_BUCKET[self.bucket_type])
        return "-".join(parts)


class OneFactorCheckpointConfigurator(OneFactorConfigurator):
    """Checkpointing-family pins for generic one-factor config mechanics."""

    RUN_LEVEL_KEYS = _RUN_LEVEL_KEYS

    def shared_keys(self, scenario, common_config):
        return dict(
            rounds=common_config.get("rounds", 1),
            scenario=scenario["scenario"],
            bucket_type=os.environ.get("GCSFS_SUBSYSTEM_BUCKET_TYPE", "regional"),
        )

    def validate_case(self, p):
        if p.rounds < 1:
            raise ValueError(
                f"rounds must be >= 1, got {p.rounds!r} for case {p.benchmark_name()!r}"
            )
