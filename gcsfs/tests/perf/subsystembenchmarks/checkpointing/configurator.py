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
    world_size: int = 1
    tensor_parallel_size: int = 1
    data_parallel_size: int = 1
    setup_world_size: int = None
    setup_tensor_parallel_size: int = None
    setup_data_parallel_size: int = None
    sweep_axis: str = "baseline"

    def __post_init__(self):
        if self.setup_world_size is None:
            self.setup_world_size = self.world_size
        if self.setup_tensor_parallel_size is None:
            self.setup_tensor_parallel_size = self.tensor_parallel_size
        if self.setup_data_parallel_size is None:
            self.setup_data_parallel_size = self.data_parallel_size

    def extra_columns(self):
        """Loader-specific CSV columns."""
        return {
            "world_size": self.world_size,
            "tensor_parallel_size": self.tensor_parallel_size,
            "data_parallel_size": self.data_parallel_size,
            "setup_world_size": self.setup_world_size,
            "setup_tensor_parallel_size": self.setup_tensor_parallel_size,
            "setup_data_parallel_size": self.setup_data_parallel_size,
        }

    def benchmark_name(self):
        """Stable, param-encoding pytest-benchmark id using swept values."""
        op = "load" if "read" in self.scenario else "save"
        parts = [
            op,
            _model_id_slug(self.model_id),
            _STRATEGY[self.strategy],
        ]

        # Encode setup topology if it differs from load topology (cross_size axis)
        setup_ws = getattr(self, "setup_world_size", None) or self.world_size
        setup_tp = (
            getattr(self, "setup_tensor_parallel_size", None)
            or self.tensor_parallel_size
        )
        setup_dp = (
            getattr(self, "setup_data_parallel_size", None) or self.data_parallel_size
        )

        if self.strategy in ("model_parallel_full", "model_parallel_sharded"):
            if (
                setup_tp != self.tensor_parallel_size
                or setup_dp != self.data_parallel_size
            ):
                parts.append(f"setup-tp{setup_tp}dp{setup_dp}")
            parts.append(f"tp{self.tensor_parallel_size}dp{self.data_parallel_size}")

        if setup_ws != self.world_size:
            parts.append(f"setup-ws{setup_ws}")
        if self.world_size > 1:
            parts.append(f"ws{self.world_size}")

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
            model_id=os.environ.get(
                "GCSFS_SUBSYSTEM_MODEL_ID", common_config.get("model_id")
            ),
        )

    def validate_case(self, p):
        if p.rounds < 1:
            raise ValueError(
                f"rounds must be >= 1, got {p.rounds!r} for case {p.benchmark_name()!r}"
            )
