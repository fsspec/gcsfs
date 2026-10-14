from gcsfs.tests.perf.subsystembenchmarks.checkpointing._dist import MAX_RANKS
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.configurator import (
    OneFactorCheckpointConfigurator,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.parameters import (
    STRATEGY_MESH_DIMS,
    PyTorchCheckpointParameters,
)


class PyTorchCheckpointConfigurator(OneFactorCheckpointConfigurator):
    FRAMEWORK = "pytorch"
    PARAMS_CLASS = PyTorchCheckpointParameters

    def validate_case(self, p):
        super().validate_case(p)
        if p.strategy not in STRATEGY_MESH_DIMS:
            raise ValueError(
                f"Unsupported strategy {p.strategy!r} for framework {self.FRAMEWORK!r}. "
                f"Allowed strategies: {sorted(STRATEGY_MESH_DIMS)}"
            )
        dims = tuple(name for name, _ in p.mesh_dims())
        if dims != STRATEGY_MESH_DIMS[p.strategy]:
            raise ValueError(
                f"Strategy {p.strategy!r} needs non-unit sizes for exactly "
                f"{STRATEGY_MESH_DIMS[p.strategy]}, got {dims}"
            )
        mesh_product = (
            p.data_parallel_replicate_size
            * p.data_parallel_size
            * p.tensor_parallel_size
            * p.pipeline_parallel_size
        )
        if mesh_product != p.world_size:
            raise ValueError(
                f"Mesh product (dpr={p.data_parallel_replicate_size} * "
                f"dp={p.data_parallel_size} * tp={p.tensor_parallel_size} * "
                f"pp={p.pipeline_parallel_size}) = {mesh_product} "
                f"must equal world_size={p.world_size}"
            )
        # EP is not a mesh factor: it reuses the dp_shard ranks.
        ep_size = p.data_parallel_size if p.strategy == "ep" else 1
        if p.expert_parallel_size != ep_size:
            raise ValueError(
                f"expert_parallel_size={p.expert_parallel_size} must be {ep_size} "
                f"for strategy {p.strategy!r}"
            )
        # run_split clamps to MAX_RANKS; fail here rather than in
        # init_device_mesh after the per-case bucket exists.
        if p.world_size > MAX_RANKS:
            raise ValueError(f"world_size={p.world_size} exceeds MAX_RANKS={MAX_RANKS}")
