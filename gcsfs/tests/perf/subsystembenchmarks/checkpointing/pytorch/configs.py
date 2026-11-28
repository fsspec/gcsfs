from gcsfs.tests.perf.subsystembenchmarks.checkpointing.configurator import (
    OneFactorCheckpointConfigurator,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.parameters import (
    PyTorchCheckpointParameters,
)


class PyTorchCheckpointConfigurator(OneFactorCheckpointConfigurator):
    PARAMS_CLASS = PyTorchCheckpointParameters
    REQUIRES_SOURCE: bool = False

    def validate_case(self, p):
        setup = (
            p.setup_world_size,
            p.setup_tensor_parallel_size,
            p.setup_data_parallel_size,
        )
        if getattr(p, "_has_setup_keys", False) or setup != (
            p.world_size,
            p.tensor_parallel_size,
            p.data_parallel_size,
        ):
            raise ValueError("set the saved layout with source:, not setup_* keys")
        if self.REQUIRES_SOURCE:
            if p.source is None:
                raise ValueError(
                    f"{type(self).__name__} requires source: on every case"
                )
        elif p.source is not None:
            raise ValueError(f"{type(self).__name__} does not accept source: on cases")
        target = p.target_layout()
        source = p.source_layout()
        target.validate()
        source.validate()
        if self.REQUIRES_SOURCE and source == target:
            raise ValueError(
                f"{type(self).__name__} requires source layout != target layout, "
                f"got {target}"
            )
        super().validate_case(p)
