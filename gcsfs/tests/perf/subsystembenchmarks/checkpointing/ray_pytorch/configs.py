from gcsfs.tests.perf.subsystembenchmarks.checkpointing.configurator import (
    OneFactorCheckpointConfigurator,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.ray_pytorch.parameters import (
    RayCheckpointParameters,
)


class RayCheckpointConfigurator(OneFactorCheckpointConfigurator):
    FRAMEWORK = "ray_pytorch"
    PARAMS_CLASS = RayCheckpointParameters
