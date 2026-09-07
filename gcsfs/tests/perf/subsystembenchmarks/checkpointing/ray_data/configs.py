from gcsfs.tests.perf.subsystembenchmarks.checkpointing.configurator import (
    OneFactorCheckpointConfigurator,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.ray_data.parameters import (
    RayCheckpointParameters,
)


class RayCheckpointConfigurator(OneFactorCheckpointConfigurator):
    FRAMEWORK = "ray_data"
    PARAMS_CLASS = RayCheckpointParameters
