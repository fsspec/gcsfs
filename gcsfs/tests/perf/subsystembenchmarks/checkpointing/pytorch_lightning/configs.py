from gcsfs.tests.perf.subsystembenchmarks.checkpointing.configurator import (
    OneFactorCheckpointConfigurator,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch_lightning.parameters import (
    PLCheckpointParameters,
)


class PyTorchLightningCheckpointConfigurator(OneFactorCheckpointConfigurator):
    FRAMEWORK = "pytorch_lightning"
    PARAMS_CLASS = PLCheckpointParameters
