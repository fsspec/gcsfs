from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.configs import (
    PyTorchCheckpointConfigurator,
)


class PyTorchReshardConfigurator(PyTorchCheckpointConfigurator):
    FRAMEWORK = "pytorch_reshard"
    REQUIRES_SOURCE = True
