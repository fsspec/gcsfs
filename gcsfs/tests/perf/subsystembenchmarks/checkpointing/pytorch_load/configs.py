from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.configs import (
    PyTorchCheckpointConfigurator,
)


class PyTorchLoadConfigurator(PyTorchCheckpointConfigurator):
    FRAMEWORK = "pytorch_load"
