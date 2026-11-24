from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.configs import (
    PyTorchCheckpointConfigurator,
)


class PyTorchSaveConfigurator(PyTorchCheckpointConfigurator):
    FRAMEWORK = "pytorch_save"
