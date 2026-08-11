import dataclasses

from gcsfs.tests.perf.subsystembenchmarks.checkpointing.configurator import (
    CheckpointParameters,
)


@dataclasses.dataclass
class PLCheckpointParameters(CheckpointParameters):
    """Parameters for a PyTorch Lightning checkpoint save benchmark case."""

    pass
