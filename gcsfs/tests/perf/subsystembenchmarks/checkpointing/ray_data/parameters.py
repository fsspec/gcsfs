import dataclasses

from gcsfs.tests.perf.subsystembenchmarks.checkpointing.configurator import (
    CheckpointParameters,
)


@dataclasses.dataclass
class RayCheckpointParameters(CheckpointParameters):
    """Parameters for a Ray checkpoint benchmark case."""

    pass
