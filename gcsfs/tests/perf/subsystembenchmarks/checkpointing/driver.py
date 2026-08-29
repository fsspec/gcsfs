import dataclasses
from typing import Protocol, runtime_checkable


@dataclasses.dataclass
class CheckpointResult:
    """Container for driver run metrics returned to checkpoint_case runner."""

    durations: list
    extra_columns: dict = dataclasses.field(default_factory=dict)


@runtime_checkable
class CheckpointDriver(Protocol):
    """Driver interface for checkpointing benchmarks."""

    def setup(self, prefix: str, params) -> None:
        """Optional setup phase executed before the monitored run block."""
        ...

    def run(self, prefix: str, params) -> CheckpointResult:
        """Run the checkpoint scenario and return durations."""
        ...
