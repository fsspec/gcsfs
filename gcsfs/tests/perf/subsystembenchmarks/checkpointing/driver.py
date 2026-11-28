import dataclasses
from typing import Callable, Optional, Protocol, runtime_checkable


@dataclasses.dataclass
class ResourceSnapshot:
    """Resource metrics snapshot matching publish_resource_metrics attributes."""

    max_cpu: float
    max_mem: float
    net_recv: float
    net_sent: float
    duration: float
    vcpus: int


@dataclasses.dataclass
class CheckpointResult:
    """Container for driver run metrics returned to checkpoint_case runner."""

    durations: list
    extra_columns: dict = dataclasses.field(default_factory=dict)
    # Called by the runner after the monitored window closes; raises if the
    # run produced wrong results. Keeps post-run checks out of the window.
    verify: Optional[Callable[[], None]] = None
    # Idempotent teardown hook called unconditionally by the runner in finally:
    # to release spawned worker processes, SyncManager, and shared-memory tensors
    # even when the monitor context or verify() raises.
    close: Optional[Callable[[], None]] = None
    bytes_transferred: Optional[int] = None
    window: Optional[tuple[float, float]] = None
    resource_metrics: Optional[object] = None


@runtime_checkable
class CheckpointDriver(Protocol):
    """Driver interface for checkpointing benchmarks."""

    def setup(self, prefix: str, params) -> None:
        """Optional setup phase executed before the monitored run block."""
        pass

    def run(self, prefix: str, params) -> CheckpointResult:
        """Run the checkpoint scenario and return durations."""
        pass

    def read_count(self, params) -> int:
        """Returns how many times the checkpoint is transferred from storage across ranks."""
        return 1

    def is_checkpoint_file(self, path: str) -> bool:
        """Returns True if the file path is part of the physical checkpoint."""
        return "model.ckpt" in path
