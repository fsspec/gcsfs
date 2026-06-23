"""Self-contained metric dataclasses + raw-metric path constants.

Trimmed copies of the tessellations raw-metric schemas so the parser and
calculator carry no dependency on the tessellations package. Field order is
significant: CSV column order derives from it via ``fieldnames``.
"""

from dataclasses import dataclass

# Raw-metric layout (mirrors tessellations directory/file names so the parser
# and calculator agree on paths).
STEP_METRICS_DIRECTORY = "training_time"
STEP_METRICS_FILE = "step_time.csv"
WRITE_DURATION_DIRECTORY = "checkpoint_write_time"
RESTORE_DURATION_DIRECTORY = "checkpoint_restore_time"
DELETE_DURATION_DIRECTORY = "checkpoint_delete_time"
PERSISTENT_STORAGE_DIRECTORY = "persistent_storage"
PER_ACCELERATOR_DIRECTORY = "per_accelerator"
CALCULATED_METRICS_DIRECTORY = "calculated_metrics"
DATA_LOADING_METRICS_FILE = "data_loading_metrics.csv"


def fieldnames(dataclass_type) -> list:
    """CSV fieldnames for a dataclass, in declaration order."""
    return list(dataclass_type.__annotations__.keys())


@dataclass(kw_only=True)
class StepMetrics:
    step: int
    step_duration: float
    step_end_time: float = None


# The per-event durations are intentionally NOT stored: the calculators derive
# every duration from ``end_time - start_time`` (per group), so a separate
# ``*_duration`` column would be dead data. The parser still reads the duration
# from the log to compute ``end_time`` where the log carries only a duration.
@dataclass(kw_only=True)
class WriteDurationMetrics:
    checkpoint_step: float
    checkpoint_location: str
    start_time: float
    end_time: float
    global_rank: int = None
    local_rank: int = None


@dataclass(kw_only=True)
class RestoreDurationMetrics:
    # checkpoint_location is the path that was restored (captured by the parser),
    # so all ranks restoring one checkpoint share it and collapse into a single
    # distributed datapoint, while two distinct restores stay separate.
    checkpoint_step: float = None
    checkpoint_location: str
    start_time: float
    end_time: float
    global_rank: int = None
    local_rank: int = None


@dataclass(kw_only=True)
class DeleteDurationMetrics:
    checkpoint_step: float = None
    checkpoint_location: str
    start_time: float
    end_time: float
    global_rank: int = None
    local_rank: int = None


@dataclass
class DataLoadingMetrics:
    run_id: str
    epoch_idx: int = None
    accelerator_blocked_time: float = None
    accelerator_blocked_percent: float = None
    update_timestamp: str = None
