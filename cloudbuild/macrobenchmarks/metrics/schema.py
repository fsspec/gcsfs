"""Self-contained metric dataclasses + raw-metric path constants.

Field order is significant: CSV column order derives from it via
``fieldnames``.
"""

from dataclasses import dataclass

# Raw-metric layout (directory/file names the parser and calculator agree on).
STEP_METRICS_DIRECTORY = "training_time"
STEP_METRICS_FILE = "step_time.csv"
WRITE_DURATION_DIRECTORY = "checkpoint_write_time"
RESTORE_DURATION_DIRECTORY = "checkpoint_restore_time"
DELETE_DURATION_DIRECTORY = "checkpoint_delete_time"
PERSISTENT_STORAGE_DIRECTORY = "persistent_storage"
PER_ACCELERATOR_DIRECTORY = "per_accelerator"
CALCULATED_METRICS_DIRECTORY = "calculated_metrics"
DATA_LOADING_METRICS_FILE = "data_loading_metrics.csv"
CHECKPOINT_SIZE_DIRECTORY = "checkpoint_size"
CHECKPOINT_SIZE_FILE = "checkpoint_size.csv"
DATA_WAIT_DIRECTORY = "data_wait"
DATA_WAIT_METRICS_FILE = "data_wait_metrics.csv"
DATASET_BUILD_DIRECTORY = "dataset_build"
DATASET_BUILD_METRICS_FILE = "dataset_build_metrics.csv"
SYSTEM_METRICS_DIRECTORY = "system_metrics"
SYSTEM_METRICS_FILE = "system_metrics.csv"


def fieldnames(dataclass_type) -> list:
    """CSV fieldnames for a dataclass, in declaration order."""
    return list(dataclass_type.__annotations__.keys())


@dataclass(kw_only=True)
class StepMetrics:
    step: int
    step_duration: float
    step_end_time: float = None
    samples_per_second: float = None


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


@dataclass(kw_only=True)
class DataWaitMetrics:
    """One dataloader-blocking span from a real-time ``Data Wait`` log line.

    ``cumulative_total`` is the emitting rank's running total, monotonically
    increasing across its lines; the max observed value is the rank's total
    blocked time as of its last surviving line, robust to lost tail lines
    (unlike summing ``duration``).
    """

    global_rank: int
    fetch_index: int
    action: str
    duration: float
    cumulative_total: float


@dataclass(kw_only=True)
class DatasetBuildMetrics:
    """One rank's ``build_train_dataset`` duration.

    Covers the Parquet glob resolution and shuffle-buffer/node-sharding setup
    that runs once before ``trainer.fit`` starts -- outside DataWaitProfiler's
    ``data_wait_total_time`` span, which only begins once the fit loop is running.
    """

    global_rank: int
    duration: float
    dataset_path: str = None


@dataclass(kw_only=True)
class SystemMetric:
    pod_name: str
    metric: str
    peak: float
    mean: float = None


@dataclass(kw_only=True)
class CheckpointSizeMetrics:
    checkpoint_step: int
    checkpoint_location: str
    size_bytes: int
    global_rank: int = None
