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
RAY_DATA_ITERATION_DIRECTORY = "ray_data_iteration"
RAY_DATA_ITERATION_METRICS_FILE = "ray_data_iteration_metrics.csv"
ARTIFACT_DIRECTORY = "artifact"
ARTIFACT_OP_METRICS_FILE = "artifact_ops.csv"
ARTIFACT_PROBE_METRICS_FILE = "artifact_probes.csv"
ARTIFACT_WORKLOAD_METRICS_FILE = "workload.csv"


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


@dataclass(kw_only=True)
class RayDataIterationMetrics:
    run_id: str
    global_rank: int
    split_index: int
    total_blocked_s: float
    total_s: float
    time_to_first_batch_s: float
    blocked_calls: int


@dataclass(kw_only=True)
class ArtifactOpMetrics:
    """One timed artifact service call (``Artifact Op`` line)."""

    seq: str
    op: str
    latency_ms: float
    bytes: int
    ok: int


@dataclass(kw_only=True)
class ArtifactProbeMetrics:
    """JSON requests per op from the serial probe (``Artifact Probe`` line)."""

    op: str
    requests_per_op: float


@dataclass(kw_only=True)
class ArtifactWorkloadMetrics:
    """Run-level artifact benchmark results (``Artifact Workload`` line)."""

    total_s: float
    cold_first_op_ms: float = None
    sessions: int
    turns: int
    ops: int = None
    untimed_read_bytes: int = None
    validation_checks: int = None
    validation_failures: int = None
    window_start: str = None
    window_end: str = None
