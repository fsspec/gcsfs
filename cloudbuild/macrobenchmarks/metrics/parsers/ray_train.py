"""Strict regex parser for Ray macrobenchmark metric log records."""

import argparse
import json
import math
import re
from collections import defaultdict
from dataclasses import dataclass, field
from numbers import Real
from typing import Dict, Iterable, List

from metrics import raw_store, schema
from metrics.parsers.common import LogEntry, iter_log_entries
from metrics.parsers.hf import (
    CHECKPOINT_DELETE_PATTERN,
    CHECKPOINT_END_PATTERN,
    CHECKPOINT_RESTORE_END_PATTERN,
    CHECKPOINT_RESTORE_START_PATTERN,
    CHECKPOINT_SIZE_PATTERN,
    CHECKPOINT_START_PATTERN,
    DATASET_BUILD_PATTERN,
    STEP_METRICS_PATTERN,
)

RAY_DATA_ITERATION_PATTERN = (
    r"Ray Data Iteration : Rank : ([0-9]+) : Split : ([0-9]+) : "
    r"Blocked : ([0-9.]+) seconds : Total : ([0-9.]+) seconds : "
    r"First Batch : ([0-9.]+) seconds : Blocked Calls : ([0-9]+)"
)

# Cloud Logging must select candidate records, not pre-validate them. Stable
# literal prefixes let malformed or truncated candidates reach the strict
# parser and fail the scrape instead of disappearing silently.
METRIC_PREFIXES = (
    "Global Rank:",
    "Dataset Build :",
    "Checkpoint Save :",
    "Finished saving checkpoint to ",
    "Checkpoint Size :",
    "Checkpoint Restore Start :",
    "Finished restoring checkpoint :",
    "Finished deleting checkpoint ",
    "Ray Data Iteration :",
)

# Events whose checkpoint_location must sit under the run's checkpoint root.
# Restores are excluded: a warm start reads a seed or external checkpoint that
# was deliberately written outside the run's own root.
_ROOTED_EVENTS = frozenset(
    {"checkpoint_committed", "checkpoint_size", "checkpoint_deleted"}
)


class MetricEventError(ValueError):
    """A Ray metric record violates the workload/parser contract."""


@dataclass
class ParsedRawMetrics:
    step_metrics: List[schema.StepMetrics] = field(default_factory=list)
    write_metrics: Dict[int, List[schema.WriteDurationMetrics]] = field(
        default_factory=lambda: defaultdict(list)
    )
    restore_metrics: Dict[int, List[schema.RestoreDurationMetrics]] = field(
        default_factory=lambda: defaultdict(list)
    )
    delete_metrics: Dict[int, List[schema.DeleteDurationMetrics]] = field(
        default_factory=lambda: defaultdict(list)
    )
    data_loading_metrics: List[schema.DataLoadingMetrics] = field(default_factory=list)
    checkpoint_sizes: List[schema.CheckpointSizeMetrics] = field(default_factory=list)
    data_wait_metrics: List[schema.DataWaitMetrics] = field(default_factory=list)
    dataset_build_metrics: List[schema.DatasetBuildMetrics] = field(
        default_factory=list
    )
    ray_data_iteration_metrics: List[schema.RayDataIterationMetrics] = field(
        default_factory=list
    )


def _require_field(payload: dict, field_name: str):
    if field_name not in payload:
        raise MetricEventError(f"Missing field {field_name}")
    return payload[field_name]


def _require_string(payload: dict, field_name: str) -> str:
    value = _require_field(payload, field_name)
    if not isinstance(value, str) or not value:
        raise MetricEventError(f"Ray metric field {field_name} must be a string")
    return value


def _require_int(payload: dict, field_name: str, *, nonnegative=True) -> int:
    value = _require_field(payload, field_name)
    if isinstance(value, bool) or not isinstance(value, int):
        raise MetricEventError(f"Ray metric field {field_name} must be an integer")
    if nonnegative and value < 0:
        raise MetricEventError(f"Ray metric field {field_name} must be nonnegative")
    return value


def _require_number(payload: dict, field_name: str, *, nonnegative=True) -> float:
    value = _require_field(payload, field_name)
    if isinstance(value, bool) or not isinstance(value, Real):
        raise MetricEventError(f"Ray metric field {field_name} must be numeric")
    value = float(value)
    if not math.isfinite(value):
        raise MetricEventError(f"non-finite metric field {field_name}")
    if nonnegative and value < 0:
        raise MetricEventError(f"Ray metric field {field_name} must be nonnegative")
    return value


def _location_root(uri: str) -> str:
    """The scheme-less prefix that checkpoint locations are expected under.

    Ray keeps the URI scheme in the filesystem object and carries bare
    ``bucket/path`` strings, while the pipeline supplies a ``gs://`` URI.
    """
    for scheme in ("gs://", "gcs://"):
        if uri.startswith(scheme):
            uri = uri[len(scheme) :]
            break
    return uri.rstrip("/")


def _require_location_under(location: str, root: str, event: str) -> None:
    # Normalized on both sides: Ray carries scheme-less paths, but a location is
    # allowed to spell out the scheme the pipeline's root argument uses.
    normalized = _location_root(location)
    if normalized != root and not normalized.startswith(f"{root}/"):
        raise MetricEventError(
            f"{event} checkpoint_location {location!r} is not under the run's "
            f"checkpoint root {root!r}"
        )


def _validate_event_fields(payload: dict) -> None:
    event = _require_field(payload, "event")
    if event == "step":
        _require_int(payload, "step")
        _require_number(payload, "duration_s")
        _require_number(payload, "samples_per_second")
        if "end_time_s" in payload:
            _require_number(payload, "end_time_s")
    elif event == "dataset_build":
        _require_int(payload, "global_rank")
        _require_number(payload, "duration_s")
        _require_string(payload, "dataset_path")
    elif event in {"checkpoint_committed", "checkpoint_size"}:
        _require_int(payload, "checkpoint_step")
        _require_string(payload, "checkpoint_location")
        if event == "checkpoint_committed":
            _require_int(payload, "global_rank")
            _require_number(payload, "duration_s")
        else:
            _require_int(payload, "size_bytes")
    elif event in {
        "checkpoint_restore_started",
        "checkpoint_restore_completed",
    }:
        _require_string(payload, "restore_id")
        _require_int(payload, "global_rank")
        if "checkpoint_step" in payload:
            _require_int(payload, "checkpoint_step")
        _require_string(payload, "checkpoint_location")
        _require_number(payload, "time_s")
    elif event == "checkpoint_deleted":
        _require_int(payload, "checkpoint_step")
        _require_string(payload, "checkpoint_location")
        _require_number(payload, "duration_s")
        if not isinstance(_require_field(payload, "success"), bool):
            raise MetricEventError("Ray metric field success must be a boolean")
    elif event == "ray_data_iteration":
        for field_name in ("global_rank", "split_index"):
            _require_int(payload, field_name)
        for field_name in (
            "total_blocked_s",
            "total_s",
            "time_to_first_batch_s",
        ):
            _require_number(payload, field_name)
        _require_int(payload, "blocked_calls")


def _base_payload(event, event_id, **fields):
    return {
        "event": event,
        "event_id": event_id,
        **fields,
    }


def _record_checkpoint_half(store, key, value, half):
    existing = store.get(key)
    if existing is None:
        store[key] = value
    elif existing != value:
        raise MetricEventError(f"conflicting checkpoint save {half} {key!r}")


def _event_payloads(entries: Iterable[LogEntry]):
    checkpoint_starts = {}
    checkpoint_completions = {}
    for entry in entries:
        message = entry.message
        match = re.fullmatch(STEP_METRICS_PATTERN, message)
        if match:
            step = int(match.group(1))
            yield entry, _base_payload(
                "step",
                f"step:{step}",
                step=step,
                duration_s=float(match.group(2)),
                samples_per_second=float(match.group(3)),
            )
            continue

        match = re.fullmatch(DATASET_BUILD_PATTERN, message)
        if match:
            rank = int(match.group(1))
            yield entry, _base_payload(
                "dataset_build",
                f"dataset-build:{rank}",
                global_rank=rank,
                duration_s=float(match.group(2)),
                dataset_path=match.group(3),
            )
            continue

        match = re.fullmatch(CHECKPOINT_START_PATTERN, message)
        if match:
            rank = int(match.group(1))
            step = int(match.group(2))
            _record_checkpoint_half(
                checkpoint_starts,
                (step, rank),
                {
                    "start_time": float(match.group(3)),
                    "path": match.group(4),
                },
                "start",
            )
            continue

        match = re.fullmatch(CHECKPOINT_END_PATTERN, message)
        if match:
            location = match.group(1)
            duration = float(match.group(2))
            step = int(match.group(3))
            rank = int(match.group(4))
            _record_checkpoint_half(
                checkpoint_completions,
                (step, rank),
                {"path": location, "duration_s": duration},
                "completion",
            )
            continue

        match = re.fullmatch(CHECKPOINT_SIZE_PATTERN, message)
        if match:
            rank = int(match.group(1))
            if rank != 0:
                raise MetricEventError("checkpoint size must be emitted by rank 0")
            step = int(match.group(2))
            yield entry, _base_payload(
                "checkpoint_size",
                f"size:{step}",
                checkpoint_step=step,
                size_bytes=int(match.group(3)),
                checkpoint_location=match.group(4),
            )
            continue

        match = re.fullmatch(CHECKPOINT_RESTORE_START_PATTERN, message)
        if match:
            rank = int(match.group(1))
            location = match.group(3)
            restore_id = f"restore:{rank}:{location}"
            yield entry, _base_payload(
                "checkpoint_restore_started",
                f"{restore_id}:started",
                restore_id=restore_id,
                global_rank=rank,
                checkpoint_location=location,
                time_s=float(match.group(2)),
            )
            continue

        match = re.fullmatch(CHECKPOINT_RESTORE_END_PATTERN, message)
        if match:
            rank = int(match.group(1))
            location = match.group(4)
            restore_id = f"restore:{rank}:{location}"
            yield entry, _base_payload(
                "checkpoint_restore_completed",
                f"{restore_id}:completed",
                restore_id=restore_id,
                global_rank=rank,
                checkpoint_location=location,
                time_s=float(match.group(3)),
            )
            continue

        match = re.fullmatch(CHECKPOINT_DELETE_PATTERN, message)
        if match:
            rank = int(match.group(4))
            if rank != 0:
                raise MetricEventError("checkpoint delete must be emitted by rank 0")
            step = int(match.group(3))
            yield entry, _base_payload(
                "checkpoint_deleted",
                f"delete:{step}",
                checkpoint_step=step,
                checkpoint_location=match.group(1),
                duration_s=float(match.group(2)),
                success=True,
            )
            continue

        match = re.fullmatch(RAY_DATA_ITERATION_PATTERN, message)
        if match:
            rank = int(match.group(1))
            yield entry, _base_payload(
                "ray_data_iteration",
                f"ray-data:{rank}",
                global_rank=rank,
                split_index=int(match.group(2)),
                total_blocked_s=float(match.group(3)),
                total_s=float(match.group(4)),
                time_to_first_batch_s=float(match.group(5)),
                blocked_calls=int(match.group(6)),
            )
            continue

        raise MetricEventError(f"unrecognized Ray metric record: {message}")

    missing_completions = checkpoint_starts.keys() - checkpoint_completions.keys()
    if missing_completions:
        key = sorted(missing_completions)[0]
        raise MetricEventError(f"unmatched checkpoint save start {key!r}")
    missing_starts = checkpoint_completions.keys() - checkpoint_starts.keys()
    if missing_starts:
        key = sorted(missing_starts)[0]
        raise MetricEventError(f"unmatched checkpoint save completion {key!r}")

    for (step, rank), start in sorted(checkpoint_starts.items()):
        completion = checkpoint_completions[(step, rank)]
        if start["path"] != completion["path"]:
            raise MetricEventError(
                f"checkpoint save {(step, rank)!r} has mismatched path"
            )
        duration = completion["duration_s"]
        yield LogEntry(
            timestamp=start["start_time"] + duration,
            message="",
        ), _base_payload(
            "checkpoint_committed",
            f"commit:{step}:{rank}",
            global_rank=rank,
            checkpoint_step=step,
            checkpoint_location=completion["path"],
            duration_s=duration,
        )


def _unique_events(entries: Iterable[LogEntry]):
    seen = {}
    events = []
    for entry, payload in _event_payloads(entries):
        _require_string(payload, "event_id")
        _validate_event_fields(payload)
        event_id = payload["event_id"]
        normalized = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        if event_id in seen:
            if seen[event_id] != normalized:
                raise MetricEventError(f"conflicting duplicate event_id {event_id!r}")
            continue
        seen[event_id] = normalized
        events.append((entry, payload))
    return events


def parse_entries(
    entries: Iterable[LogEntry], *, run_id: str, checkpoint_location: str
) -> ParsedRawMetrics:
    """Decode complete, valid Ray benchmark observations into raw records."""
    if not isinstance(checkpoint_location, str) or not checkpoint_location:
        raise MetricEventError("checkpoint_location must be a nonempty string")
    checkpoint_root = _location_root(checkpoint_location)

    out = ParsedRawMetrics()
    restore_starts = {}
    restore_completions = {}

    for entry, payload in _unique_events(entries):
        event = payload["event"]
        if event in _ROOTED_EVENTS:
            _require_location_under(
                payload["checkpoint_location"], checkpoint_root, event
            )
        if event == "step":
            out.step_metrics.append(
                schema.StepMetrics(
                    step=payload["step"],
                    step_duration=float(payload["duration_s"]),
                    step_end_time=float(payload.get("end_time_s", entry.timestamp)),
                    samples_per_second=float(payload["samples_per_second"]),
                )
            )
        elif event == "dataset_build":
            out.dataset_build_metrics.append(
                schema.DatasetBuildMetrics(
                    global_rank=payload["global_rank"],
                    duration=float(payload["duration_s"]),
                    dataset_path=payload["dataset_path"],
                )
            )
        elif event == "checkpoint_committed":
            rank = payload["global_rank"]
            duration = float(payload["duration_s"])
            out.write_metrics[rank].append(
                schema.WriteDurationMetrics(
                    global_rank=rank,
                    checkpoint_step=payload["checkpoint_step"],
                    checkpoint_location=payload["checkpoint_location"],
                    start_time=entry.timestamp - duration,
                    end_time=entry.timestamp,
                )
            )
        elif event == "checkpoint_size":
            out.checkpoint_sizes.append(
                schema.CheckpointSizeMetrics(
                    global_rank=0,
                    checkpoint_step=payload["checkpoint_step"],
                    checkpoint_location=payload["checkpoint_location"],
                    size_bytes=payload["size_bytes"],
                )
            )
        elif event == "checkpoint_restore_started":
            restore_id = payload["restore_id"]
            if restore_id in restore_starts:
                raise MetricEventError(f"duplicate restore start {restore_id!r}")
            restore_starts[restore_id] = payload
        elif event == "checkpoint_restore_completed":
            restore_id = payload["restore_id"]
            if restore_id in restore_completions:
                raise MetricEventError(f"duplicate restore completion {restore_id!r}")
            restore_completions[restore_id] = payload
        elif event == "checkpoint_deleted":
            if not payload["success"]:
                raise MetricEventError(
                    "failed checkpoint deletion: " f"{payload['checkpoint_location']}"
                )
            end_time = entry.timestamp
            duration = float(payload["duration_s"])
            out.delete_metrics[0].append(
                schema.DeleteDurationMetrics(
                    global_rank=0,
                    checkpoint_step=payload["checkpoint_step"],
                    checkpoint_location=payload["checkpoint_location"],
                    start_time=end_time - duration,
                    end_time=end_time,
                )
            )
        elif event == "ray_data_iteration":
            out.ray_data_iteration_metrics.append(
                schema.RayDataIterationMetrics(
                    run_id=run_id,
                    global_rank=payload["global_rank"],
                    split_index=payload["split_index"],
                    total_blocked_s=payload["total_blocked_s"],
                    total_s=payload["total_s"],
                    time_to_first_batch_s=payload["time_to_first_batch_s"],
                    blocked_calls=payload["blocked_calls"],
                )
            )

    missing_completions = restore_starts.keys() - restore_completions.keys()
    if missing_completions:
        raise MetricEventError(
            f"unmatched restore start {sorted(missing_completions)[0]!r}"
        )
    missing_starts = restore_completions.keys() - restore_starts.keys()
    if missing_starts:
        raise MetricEventError(
            f"unmatched restore completion {sorted(missing_starts)[0]!r}"
        )

    for restore_id, start in restore_starts.items():
        completion = restore_completions[restore_id]
        for field_name in ("global_rank", "checkpoint_location"):
            if start[field_name] != completion[field_name]:
                raise MetricEventError(
                    f"restore {restore_id!r} has mismatched {field_name}"
                )
        if start.get("checkpoint_step") != completion.get("checkpoint_step"):
            raise MetricEventError(
                f"restore {restore_id!r} has mismatched checkpoint_step"
            )
        if completion["time_s"] < start["time_s"]:
            raise MetricEventError(
                f"restore {restore_id!r} completed before it started"
            )
        rank = start["global_rank"]
        out.restore_metrics[rank].append(
            schema.RestoreDurationMetrics(
                global_rank=rank,
                checkpoint_step=start.get("checkpoint_step"),
                checkpoint_location=start["checkpoint_location"],
                start_time=float(start["time_s"]),
                end_time=float(completion["time_s"]),
            )
        )

    return out


def build_filter(*, project: str, run_id: str, start_time: str, end_time: str) -> str:
    metric_candidates = " OR ".join(
        f'textPayload:"{prefix}"' for prefix in METRIC_PREFIXES
    )
    return (
        'resource.type="k8s_container" '
        f'resource.labels.project_id="{project}" '
        f'resource.labels.pod_name:"{run_id}-workload-0-" '
        "severity>=DEFAULT "
        f'timestamp>="{start_time}" '
        f'timestamp<="{end_time}" '
        f"AND ({metric_candidates})"
    )


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(
        description="Scrape Ray benchmark metrics from Cloud Logging into raw CSVs."
    )
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--project", required=True)
    parser.add_argument("--start-time", required=True, help="RFC3339")
    parser.add_argument("--end-time", required=True, help="RFC3339")
    parser.add_argument(
        "--checkpoint-location",
        required=True,
        help=(
            "The run's checkpoint root. Commit, size, and delete events must "
            "name a location under it."
        ),
    )
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--run-type", default="perf_optimization")
    args = parser.parse_args(argv)

    from google.cloud.logging_v2.services.logging_service_v2 import (
        LoggingServiceV2Client,
    )

    client = LoggingServiceV2Client()
    filter_string = build_filter(
        project=args.project,
        run_id=args.run_id,
        start_time=args.start_time,
        end_time=args.end_time,
    )
    parsed = parse_entries(
        iter_log_entries(client, args.project, filter_string),
        run_id=args.run_id,
        checkpoint_location=args.checkpoint_location,
    )
    raw_store.write_raw_metrics(parsed, args.out_dir, run_type=args.run_type)
    print(f"Wrote raw metrics to {args.out_dir}")


if __name__ == "__main__":
    main()
