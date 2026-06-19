"""HF Llama benchmark log parser.

The 7 regex constants below are byte-identical to tessellations
metrics/raw_metrics_extraction/hf.py (lines 32-38). parse_entries reproduces
hf.py's matching/pairing logic over an injectable iterable of LogEntry, so it is
unit-testable without a Cloud Logging client.
"""

import argparse
import re
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, Iterable, List

from metrics import raw_store, schema

# --- regexes (verbatim from tessellations hf.py) ---------------------------
STEP_METRICS_PATTERN = r"Global Rank: 0 \| Step: ([0-9]+) \| Loss: [0-9.]+ \| Step Time: ([0-9.]+)s \| Throughput: [0-9.]+ samples/s"
CHECKPOINT_START_PATTERN = r"Checkpoint Save : Rank: ([0-9]+) : Step: ([0-9]+) : Start time: ([0-9.]+) seconds: Path: (.*)"
CHECKPOINT_END_PATTERN = r"Finished saving checkpoint to (.*) in ([0-9.]+) seconds for global_step ([0-9]+)\s+from rank ([0-9]+)"
CHECKPOINT_RESTORE_START_PATTERN = r"Checkpoint Restore Start : Rank : ([0-9]+) : Start time: ([0-9.]+) seconds : Path: (.*)"
CHECKPOINT_RESTORE_END_PATTERN = r"Finished restoring checkpoint : Rank : ([0-9]+) : Duration: ([0-9.]+) seconds : End Time: ([0-9.]+) seconds : Path: (.*)"
CHECKPOINT_DELETE_PATTERN = r"Finished deleting checkpoint (.*) in ([0-9.]+) seconds for global_step ([0-9]+) from rank ([0-9]+)"
ACCELERATOR_BLOCKED_TIME_PATTERN = r"\[_TrainingEpochLoop\]\.train_dataloader_next\s+(?:\|\s+[\d\.]+\s+){2}\|\s+([\d\.]+)\s+\|\s+([\d\.]+)\s+\|"

ALL_PATTERNS = [
    STEP_METRICS_PATTERN, CHECKPOINT_START_PATTERN, CHECKPOINT_END_PATTERN,
    CHECKPOINT_RESTORE_START_PATTERN, CHECKPOINT_RESTORE_END_PATTERN,
    CHECKPOINT_DELETE_PATTERN, ACCELERATOR_BLOCKED_TIME_PATTERN
]


@dataclass
class LogEntry:
    timestamp: float  # epoch seconds
    message: str


@dataclass
class ParsedRawMetrics:
    step_metrics: List[schema.StepMetrics] = field(default_factory=list)
    write_metrics: Dict[int, List[schema.WriteDurationMetrics]] = field(
        default_factory=lambda: defaultdict(list))
    restore_metrics: Dict[int, List[schema.RestoreDurationMetrics]] = field(
        default_factory=lambda: defaultdict(list))
    delete_metrics: Dict[int, List[schema.DeleteDurationMetrics]] = field(
        default_factory=lambda: defaultdict(list))
    data_loading_metrics: List[schema.DataLoadingMetrics] = field(
        default_factory=list)


def parse_entries(entries: Iterable[LogEntry], *, run_id: str,
                  checkpoint_location: str) -> ParsedRawMetrics:
    """Scrape raw metrics from log entries (mirrors hf.py._scrape_raw_metrics)."""
    out = ParsedRawMetrics()
    checkpoint_starts = {}   # (step, rank) -> {start_time, path}
    restore_starts = {}      # rank -> {start_time, path}

    for entry in entries:
        message = entry.message
        if not message:
            continue
        ts = entry.timestamp

        m = re.search(STEP_METRICS_PATTERN, message)
        if m:
            try:
                out.step_metrics.append(
                    schema.StepMetrics(step=int(m.group(1)),
                                       step_duration=float(m.group(2)),
                                       step_end_time=ts))
            except (ValueError, IndexError):
                print(f"Warning: Could not parse step metrics from: {message}")

        m = re.search(CHECKPOINT_START_PATTERN, message)
        if m:
            rank = int(m.group(1))
            step = int(m.group(2))
            if (step, rank) not in checkpoint_starts:
                checkpoint_starts[(step, rank)] = {
                    "start_time": float(m.group(3)),
                    "path": m.group(4),
                }

        m = re.search(CHECKPOINT_END_PATTERN, message)
        if m:
            step = int(m.group(3))
            rank = int(m.group(4))
            if (step, rank) in checkpoint_starts:
                start_info = checkpoint_starts[(step, rank)]
                duration = float(m.group(2))
                start_time = start_info["start_time"]
                # The "Finished saving" log carries only a duration, so derive
                # end_time from the paired start; calc recomputes end - start.
                out.write_metrics[rank].append(
                    schema.WriteDurationMetrics(
                        global_rank=rank,
                        checkpoint_location=checkpoint_location,
                        checkpoint_step=step,
                        start_time=start_time,
                        end_time=start_time + duration))
                del checkpoint_starts[(step, rank)]

        m = re.search(CHECKPOINT_DELETE_PATTERN, message)
        if m:
            step = int(m.group(3))
            rank = int(m.group(4))
            if rank == 0:
                # Delete logs no absolute start; anchor end_time to the log's
                # Cloud Logging timestamp and back out start from the duration.
                duration = float(m.group(2))
                end_time = ts
                out.delete_metrics[rank].append(
                    schema.DeleteDurationMetrics(
                        global_rank=rank,
                        checkpoint_location=checkpoint_location,
                        checkpoint_step=step,
                        start_time=end_time - duration,
                        end_time=end_time))

        m = re.search(CHECKPOINT_RESTORE_START_PATTERN, message)
        if m:
            rank = int(m.group(1))
            if rank not in restore_starts:
                restore_starts[rank] = {
                    "start_time": float(m.group(2)),
                    "path": m.group(3),
                }

        m = re.search(CHECKPOINT_RESTORE_END_PATTERN, message)
        if m:
            rank = int(m.group(1))
            if rank in restore_starts:
                start_info = restore_starts[rank]
                # Key each restore by the checkpoint path it loaded (captured at
                # the paired start), not the run-wide checkpoint_location. Under
                # DDP every rank restores the same path, so calc_restore_metrics
                # collapses all ranks into one distributed datapoint (max end -
                # min start); two distinct restores keep separate paths and stay
                # separate datapoints instead of merging into one inflated span.
                # Both the start ("Start time") and end ("End Time") are
                # wall-clock timestamps from the workload, so the cross-rank span
                # is valid across nodes.
                out.restore_metrics[rank].append(
                    schema.RestoreDurationMetrics(
                        checkpoint_step=0,
                        global_rank=rank,
                        checkpoint_location=start_info["path"],
                        start_time=start_info["start_time"],
                        end_time=float(m.group(3))))
                del restore_starts[rank]

        m = re.search(ACCELERATOR_BLOCKED_TIME_PATTERN, message)
        if m:
            try:
                out.data_loading_metrics.append(
                    schema.DataLoadingMetrics(
                        run_id=run_id,
                        epoch_idx=-1,
                        accelerator_blocked_time=float(m.group(1)),
                        accelerator_blocked_percent=float(m.group(2)),
                        update_timestamp=None))
            except (ValueError, IndexError):
                print("Warning: Could not parse accelerator blocked time "
                      f"metrics from: {message}")

    return out


def build_filter(*, project: str, run_id: str, start_time: str,
                 end_time: str) -> str:
    """Cloud Logging filter mirroring hf.py._scrape_raw_metrics."""
    regex_or = " OR ".join(f'textPayload =~ "{p}"' for p in ALL_PATTERNS)
    return (
        'resource.type="k8s_container" '
        f'resource.labels.project_id="{project}" '
        f'resource.labels.pod_name:"{run_id}-workload-0-" '
        'severity>=DEFAULT '
        f'timestamp>="{start_time}" '
        f'timestamp<="{end_time}" '
        f'AND ({regex_or})')


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description="Scrape HF benchmark metrics "
                                     "from Cloud Logging into raw CSVs.")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--project", required=True)
    parser.add_argument("--start-time", required=True, help="RFC3339")
    parser.add_argument("--end-time", required=True, help="RFC3339")
    parser.add_argument("--checkpoint-location", required=True)
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--run-type", default="perf_optimization")
    args = parser.parse_args(argv)

    from google.cloud import logging as cloud_logging
    client = cloud_logging.Client(project=args.project)
    filter_string = build_filter(project=args.project, run_id=args.run_id,
                                 start_time=args.start_time,
                                 end_time=args.end_time)

    def _entries():
        for e in client.list_entries(filter_=filter_string,
                                     order_by="timestamp asc"):
            payload = e.payload if isinstance(e.payload, str) else (
                e.payload.get("message", "") if e.payload else "")
            yield LogEntry(timestamp=e.timestamp.timestamp(), message=payload)

    parsed = parse_entries(_entries(), run_id=args.run_id,
                           checkpoint_location=args.checkpoint_location)
    raw_store.write_raw_metrics(parsed, args.out_dir, run_type=args.run_type)
    print(f"Wrote raw metrics to {args.out_dir}")


if __name__ == "__main__":
    main()
