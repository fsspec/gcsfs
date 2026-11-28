"""Scrape artifact benchmark lines from Cloud Logging into raw CSVs.

``workloads/adk-agent/helm_chart/adk_bench.py`` prints one line per artifact
service call plus probe and workload summary lines. This parser validates and
deduplicates them into the raw tables ``calculate.py`` aggregates with
``--require-artifact-metrics``.
"""

import argparse
import math
import re
from dataclasses import dataclass, field
from typing import Iterable, List

from metrics import raw_store, schema
from metrics.parsers.common import LogEntry, iter_log_entries

OP_PATTERN = re.compile(
    r"Artifact Op : Seq: ([a-z]+:[1-9][0-9]*) : Op: (\w+) : "
    r"Latency: ([0-9.]+) ms : Bytes: ([0-9]+) : Ok: (true|false)"
)
PROBE_PATTERN = re.compile(r"Artifact Probe : Op: (\w+) : Requests Per Op: ([0-9.]+)")
WORKLOAD_PATTERN = re.compile(
    r"Artifact Workload : "
    r"Start: (\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z) : "
    r"End: (\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z) : "
    r"Total: ([0-9.]+) s : "
    r"Cold First Op: ([0-9.]+|N/A) ms : Sessions: ([0-9]+) : Turns: ([0-9]+) : "
    r"Ops: ([0-9]+) : "
    r"Untimed Read Bytes: ([0-9]+) : Checks: ([0-9]+) : Failures: ([0-9]+)"
)
# Literal prefixes select candidate records in Cloud Logging without
# pre-validating them, so truncated or malformed records fail parse_entries.
METRIC_PREFIXES = (
    "Artifact Op :",
    "Artifact Probe :",
    "Artifact Workload :",
)


class MetricEventError(ValueError):
    """An artifact metric record violates the driver/parser contract."""


@dataclass
class ParsedArtifactMetrics:
    artifact_op_metrics: List[schema.ArtifactOpMetrics] = field(default_factory=list)
    artifact_probe_metrics: List[schema.ArtifactProbeMetrics] = field(
        default_factory=list
    )
    artifact_workload_metrics: List[schema.ArtifactWorkloadMetrics] = field(
        default_factory=list
    )


def _finite_nonnegative(raw: str, field_name: str) -> float:
    try:
        value = float(raw)
    except ValueError as exc:
        raise MetricEventError(f"invalid {field_name}: {raw!r}") from exc
    if not math.isfinite(value) or value < 0:
        raise MetricEventError(f"non-finite or negative {field_name}: {raw!r}")
    return value


def parse_entries(entries: Iterable[LogEntry]) -> ParsedArtifactMetrics:
    """Parse and deduplicate artifact benchmark log lines."""
    parsed = ParsedArtifactMetrics()
    seen_ops = {}
    seen_probes = {}
    seen_workload = None
    for entry in entries:
        message = (entry.message or "").rstrip("\r\n")
        if not any(prefix in message for prefix in METRIC_PREFIXES):
            continue
        match = OP_PATTERN.fullmatch(message)
        if match:
            seq, op, latency_ms, size, ok = match.groups()
            record = schema.ArtifactOpMetrics(
                seq=seq,
                op=op,
                latency_ms=_finite_nonnegative(latency_ms, "latency_ms"),
                bytes=int(size),
                ok=1 if ok == "true" else 0,
            )
            existing = seen_ops.get(seq)
            if existing is not None:
                if existing != record:
                    raise MetricEventError(f"conflicting duplicate op seq {seq!r}")
                continue
            seen_ops[seq] = record
            parsed.artifact_op_metrics.append(record)
            continue
        match = PROBE_PATTERN.fullmatch(message)
        if match:
            op, requests = match.groups()
            record = schema.ArtifactProbeMetrics(
                op=op,
                requests_per_op=_finite_nonnegative(requests, "requests_per_op"),
            )
            existing = seen_probes.get(op)
            if existing is not None:
                if existing != record:
                    raise MetricEventError(f"conflicting duplicate probe op {op!r}")
                continue
            seen_probes[op] = record
            parsed.artifact_probe_metrics.append(record)
            continue
        match = WORKLOAD_PATTERN.fullmatch(message)
        if match:
            (
                window_start,
                window_end,
                total,
                cold,
                sessions,
                turns,
                ops,
                untimed_read_bytes,
                checks,
                failures,
            ) = match.groups()
            record = schema.ArtifactWorkloadMetrics(
                window_start=window_start,
                window_end=window_end,
                total_s=_finite_nonnegative(total, "total_s"),
                cold_first_op_ms=(
                    None
                    if cold == "N/A"
                    else _finite_nonnegative(cold, "cold_first_op_ms")
                ),
                sessions=int(sessions),
                turns=int(turns),
                ops=int(ops),
                untimed_read_bytes=int(untimed_read_bytes),
                validation_checks=int(checks),
                validation_failures=int(failures),
            )
            if seen_workload is not None:
                if seen_workload != record:
                    raise MetricEventError("conflicting duplicate workload record")
                continue
            seen_workload = record
            parsed.artifact_workload_metrics.append(record)
            continue
        raise MetricEventError(f"unrecognized artifact metric record: {message}")
    return parsed


def build_filter(*, project: str, run_id: str, start_time: str, end_time: str) -> str:
    """Cloud Logging filter for the artifact lines of every pod in the release."""
    metric_candidates = " OR ".join(
        f'textPayload:"{prefix}"' for prefix in METRIC_PREFIXES
    )
    return (
        'resource.type="k8s_container" '
        f'resource.labels.project_id="{project}" '
        f'resource.labels.pod_name:"{run_id}-workload-" '
        "severity>=DEFAULT "
        f'timestamp>="{start_time}" '
        f'timestamp<="{end_time}" '
        f"AND ({metric_candidates})"
    )


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(
        description="Scrape artifact benchmark metrics from Cloud Logging into raw CSVs."
    )
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--project", required=True)
    parser.add_argument("--start-time", required=True, help="RFC3339")
    parser.add_argument("--end-time", required=True, help="RFC3339")
    # Accepted so scrape_metrics.sh calls every parser the same way.
    parser.add_argument("--checkpoint-location")
    parser.add_argument("--out-dir", required=True)
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
    parsed = parse_entries(iter_log_entries(client, args.project, filter_string))
    raw_store.write_artifact_metrics(parsed, args.out_dir)
    print(f"Wrote {len(parsed.artifact_op_metrics)} artifact op rows to {args.out_dir}")


if __name__ == "__main__":
    main()
