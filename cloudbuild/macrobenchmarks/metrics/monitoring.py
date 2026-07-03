"""Fetch per-pod system metrics from Cloud Monitoring.

Usage:
    python3 -m metrics.monitoring --project P --run-id R \
        --start-time RFC3339 --end-time RFC3339 --out-dir DIR
"""

import argparse
import datetime
import statistics
import traceback
from dataclasses import dataclass

from metrics import raw_store, schema


@dataclass(frozen=True)
class Series:
    """One monitoring series mapped to our internal metric name."""

    name: str  # internal series name: cpu | memory | network_received | network_sent
    metric_type: str  # Cloud Monitoring metric.type
    resource_type: str  # k8s_container | k8s_pod
    aligner: str  # per-series aligner name


# CPU: cores (RATE), Memory: peak bytes (MAX), Network: bytes/s (RATE).
SERIES = [
    Series(
        "cpu",
        "kubernetes.io/container/cpu/core_usage_time",
        "k8s_container",
        "ALIGN_RATE",
    ),
    Series(
        "memory",
        "kubernetes.io/container/memory/used_bytes",
        "k8s_container",
        "ALIGN_MAX",
    ),
    Series(
        "network_received",
        "kubernetes.io/pod/network/received_bytes_count",
        "k8s_pod",
        "ALIGN_RATE",
    ),
    Series(
        "network_sent",
        "kubernetes.io/pod/network/sent_bytes_count",
        "k8s_pod",
        "ALIGN_RATE",
    ),
]


def _to_epoch(rfc3339: str) -> int:
    """Parse RFC3339 to epoch seconds."""
    dt = datetime.datetime.fromisoformat(rfc3339.replace("Z", "+00:00"))
    return int(dt.timestamp())


def _point_value(point) -> float:
    """Read numeric value from point."""
    v = point.value
    if getattr(v, "double_value", 0.0):
        return float(v.double_value)
    return float(getattr(v, "int64_value", 0))


def reduce_points(values: list) -> tuple:
    """Return (peak, mean) of values, or (None, None)."""
    if not values:
        return None, None
    return max(values), statistics.mean(values)


def _build_request(project, run_id, series, start_epoch, end_epoch, period):
    """Build list_time_series request."""
    filter_ = (
        f'metric.type = "{series.metric_type}" '
        f'AND resource.type = "{series.resource_type}" '
        f'AND resource.labels.pod_name = starts_with("{run_id}-workload-0-")'
    )
    return {
        "name": f"projects/{project}",
        "filter": filter_,
        "interval": {
            "start_time": {"seconds": int(start_epoch)},
            "end_time": {"seconds": int(end_epoch)},
        },
        "aggregation": {
            "alignment_period": {"seconds": period},
            "per_series_aligner": series.aligner,
        },
    }


def collect(client, *, project, run_id, start_epoch, end_epoch, period=60) -> list:
    """Collect SystemMetric rows for all SERIES."""
    rows = []
    for series in SERIES:
        request = _build_request(
            project, run_id, series, start_epoch, end_epoch, period
        )
        for ts in client.list_time_series(request):
            pod_name = ts.resource.labels.get("pod_name", "")
            values = [_point_value(p) for p in ts.points]
            peak, mean = reduce_points(values)
            if peak is None:
                continue
            rows.append(
                schema.SystemMetric(
                    pod_name=pod_name, metric=series.name, peak=peak, mean=mean
                )
            )
    return rows


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(
        description="Fetch per-pod system metrics from Cloud Monitoring."
    )
    parser.add_argument("--project", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--start-time", required=True, help="RFC3339")
    parser.add_argument("--end-time", required=True, help="RFC3339")
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--period", type=int, default=60)
    args = parser.parse_args(argv)

    # Import here to handle missing library case separately.
    try:
        from google.cloud import monitoring_v3
    except ImportError as e:
        print(f"Warning: google-cloud-monitoring unavailable, columns will be N/A: {e}")
        return

    try:
        client = monitoring_v3.MetricServiceClient()
        rows = collect(
            client,
            project=args.project,
            run_id=args.run_id,
            start_epoch=_to_epoch(args.start_time),
            end_epoch=_to_epoch(args.end_time),
            period=args.period,
        )
        raw_store.write_system_metrics(rows, args.out_dir)
        print(f"Wrote {len(rows)} system-metric rows to {args.out_dir}")
    except Exception as e:  # best-effort
        print(
            f"Warning: system-metrics fetch failed, columns will be N/A: {e}\n"
            f"{traceback.format_exc()}"
        )


if __name__ == "__main__":
    main()
