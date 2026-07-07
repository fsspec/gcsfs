"""Fetch per-pod and per-bucket system metrics from Cloud Monitoring.

Usage:
    python3 -m metrics.monitoring --project P --run-id R \
        --start-time RFC3339 --end-time RFC3339 --out-dir DIR \
        [--checkpoint-bucket B] [--dataset-bucket B]
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

    name: str  # internal series name
    metric_type: str  # Cloud Monitoring metric.type
    resource_type: str  # k8s_container | k8s_pod | gcs_bucket | k8s_node
    aligner: str  # per-series aligner name
    # "pod" (pod_name prefix) | "bucket" (bucket_name) | "node" (cluster_name)
    filter_kind: str = "pod"
    method: str = None  # optional metric.labels.method filter (bucket series)


# CPU: cores (RATE), Memory: peak bytes (MAX), Network: bytes/s (RATE),
# limit utilizations: fraction of the container limit (MAX).
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

# Node allocatable CPU/memory (GAUGE); stands in for the container resource
# limit we don't set, so calculate.py can derive utilization as peak usage /
# node allocatable.
NODE_SERIES = [
    Series(
        "node_allocatable_cores",
        "kubernetes.io/node/cpu/allocatable_cores",
        "k8s_node",
        "ALIGN_MAX",
        filter_kind="node",
    ),
    Series(
        "node_allocatable_bytes",
        "kubernetes.io/node/memory/allocatable_bytes",
        "k8s_node",
        "ALIGN_MAX",
        filter_kind="node",
    ),
]

# Per-bucket totals summed over the window; `name` is prefixed with
# "checkpoint"/"dataset" to form the metric/column name.
GCS_BUCKET_SERIES = [
    Series(
        "read_bytes",
        "storage.googleapis.com/network/sent_bytes_count",
        "gcs_bucket",
        "ALIGN_DELTA",
        filter_kind="bucket",
    ),
    Series(
        "read_request_count",
        "storage.googleapis.com/api/request_count",
        "gcs_bucket",
        "ALIGN_DELTA",
        filter_kind="bucket",
        method="ReadObject",
    ),
]


def _to_epoch(rfc3339: str) -> int:
    """Parse RFC3339 to epoch seconds."""
    dt = datetime.datetime.fromisoformat(rfc3339.upper().replace("Z", "+00:00"))
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


def _build_request(project, series, target, start_epoch, end_epoch, period):
    """Build a list_time_series request.

    ``target`` is the run id (pod), bucket name (bucket), or cluster name (node).
    """
    if series.filter_kind == "bucket":
        filter_ = (
            f'metric.type = "{series.metric_type}" '
            f'AND resource.type = "{series.resource_type}" '
            f'AND resource.labels.bucket_name = "{target}"'
        )
        if series.method:
            filter_ += f' AND metric.labels.method = "{series.method}"'
    elif series.filter_kind == "node":
        # Scope to this run's cluster so a concurrent build's nodes aren't read.
        filter_ = (
            f'metric.type = "{series.metric_type}" '
            f'AND resource.type = "{series.resource_type}" '
            f'AND resource.labels.cluster_name = "{target}"'
        )
    else:
        filter_ = (
            f'metric.type = "{series.metric_type}" '
            f'AND resource.type = "{series.resource_type}" '
            f'AND resource.labels.pod_name = starts_with("{target}-workload-0-")'
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
    """Collect SystemMetric rows for all per-pod SERIES; a failed series is skipped."""
    rows = []
    for series in SERIES:
        try:
            request = _build_request(
                project, series, run_id, start_epoch, end_epoch, period
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
        except Exception as e:  # best-effort: keep the other series
            print(
                f"Warning: system series '{series.name}' failed, its columns N/A: {e}"
            )
    return rows


def collect_bucket_totals(
    client, *, project, bucket, prefix, start_epoch, end_epoch, period=60
) -> list:
    """Sum each GCS bucket series over the window into one SystemMetric row each."""
    rows = []
    for series in GCS_BUCKET_SERIES:
        try:
            request = _build_request(
                project, series, bucket, start_epoch, end_epoch, period
            )
            total = 0.0
            found = False
            for ts in client.list_time_series(request):
                for p in ts.points:
                    total += _point_value(p)
                    found = True
            if found:
                rows.append(
                    schema.SystemMetric(
                        pod_name=bucket,
                        metric=f"{prefix}_{series.name}",
                        peak=total,
                        mean=None,
                    )
                )
        except Exception as e:  # best-effort: keep the other series
            print(
                f"Warning: bucket series '{prefix}_{series.name}' failed, "
                f"its column N/A: {e}"
            )
    return rows


def collect_node_capacity(
    client, *, project, cluster, start_epoch, end_epoch, period=60
) -> list:
    """Peak node allocatable CPU/memory for the cluster, one SystemMetric each.

    Max over the cluster's nodes, so the (larger) benchmark pool wins over
    any small system node pool.
    """
    rows = []
    for series in NODE_SERIES:
        try:
            request = _build_request(
                project, series, cluster, start_epoch, end_epoch, period
            )
            peak = None
            for ts in client.list_time_series(request):
                for p in ts.points:
                    v = _point_value(p)
                    if peak is None or v > peak:
                        peak = v
            if peak is not None:
                rows.append(
                    schema.SystemMetric(
                        pod_name=cluster, metric=series.name, peak=peak, mean=None
                    )
                )
        except Exception as e:  # best-effort: keep the other series
            print(f"Warning: node series '{series.name}' failed, its column N/A: {e}")
    return rows


def assemble_rows(
    client,
    storage_client,
    *,
    project,
    run_id,
    checkpoint_bucket,
    dataset_bucket,
    restore_rows,
    start_epoch,
    end_epoch,
    cluster=None,
    period=60,
) -> list:
    """Pod gauges + node capacity + per-bucket totals + du sizes, as one row list."""
    rows = collect(
        client,
        project=project,
        run_id=run_id,
        start_epoch=start_epoch,
        end_epoch=end_epoch,
        period=period,
    )
    if cluster:
        rows += collect_node_capacity(
            client,
            project=project,
            cluster=cluster,
            start_epoch=start_epoch,
            end_epoch=end_epoch,
            period=period,
        )
    if checkpoint_bucket:
        rows += collect_bucket_totals(
            client,
            project=project,
            bucket=checkpoint_bucket,
            prefix="checkpoint",
            start_epoch=start_epoch,
            end_epoch=end_epoch,
            period=period,
        )
    if dataset_bucket:
        rows += collect_bucket_totals(
            client,
            project=project,
            bucket=dataset_bucket,
            prefix="dataset",
            start_epoch=start_epoch,
            end_epoch=end_epoch,
            period=period,
        )
    if storage_client is not None:
        from metrics import sizes

        # Best-effort; must not discard the rows already collected above.
        try:
            loc = sizes.restored_checkpoint_location(restore_rows or [])
            rows += sizes.size_rows(
                storage_client, dataset_bucket=dataset_bucket, restored_location=loc
            )
        except Exception as e:
            print(f"Warning: du sizes failed, size columns N/A: {e}")
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
    parser.add_argument("--checkpoint-bucket")
    parser.add_argument("--dataset-bucket")
    parser.add_argument(
        "--cluster",
        help="GKE cluster name; scopes node allocatable-capacity queries. "
        "Omit to skip capacity (utilization columns stay N/A).",
    )
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
        storage_client = None
        try:
            from google.cloud import storage

            storage_client = storage.Client(project=args.project)
        except Exception as e:  # sizes become N/A, run continues
            print(f"Warning: storage client unavailable, size columns N/A: {e}")
        restore_rows = []
        try:
            restore_rows = raw_store.read_raw_metrics(args.out_dir).restore_rows
        except Exception as e:
            print(f"Warning: could not read restore rows for checkpoint du: {e}")
        rows = assemble_rows(
            client,
            storage_client,
            project=args.project,
            run_id=args.run_id,
            checkpoint_bucket=args.checkpoint_bucket,
            dataset_bucket=args.dataset_bucket,
            restore_rows=restore_rows,
            start_epoch=_to_epoch(args.start_time),
            end_epoch=_to_epoch(args.end_time),
            cluster=args.cluster,
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
