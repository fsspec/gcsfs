"""Post-run GCS read-amplification scrape.

Reads GCS server-side egress and request metrics per bucket epoch window from Cloud Monitoring.
Buckets are strictly per-case to align with Cloud Monitoring's 60s bucket-level grid.
Best-effort: failures leave columns blank without raising to caller.
"""

import argparse
import csv
import dataclasses
import logging
import time

_SENT = "storage.googleapis.com/network/sent_bytes_count"
_REQ = "storage.googleapis.com/api/request_count"
_READ_METHODS = ("ReadObject", "BidiReadObject")
_NEW_COLS = [
    "dataset_read_bytes",
    "dataset_read_request_count",
    "dataset_read_amplification_ratio",
]


@dataclasses.dataclass(frozen=True)
class EnrichmentResult:
    eligible: int
    enriched: int
    missing_buckets: tuple

    @property
    def complete(self):
        return not self.missing_buckets


def align_interval(start_epoch, end_epoch, period=60):
    start = (int(start_epoch) // period) * period
    end = ((int(end_epoch) + period - 1) // period) * period
    return start, end


def _point_value(point):
    v = point.value
    if getattr(v, "double_value", 0.0):
        return float(v.double_value)
    return float(getattr(v, "int64_value", 0))


def _sum_series(client, project, filter_, start_epoch, end_epoch, period):
    s, e = align_interval(start_epoch, end_epoch, period)
    request = {
        "name": f"projects/{project}",
        "filter": filter_,
        "interval": {"start_time": {"seconds": s}, "end_time": {"seconds": e}},
        "aggregation": {
            "alignment_period": {"seconds": period},
            "per_series_aligner": "ALIGN_DELTA",
        },
    }
    total, found = 0.0, False
    for ts in client.list_time_series(request):
        for p in ts.points:
            total += _point_value(p)
            found = True
    return total if found else None


def bucket_egress_bytes(client, project, bucket, start_epoch, end_epoch, period=60):
    methods = " OR ".join(f'metric.labels.method = "{m}"' for m in _READ_METHODS)
    filter_ = (
        f'metric.type = "{_SENT}" AND resource.type = "gcs_bucket" '
        f'AND resource.labels.bucket_name = "{bucket}" AND ({methods})'
    )
    return _sum_series(client, project, filter_, start_epoch, end_epoch, period)


def bucket_read_requests(client, project, bucket, start_epoch, end_epoch, period=60):
    methods = " OR ".join(f'metric.labels.method = "{m}"' for m in _READ_METHODS)
    filter_ = (
        f'metric.type = "{_REQ}" AND resource.type = "gcs_bucket" '
        f'AND resource.labels.bucket_name = "{bucket}" AND ({methods})'
    )
    return _sum_series(client, project, filter_, start_epoch, end_epoch, period)


def enrich_csv(csv_path, project, *, client):
    """Add amplification columns and report complete/missing eligible rows."""
    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        fieldnames = list(reader.fieldnames or [])
    for c in _NEW_COLS:
        if c not in fieldnames:
            fieldnames.append(c)

    eligible = 0
    missing = []
    for row in rows:
        bucket = row.get("gcs_bucket_name")
        if not bucket:
            continue
        eligible += 1
        if all(row.get(column) not in (None, "") for column in _NEW_COLS):
            continue
        try:
            ws = int(float(row["measurement_window_start_unix_seconds"]))
            we = int(float(row["measurement_window_end_unix_seconds"]))
            dataset_size = float(row["dataset_size_bytes"])
            # Normalize GCS bytes sent by stored dataset bytes times measured rounds.
            rounds = int(float(row.get("measurement_round_count") or 1))
            egress = bucket_egress_bytes(client, project, bucket, ws, we)
            reqs = bucket_read_requests(client, project, bucket, ws, we)
            if egress is not None:
                row["dataset_read_bytes"] = str(int(egress))
                ideal = dataset_size * rounds
                if ideal:
                    row["dataset_read_amplification_ratio"] = str(egress / ideal)
            if reqs is not None:
                row["dataset_read_request_count"] = str(int(reqs))
        except Exception as exc:
            logging.warning("amplification scrape failed for %s: %s", bucket, exc)
        if not all(row.get(column) not in (None, "") for column in _NEW_COLS):
            missing.append(bucket)

    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    missing_buckets = tuple(sorted(set(missing)))
    return EnrichmentResult(
        eligible=eligible,
        enriched=eligible - len(missing),
        missing_buckets=missing_buckets,
    )


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Merge GCS read-amplification into CSV."
    )
    parser.add_argument("--csv", required=True)
    parser.add_argument("--project", required=True)
    parser.add_argument(
        "--wait-seconds",
        type=int,
        default=300,
        help="sleep before scrape so Cloud Monitoring egress lands",
    )
    args = parser.parse_args(argv)
    try:
        from google.cloud import monitoring_v3
    except ImportError as e:
        print(f"Warning: google-cloud-monitoring unavailable, columns N/A: {e}")
        return
    if args.wait_seconds:
        time.sleep(args.wait_seconds)
    client = monitoring_v3.MetricServiceClient()
    result = enrich_csv(args.csv, args.project, client=client)
    print(
        f"Amplification enriched {result.enriched}/{result.eligible} rows in {args.csv}; "
        f"missing={list(result.missing_buckets)}"
    )


if __name__ == "__main__":
    main()
