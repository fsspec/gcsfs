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

from gcsfs.tests.perf.subsystembenchmarks.dataloading import rapid_cache

_SENT = "storage.googleapis.com/network/sent_bytes_count"
_REQ = "storage.googleapis.com/api/request_count"
_CACHE_SENT = "storage.googleapis.com/anywhere_cache/sent_bytes_count"
_READ_METHODS = ("ReadObject", "BidiReadObject")
_HIT_RATIO = "rapid_cache_hit_ratio"
_NEW_COLS = [
    "dataset_read_bytes",
    "dataset_read_request_count",
    "dataset_read_amplification_ratio",
    "checkpoint_read_bytes",
    "checkpoint_read_request_count",
    "checkpoint_read_amplification_ratio",
    _HIT_RATIO,
]
# report.generate_csv writes "N/A" for extra_info keys a case did not publish.
_MISSING_VALUES = (None, "", "N/A")


def _has_value(value):
    return value not in _MISSING_VALUES


@dataclasses.dataclass(frozen=True)
class EnrichmentResult:
    eligible: int
    enriched: int
    missing_buckets: tuple

    @property
    def complete(self):
        return not self.missing_buckets


# GCS 60s DELTA counters and BidiReadObject stream-close counts can flush 1 to 2
# minutes after window_end. Each case uses an isolated bucket with no post-window
# object reads, so padding end_epoch captures trailing flushes cleanly.
# 120s completes most rows. ray_data on zonal and RCU buckets can still come up
# short (up to about 1% of bytes and 40% of request_count) because BidiReadObject
# CANCELLED counts keep landing for up to about 13 minutes after window_end.
_END_PAD_SECONDS = 120
# The padded window can end up to 179s after window_end, and these metrics take up
# to 210s to become visible in Cloud Monitoring.
DEFAULT_SCRAPE_WAIT_SECONDS = 420


def align_interval(start_epoch, end_epoch, period=60, end_pad=_END_PAD_SECONDS):
    start = (int(start_epoch) // period) * period
    end = ((int(end_epoch) + end_pad + period - 1) // period) * period
    return start, end


def _point_value(point):
    v = point.value
    if getattr(v, "double_value", 0.0):
        return float(v.double_value)
    return float(getattr(v, "int64_value", 0))


def _list_series(client, project, filter_, start_epoch, end_epoch, period):
    s, e = align_interval(start_epoch, end_epoch, period)
    return client.list_time_series(
        {
            "name": f"projects/{project}",
            "filter": filter_,
            "interval": {"start_time": {"seconds": s}, "end_time": {"seconds": e}},
            "aggregation": {
                "alignment_period": {"seconds": period},
                "per_series_aligner": "ALIGN_DELTA",
            },
        }
    )


def _minute_series(client, project, filter_, start_epoch, end_epoch, period):
    """Per-minute totals keyed by point end time, or None without any points."""
    minutes = {}
    for ts in _list_series(client, project, filter_, start_epoch, end_epoch, period):
        for p in ts.points:
            minute = int(p.interval.end_time.timestamp())
            minutes[minute] = minutes.get(minute, 0.0) + _point_value(p)
    return minutes or None


def _sum_series(client, project, filter_, start_epoch, end_epoch, period):
    minutes = _minute_series(client, project, filter_, start_epoch, end_epoch, period)
    return sum(minutes.values()) if minutes is not None else None


def _minutes_consistent(egress_minutes, req_minutes):
    """False if a minute with reads in one metric has no point in the other.

    Cloud Monitoring omits a dropped DELTA point entirely, while a quiet minute
    reports an explicit 0, so a missing minute means a truncated total.
    """
    for minutes, other in (
        (egress_minutes, req_minutes),
        (req_minutes, egress_minutes),
    ):
        if any(value > 0 and minute not in other for minute, value in minutes.items()):
            return False
    return True


def _read_filter(metric, bucket):
    methods = " OR ".join(f'metric.labels.method = "{m}"' for m in _READ_METHODS)
    return (
        f'metric.type = "{metric}" AND resource.type = "gcs_bucket" '
        f'AND resource.labels.bucket_name = "{bucket}" AND ({methods})'
    )


def bucket_egress_bytes(client, project, bucket, start_epoch, end_epoch, period=60):
    filter_ = _read_filter(_SENT, bucket)
    return _sum_series(client, project, filter_, start_epoch, end_epoch, period)


def bucket_read_requests(client, project, bucket, start_epoch, end_epoch, period=60):
    filter_ = _read_filter(_REQ, bucket)
    return _sum_series(client, project, filter_, start_epoch, end_epoch, period)


def bucket_cache_hit_ratio(client, project, bucket, start_epoch, end_epoch, period=60):
    """Fraction of Anywhere Cache read bytes served as hits, or None without data.

    A RUNNING cache does not prove reads were served from it (e.g. a VM in another
    campus than the cache gets no hits), so warm results carry this as evidence.
    """
    filter_ = _read_filter(_CACHE_SENT, bucket)
    hit = total = 0.0
    for ts in _list_series(client, project, filter_, start_epoch, end_epoch, period):
        is_hit = str(ts.metric.labels.get("anywhere_cache_hit", "")).lower() == "true"
        for p in ts.points:
            value = _point_value(p)
            total += value
            if is_hit:
                hit += value
    return hit / total if total > 0 else None


def _fill_hit_ratio(row, project, client):
    """Best-effort: a missing ratio leaves the column blank and is not retried for."""
    try:
        ratio = bucket_cache_hit_ratio(
            client,
            project,
            row["gcs_bucket_name"],
            int(float(row["measurement_window_start_unix_seconds"])),
            int(float(row["measurement_window_end_unix_seconds"])),
        )
    except Exception as exc:
        logging.warning(
            "rapid cache hit scrape failed for %s: %s", row["gcs_bucket_name"], exc
        )
        return
    if ratio is not None:
        row[_HIT_RATIO] = str(ratio)


def _read_prefix(row):
    scenario = row.get("workload_scenario") or ""
    if "write" in scenario:
        return None
    if _has_value(row.get("dataset_size_bytes")):
        return "dataset"
    if (
        scenario == "checkpoint_read"
        or _has_value(row.get("checkpoint_read_throughput_mean_bytes_per_second"))
    ) and _has_value(row.get("checkpoint_physical_size_bytes")):
        return "checkpoint"
    return None


def has_eligible_rows(csv_path) -> bool:
    try:
        with open(csv_path, newline="") as f:
            return any(
                bool(row.get("gcs_bucket_name")) and _read_prefix(row) is not None
                for row in csv.DictReader(f)
            )
    except OSError:
        return False


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

        # Check if this row is for dataset or checkpoint read
        prefix = _read_prefix(row)
        if not prefix:
            continue

        eligible += 1
        cols_to_check = [
            f"{prefix}_read_bytes",
            f"{prefix}_read_request_count",
            f"{prefix}_read_amplification_ratio",
        ]
        if all(row.get(column) not in (None, "") for column in cols_to_check):
            continue

        try:
            ws = int(float(row["measurement_window_start_unix_seconds"]))
            we = int(float(row["measurement_window_end_unix_seconds"]))
            physical_size = float(
                row[f"{prefix}_size_bytes"]
                if prefix == "dataset"
                else row["checkpoint_physical_size_bytes"]
            )
            # Normalize GCS bytes sent by stored bytes times measured rounds.
            rounds = int(float(row.get("measurement_round_count") or 1))
            egress_minutes = _minute_series(
                client, project, _read_filter(_SENT, bucket), ws, we, 60
            )
            req_minutes = _minute_series(
                client, project, _read_filter(_REQ, bucket), ws, we, 60
            )
            egress = sum(egress_minutes.values()) if egress_minutes else None
            reqs = sum(req_minutes.values()) if req_minutes else None
            is_rapid_cache = rapid_cache.is_rapid_cache_bucket_type(
                row.get("bucket_type")
            )
            if is_rapid_cache and egress in (None, 0.0) and reqs in (None, 0.0):
                # The cache served every read, so the bucket saw no origin reads.
                egress = 0.0
                reqs = 0.0
                ready = True
            else:
                # A read case always reads, so a zero sum means the real point was
                # dropped and only trailing zero-valued DELTA points were found.
                ready = (
                    (egress or 0.0) > 0
                    and (reqs or 0.0) > 0
                    and _minutes_consistent(egress_minutes, req_minutes)
                )
            ideal = physical_size * rounds
            if ready and ideal > 0:
                row[f"{prefix}_read_bytes"] = str(int(egress))
                row[f"{prefix}_read_request_count"] = str(int(reqs))
                row[f"{prefix}_read_amplification_ratio"] = str(egress / ideal)
        except Exception as exc:
            logging.warning("amplification scrape failed for %s: %s", bucket, exc)

        if not all(row.get(column) not in (None, "") for column in cols_to_check):
            missing.append(bucket)

    for row in rows:
        if (
            _read_prefix(row)
            and row.get("gcs_bucket_name")
            and rapid_cache.is_rapid_cache_bucket_type(row.get("bucket_type"))
            and not _has_value(row.get(_HIT_RATIO))
        ):
            _fill_hit_ratio(row, project, client)

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
        default=DEFAULT_SCRAPE_WAIT_SECONDS,
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
