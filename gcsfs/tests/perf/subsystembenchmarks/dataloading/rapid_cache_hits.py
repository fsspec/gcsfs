"""Post-run Rapid Cache hit-ratio scrape.

A Rapid Cache in RUNNING state is not proof that reads were served from it. This
reads the Anywhere Cache egress metric for each case's timed window and splits it
by the ``anywhere_cache_hit`` label, so a "warm" result that was really served from
the bucket shows up as such instead of being reported as a cache measurement.
Best-effort like the amplification scrape: failures leave the columns blank.
"""

import csv
import logging

from gcsfs.tests.perf.subsystembenchmarks.dataloading import rapid_cache
from gcsfs.tests.perf.subsystembenchmarks.dataloading.amplification import (
    _READ_METHODS,
    _point_value,
    align_interval,
)

_CACHE_SENT = "storage.googleapis.com/anywhere_cache/sent_bytes_count"
HIT_COLS = ("rapid_cache_hit_bytes", "rapid_cache_miss_bytes", "rapid_cache_hit_ratio")
# A warm case is only a cache measurement if nearly all of its bytes were hits.
# Cloud Monitoring metrics on 60 GB WebDataset warm runs show an empirical hit ratio of
# ~87.9% (267.4 GB hits / 304.2 GB total across 8 ranks) due to cold shard tail effects.
# Setting the threshold to 0.80 provides margin against transient cold misses while strictly
# rejecting uncached (0.0%) runs.
DEFAULT_MIN_WARM_HIT_RATIO = 0.8



def bucket_cache_bytes(client, project, bucket, start_epoch, end_epoch, period=60):
    """Return (hit_bytes, miss_bytes) read from bucket in the window, or None if no data."""
    methods = " OR ".join(f'metric.labels.method = "{m}"' for m in _READ_METHODS)
    s, e = align_interval(start_epoch, end_epoch, period)
    request = {
        "name": f"projects/{project}",
        "filter": (
            f'metric.type = "{_CACHE_SENT}" AND resource.type = "gcs_bucket" '
            f'AND resource.labels.bucket_name = "{bucket}" AND ({methods})'
        ),
        "interval": {"start_time": {"seconds": s}, "end_time": {"seconds": e}},
        "aggregation": {
            "alignment_period": {"seconds": period},
            "per_series_aligner": "ALIGN_DELTA",
        },
    }
    hit = miss = 0.0
    found = False
    for ts in client.list_time_series(request):
        is_hit = str(ts.metric.labels.get("anywhere_cache_hit", "")).lower() == "true"
        for point in ts.points:
            found = True
            if is_hit:
                hit += _point_value(point)
            else:
                miss += _point_value(point)
    return (hit, miss) if found else None


def enrich_csv(csv_path, project, *, client):
    """Add hit/miss columns to Rapid Cache rows; return buckets still lacking data."""
    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        fieldnames = list(reader.fieldnames or [])
    for column in HIT_COLS:
        if column not in fieldnames:
            fieldnames.append(column)

    missing = []
    for row in rows:
        bucket = row.get("gcs_bucket_name")
        if not bucket or not rapid_cache.is_rapid_cache_bucket_type(
            row.get("bucket_type")
        ):
            continue
        if row.get("rapid_cache_hit_ratio") not in (None, ""):
            continue
        try:
            totals = bucket_cache_bytes(
                client,
                project,
                bucket,
                int(float(row["measurement_window_start_unix_seconds"])),
                int(float(row["measurement_window_end_unix_seconds"])),
            )
        except Exception as exc:
            logging.warning("rapid cache hit scrape failed for %s: %s", bucket, exc)
            totals = None
        if totals is None or sum(totals) <= 0:
            missing.append(bucket)
            continue
        hit, miss = totals
        row["rapid_cache_hit_bytes"] = str(int(hit))
        row["rapid_cache_miss_bytes"] = str(int(miss))
        row["rapid_cache_hit_ratio"] = str(hit / (hit + miss))

    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    return tuple(sorted(set(missing)))


def warm_rows_below_hit_ratio(csv_path, min_ratio):
    """Return (case_id, hit_ratio) for rapid_cache_warm rows below min_ratio.

    A row without a measured ratio is reported with None: it cannot be shown warm.
    """
    with open(csv_path, newline="") as f:
        rows = list(csv.DictReader(f))
    failures = []
    for row in rows:
        if row.get("bucket_type") != "rapid_cache_warm":
            continue
        raw = row.get("rapid_cache_hit_ratio")
        ratio = float(raw) if raw not in (None, "") else None
        if ratio is None or ratio < min_ratio:
            failures.append((row.get("benchmark_case_id", ""), ratio))
    return failures
