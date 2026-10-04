import csv
import types

from gcsfs.tests.perf.subsystembenchmarks.dataloading import rapid_cache_hits


def _point(value):
    return types.SimpleNamespace(
        value=types.SimpleNamespace(double_value=0.0, int64_value=value)
    )


def _series(hit, values):
    labels = {"anywhere_cache_hit": "true" if hit else "false"}
    return types.SimpleNamespace(
        metric=types.SimpleNamespace(labels=labels),
        points=[_point(v) for v in values],
    )


class _Client:
    def __init__(self, series_by_bucket):
        self.series_by_bucket = series_by_bucket
        self.requests = []

    def list_time_series(self, request):
        self.requests.append(request)
        for bucket, series in self.series_by_bucket.items():
            if f'bucket_name = "{bucket}"' in request["filter"]:
                if isinstance(series, Exception):
                    raise series
                return series
        return []


_FIELDS = [
    "benchmark_case_id",
    "bucket_type",
    "gcs_bucket_name",
    "measurement_window_start_unix_seconds",
    "measurement_window_end_unix_seconds",
]


def _write(path, rows):
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=_FIELDS)
        writer.writeheader()
        writer.writerows(rows)


def _read(path):
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def _row(case, bucket_type, bucket):
    return {
        "benchmark_case_id": case,
        "bucket_type": bucket_type,
        "gcs_bucket_name": bucket,
        "measurement_window_start_unix_seconds": "1000",
        "measurement_window_end_unix_seconds": "1100",
    }


def test_bucket_cache_bytes_splits_hits_from_misses():
    client = _Client({"b": [_series(True, [70, 20]), _series(False, [10])]})

    assert rapid_cache_hits.bucket_cache_bytes(client, "p", "b", 1000, 1100) == (
        90.0,
        10.0,
    )
    filter_ = client.requests[0]["filter"]
    assert "anywhere_cache/sent_bytes_count" in filter_
    assert 'metric.labels.method = "ReadObject"' in filter_
    assert 'metric.labels.method = "BidiReadObject"' in filter_


def test_bucket_cache_bytes_returns_none_without_points():
    assert rapid_cache_hits.bucket_cache_bytes(_Client({}), "p", "b", 1, 2) is None


def test_enrich_csv_adds_ratio_only_to_rapid_cache_rows(tmp_path):
    path = tmp_path / "results.csv"
    _write(
        path,
        [
            _row("warm", "rapid_cache_warm", "wb"),
            _row("std", "standard", "sb"),
        ],
    )
    client = _Client(
        {
            "wb": [_series(True, [95]), _series(False, [5])],
            "sb": [_series(False, [100])],
        }
    )

    missing = rapid_cache_hits.enrich_csv(path, "p", client=client)

    assert missing == ()
    warm, std = _read(path)
    assert warm["rapid_cache_hit_bytes"] == "95"
    assert warm["rapid_cache_miss_bytes"] == "5"
    assert float(warm["rapid_cache_hit_ratio"]) == 0.95
    assert std["rapid_cache_hit_ratio"] == ""
    assert len(client.requests) == 1


def test_enrich_csv_reports_buckets_without_data_or_on_error(tmp_path):
    path = tmp_path / "results.csv"
    _write(
        path,
        [
            _row("a", "rapid_cache_warm", "empty"),
            _row("b", "rapid_cache_cold", "broken"),
        ],
    )
    client = _Client({"broken": RuntimeError("boom")})

    missing = rapid_cache_hits.enrich_csv(path, "p", client=client)

    assert missing == ("broken", "empty")
    assert all(row["rapid_cache_hit_ratio"] == "" for row in _read(path))


def test_warm_rows_below_hit_ratio_flags_uncached_and_unknown_rows(tmp_path):
    path = tmp_path / "results.csv"
    rows = [
        _row("cached", "rapid_cache_warm", "b1"),
        _row("uncached", "rapid_cache_warm", "b2"),
        _row("unknown", "rapid_cache_warm", "b3"),
        _row("cold", "rapid_cache_cold", "b4"),
    ]
    rows[0]["rapid_cache_hit_ratio"] = "0.99"
    rows[1]["rapid_cache_hit_ratio"] = "0.0"
    rows[3]["rapid_cache_hit_ratio"] = "0.0"
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=_FIELDS + ["rapid_cache_hit_ratio"])
        writer.writeheader()
        writer.writerows(rows)

    assert rapid_cache_hits.warm_rows_below_hit_ratio(path, 0.9) == [
        ("uncached", 0.0),
        ("unknown", None),
    ]
