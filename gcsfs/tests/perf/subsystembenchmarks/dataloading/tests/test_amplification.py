import csv
import datetime
import logging
import types

import pytest

from gcsfs.tests.perf.subsystembenchmarks.dataloading import amplification


def _point(value, end=1020, int64=False):
    """A Monitoring point. Real GCS read metrics are INT64, so int64=True is closest."""
    return types.SimpleNamespace(
        value=types.SimpleNamespace(
            double_value=0.0 if int64 else value,
            int64_value=int(value) if int64 else 0,
        ),
        interval=types.SimpleNamespace(
            end_time=datetime.datetime.fromtimestamp(end, tz=datetime.timezone.utc)
        ),
    )


class _TimeSeries:
    def __init__(self, points):
        self.points = points


class _FakeClient:
    """Mock Monitoring client returning a canned time series."""

    def __init__(self, series):
        self._series = series

    def list_time_series(self, request):
        return self._series


class _SequencedClient:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = 0

    def list_time_series(self, request):
        response = self.responses[self.calls]
        self.calls += 1
        return response


class _RecordingClient:
    def __init__(self):
        self.requests = []

    def list_time_series(self, request):
        self.requests.append(request)
        return []


def _write_csv(path, rows, fieldnames):
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


_FIELDS = [
    "benchmark_case_id",
    "gcs_bucket_name",
    "measurement_window_start_unix_seconds",
    "measurement_window_end_unix_seconds",
    "dataset_size_bytes",
    "measurement_round_count",
]


def test_bucket_egress_bytes_filters_to_object_reads():
    client = _RecordingClient()

    amplification.bucket_egress_bytes(client, "proj", "bucket", 100, 160)

    filter_ = client.requests[0]["filter"]
    assert 'metric.labels.method = "ReadObject"' in filter_
    assert 'metric.labels.method = "BidiReadObject"' in filter_


def test_align_interval_pads_end_window_for_late_delta_flushes():
    assert amplification.align_interval(1010, 1054, period=60) == (960, 1200)
    assert amplification.align_interval(1010, 1054, period=60, end_pad=0) == (960, 1080)


def test_align_interval_rounds_a_minute_boundary_end_strictly_up():
    # window_end is truncated to whole seconds, so 1020 still read during
    # (1020, 1080] and needs two trailing minutes after that one.
    assert amplification.align_interval(1000, 1020, period=60) == (960, 1200)


def test_bucket_queries_include_padded_trailing_minutes():
    client = _RecordingClient()

    amplification.bucket_egress_bytes(client, "proj", "bucket", 1010, 1054)
    amplification.bucket_read_requests(client, "proj", "bucket", 1010, 1054)
    amplification.bucket_cache_hit_ratio(client, "proj", "bucket", 1010, 1054)

    assert len(client.requests) == 3
    for req in client.requests:
        assert req["interval"] == {
            "start_time": {"seconds": 960},
            "end_time": {"seconds": 1200},
        }


def test_enrich_csv_does_not_count_a_row_with_no_monitoring_data(tmp_path):
    """Verify that rows without Monitoring data are marked un-enriched with missing buckets."""
    csv_path = tmp_path / "results.csv"
    _write_csv(
        csv_path,
        [
            {
                "benchmark_case_id": "case-a",
                "gcs_bucket_name": "b1",
                "measurement_window_start_unix_seconds": "1000",
                "measurement_window_end_unix_seconds": "1060",
                "dataset_size_bytes": "500",
                "measurement_round_count": "1",
            }
        ],
        _FIELDS,
    )
    result = amplification.enrich_csv(str(csv_path), "proj", client=_FakeClient([]))
    assert result.eligible == 1
    assert result.enriched == 0
    assert result.missing_buckets == ("b1",)
    with open(csv_path, newline="") as f:
        row = next(csv.DictReader(f))
    assert row["dataset_read_bytes"] == ""
    assert row["dataset_read_amplification_ratio"] == ""


def test_enrich_csv_counts_a_row_with_monitoring_data(tmp_path):
    csv_path = tmp_path / "results.csv"
    _write_csv(
        csv_path,
        [
            {
                "benchmark_case_id": "case-a",
                "gcs_bucket_name": "b1",
                "measurement_window_start_unix_seconds": "1000",
                "measurement_window_end_unix_seconds": "1060",
                "dataset_size_bytes": "500",
                "measurement_round_count": "1",
            }
        ],
        _FIELDS,
    )
    client = _FakeClient([_TimeSeries([_point(1000.0)])])
    result = amplification.enrich_csv(str(csv_path), "proj", client=client)
    assert result.eligible == 1
    assert result.enriched == 1
    assert result.missing_buckets == ()
    with open(csv_path, newline="") as f:
        row = next(csv.DictReader(f))
    assert row["dataset_read_bytes"] == "1000"
    assert float(row["dataset_read_amplification_ratio"]) == 2.0


def test_enrich_csv_logs_instead_of_printing_on_row_failure(tmp_path, caplog):
    csv_path = tmp_path / "results.csv"
    _write_csv(
        csv_path,
        [
            {
                "benchmark_case_id": "case-a",
                "gcs_bucket_name": "b1",
                "measurement_window_start_unix_seconds": "not-a-number",
                "measurement_window_end_unix_seconds": "1060",
                "dataset_size_bytes": "500",
                "measurement_round_count": "1",
            }
        ],
        _FIELDS,
    )
    with caplog.at_level(logging.WARNING):
        result = amplification.enrich_csv(
            str(csv_path), "proj", client=_FakeClient([_TimeSeries([_point(1.0)])])
        )
    assert result.enriched == 0
    assert result.missing_buckets == ("b1",)
    assert result.failed_buckets == ("b1",)
    assert any("amplification scrape failed" in rec.message for rec in caplog.records)


def test_enrich_csv_retry_completes_only_the_missing_row(tmp_path):
    csv_path = tmp_path / "results.csv"
    _write_csv(
        csv_path,
        [
            {
                "benchmark_case_id": "case-a",
                "gcs_bucket_name": "b1",
                "measurement_window_start_unix_seconds": "1000",
                "measurement_window_end_unix_seconds": "1060",
                "dataset_size_bytes": "500",
                "measurement_round_count": "1",
            }
        ],
        _FIELDS,
    )
    series = [_TimeSeries([_point(1000.0)])]
    client = _SequencedClient([[], [], series, series])

    first = amplification.enrich_csv(str(csv_path), "proj", client=client)
    second = amplification.enrich_csv(str(csv_path), "proj", client=client)

    assert first.missing_buckets == ("b1",)
    assert second.missing_buckets == ()
    assert second.enriched == 1
    assert client.calls == 4


# report.generate_csv fills keys a case did not publish with "N/A", so a mixed
# write/read checkpointing CSV carries "N/A" (not "") in the other scenario's
# throughput column.
@pytest.mark.parametrize("missing", ["", "N/A"])
def test_enrich_csv_checkpoint_read_and_write(tmp_path, missing):
    csv_path = tmp_path / "results.csv"
    fields = [
        "benchmark_case_id",
        "workload_scenario",
        "gcs_bucket_name",
        "measurement_window_start_unix_seconds",
        "measurement_window_end_unix_seconds",
        "checkpoint_physical_size_bytes",
        "checkpoint_read_throughput_mean_bytes_per_second",
        "checkpoint_write_throughput_mean_bytes_per_second",
        "measurement_round_count",
    ]
    _write_csv(
        csv_path,
        [
            {
                "benchmark_case_id": "read-case",
                "workload_scenario": "checkpoint_read",
                "gcs_bucket_name": "b-read",
                "measurement_window_start_unix_seconds": "1000",
                "measurement_window_end_unix_seconds": "1060",
                "checkpoint_physical_size_bytes": "500",
                "checkpoint_read_throughput_mean_bytes_per_second": "250",
                "checkpoint_write_throughput_mean_bytes_per_second": missing,
                "measurement_round_count": "1",
            },
            {
                "benchmark_case_id": "write-case",
                "workload_scenario": "checkpoint_write",
                "gcs_bucket_name": "b-write",
                "measurement_window_start_unix_seconds": "1000",
                "measurement_window_end_unix_seconds": "1060",
                "checkpoint_physical_size_bytes": "500",
                "checkpoint_read_throughput_mean_bytes_per_second": missing,
                "checkpoint_write_throughput_mean_bytes_per_second": "250",
                "measurement_round_count": "1",
            },
        ],
        fields,
    )
    client = _FakeClient([_TimeSeries([_point(1000.0)])])
    result = amplification.enrich_csv(str(csv_path), "proj", client=client)
    assert result.eligible == 1
    assert result.enriched == 1
    assert result.missing_buckets == ()
    with open(csv_path, newline="") as f:
        rows = list(csv.DictReader(f))
    assert rows[0]["checkpoint_read_bytes"] == "1000"
    assert float(rows[0]["checkpoint_read_amplification_ratio"]) == 2.0
    assert rows[1]["checkpoint_read_bytes"] == ""


@pytest.mark.parametrize("bucket_type", ["rapid_cache_cold", "rapid_cache_warm"])
@pytest.mark.parametrize(
    "egress_series,req_series",
    [
        ([], []),
        ([_TimeSeries([_point(0.0)])], []),
        ([], [_TimeSeries([_point(0.0)])]),
        ([_TimeSeries([_point(0.0)])], [_TimeSeries([_point(0.0)])]),
    ],
)
def test_enrich_csv_rapid_cache_treats_empty_or_zero_series_as_zero_origin_egress(
    tmp_path, bucket_type, egress_series, req_series
):
    """When Rapid Cache serves reads from zonal cache, 0 origin egress/reqs emits empty or 0.0 series."""
    csv_path = tmp_path / "results.csv"
    fields = _FIELDS + ["bucket_type"]
    _write_csv(
        csv_path,
        [
            {
                "benchmark_case_id": f"case-{bucket_type}",
                "gcs_bucket_name": f"b-{bucket_type}",
                "bucket_type": bucket_type,
                "measurement_window_start_unix_seconds": "1000",
                "measurement_window_end_unix_seconds": "1060",
                "dataset_size_bytes": "500",
                "measurement_round_count": "3",
            }
        ],
        fields,
    )
    client = _SequencedClient([egress_series, req_series])
    result = amplification.enrich_csv(str(csv_path), "proj", client=client)
    assert result.eligible == 1
    assert result.enriched == 1
    assert result.missing_buckets == ()
    with open(csv_path, newline="") as f:
        row = next(csv.DictReader(f))
    assert row["dataset_read_bytes"] == "0"
    assert row["dataset_read_request_count"] == "0"
    assert float(row["dataset_read_amplification_ratio"]) == 0.0


@pytest.mark.parametrize(
    "pass1_egress,pass1_reqs",
    [
        ([], [_TimeSeries([_point(10.0)])]),
        ([_TimeSeries([_point(1500.0)])], []),
    ],
)
def test_enrich_csv_rapid_cache_asymmetric_none_triggers_retry_without_torn_row(
    tmp_path, pass1_egress, pass1_reqs
):
    """If one metric is >0 while the other is None due to ingestion lag, keep row blank and retry."""
    csv_path = tmp_path / "results.csv"
    fields = _FIELDS + ["bucket_type"]
    _write_csv(
        csv_path,
        [
            {
                "benchmark_case_id": "case-cold",
                "gcs_bucket_name": "b-cold",
                "bucket_type": "rapid_cache_cold",
                "measurement_window_start_unix_seconds": "1000",
                "measurement_window_end_unix_seconds": "1060",
                "dataset_size_bytes": "500",
                "measurement_round_count": "3",
            }
        ],
        fields,
    )
    egress_series = [_TimeSeries([_point(1500.0)])]
    req_series = [_TimeSeries([_point(10.0)])]
    # Each pass ends with one (here empty) Rapid Cache hit-ratio query.
    client = _SequencedClient(
        [pass1_egress, pass1_reqs, [], egress_series, req_series, []]
    )

    first = amplification.enrich_csv(str(csv_path), "proj", client=client)
    assert first.missing_buckets == ("b-cold",)
    with open(csv_path, newline="") as f:
        row_pass1 = next(csv.DictReader(f))
    assert row_pass1["dataset_read_bytes"] == ""
    assert row_pass1["dataset_read_request_count"] == ""
    assert row_pass1["dataset_read_amplification_ratio"] == ""

    second = amplification.enrich_csv(str(csv_path), "proj", client=client)
    assert second.missing_buckets == ()
    with open(csv_path, newline="") as f:
        row = next(csv.DictReader(f))
    assert row["dataset_read_bytes"] == "1500"
    assert row["dataset_read_request_count"] == "10"
    assert float(row["dataset_read_amplification_ratio"]) == 1.0


@pytest.mark.parametrize("bucket_type", ["zonal", "regional", "standard_rcu_cold"])
@pytest.mark.parametrize(
    "egress_value,req_value",
    [(0.0, 1447.0), (1000.0, 0.0), (0.0, 0.0)],
    ids=["zero-bytes", "zero-requests", "both-zero"],
)
def test_enrich_csv_keeps_zero_sums_missing_outside_rapid_cache(
    tmp_path, bucket_type, egress_value, req_value
):
    """Trailing zero-valued DELTA points after a dropped point must not pass as data."""
    csv_path = tmp_path / "results.csv"
    _write_csv(
        csv_path,
        [
            {
                "benchmark_case_id": f"case-{bucket_type}",
                "gcs_bucket_name": f"b-{bucket_type}",
                "bucket_type": bucket_type,
                "measurement_window_start_unix_seconds": "1000",
                "measurement_window_end_unix_seconds": "1060",
                "dataset_size_bytes": "500",
                "measurement_round_count": "1",
            }
        ],
        _FIELDS + ["bucket_type"],
    )
    client = _SequencedClient(
        [[_TimeSeries([_point(egress_value)])], [_TimeSeries([_point(req_value)])]]
    )

    result = amplification.enrich_csv(str(csv_path), "proj", client=client)

    assert result.enriched == 0
    assert result.missing_buckets == (f"b-{bucket_type}",)
    with open(csv_path, newline="") as f:
        row = next(csv.DictReader(f))
    assert row["dataset_read_bytes"] == ""
    assert row["dataset_read_request_count"] == ""
    assert row["dataset_read_amplification_ratio"] == ""


def _dataset_row(bucket_type, bucket="b1"):
    return {
        "benchmark_case_id": f"case-{bucket_type}",
        "gcs_bucket_name": bucket,
        "bucket_type": bucket_type,
        "measurement_window_start_unix_seconds": "1000",
        "measurement_window_end_unix_seconds": "1060",
        "dataset_size_bytes": "500",
        "measurement_round_count": "1",
    }


def _read_row(csv_path):
    with open(csv_path, newline="") as f:
        return next(csv.DictReader(f))


@pytest.mark.parametrize(
    "egress_value,req_value",
    [(0.0, 10.0), (1500.0, 0.0)],
    ids=["zero-bytes", "zero-requests"],
)
def test_enrich_csv_rapid_cache_keeps_one_sided_zero_missing(
    tmp_path, egress_value, req_value
):
    csv_path = tmp_path / "results.csv"
    _write_csv(csv_path, [_dataset_row("rapid_cache_cold")], _FIELDS + ["bucket_type"])
    # Egress, requests, then the (empty) Rapid Cache hit-ratio query.
    client = _SequencedClient(
        [
            [_TimeSeries([_point(egress_value)])],
            [_TimeSeries([_point(req_value)])],
            [],
        ]
    )

    result = amplification.enrich_csv(str(csv_path), "proj", client=client)

    assert result.missing_buckets == ("b1",)
    assert _read_row(csv_path)["dataset_read_bytes"] == ""


def test_enrich_csv_keeps_a_row_with_a_dropped_minute_missing(tmp_path):
    """Requests at 1020 with no egress point there means the egress point was dropped."""
    csv_path = tmp_path / "results.csv"
    _write_csv(csv_path, [_dataset_row("zonal")], _FIELDS + ["bucket_type"])
    egress = [_TimeSeries([_point(200.0, end=1080, int64=True)])]
    reqs = [
        _TimeSeries(
            [_point(7.0, end=1020, int64=True), _point(3.0, end=1080, int64=True)]
        )
    ]

    result = amplification.enrich_csv(
        str(csv_path), "proj", client=_SequencedClient([egress, reqs])
    )

    assert result.missing_buckets == ("b1",)
    # A dropped point is missing data, not a scrape error.
    assert result.failed_buckets == ()
    row = _read_row(csv_path)
    assert row["dataset_read_bytes"] == ""
    assert row["dataset_read_request_count"] == ""


def _minute_points(values):
    return [_TimeSeries([_point(value, end=end, int64=True) for end, value in values])]


def test_enrich_csv_keeps_a_minute_dropped_from_both_metrics_missing(tmp_path):
    """Reads at 1020 and 1140 with no point at all for 1080 means 1080 was dropped."""
    csv_path = tmp_path / "results.csv"
    _write_csv(csv_path, [_dataset_row("zonal")], _FIELDS + ["bucket_type"])
    egress = _minute_points([(1020, 500.0), (1140, 500.0)])
    reqs = _minute_points([(1020, 5.0), (1140, 5.0)])

    result = amplification.enrich_csv(
        str(csv_path), "proj", client=_SequencedClient([egress, reqs])
    )

    assert result.missing_buckets == ("b1",)
    assert _read_row(csv_path)["dataset_read_bytes"] == ""


def test_enrich_csv_accepts_an_idle_pause_with_explicit_zeros(tmp_path):
    """An idle minute reports explicit 0s, so reads resuming after it are complete."""
    csv_path = tmp_path / "results.csv"
    _write_csv(csv_path, [_dataset_row("zonal")], _FIELDS + ["bucket_type"])
    egress = _minute_points([(1020, 500.0), (1080, 0.0), (1140, 500.0)])
    reqs = _minute_points([(1020, 5.0), (1080, 0.0), (1140, 5.0)])

    result = amplification.enrich_csv(
        str(csv_path), "proj", client=_SequencedClient([egress, reqs])
    )

    assert result.missing_buckets == ()
    row = _read_row(csv_path)
    assert row["dataset_read_bytes"] == "1000"
    assert row["dataset_read_request_count"] == "10"


def test_enrich_csv_accepts_explicit_zero_minutes(tmp_path):
    """A quiet minute reports an explicit 0, unlike a dropped one."""
    csv_path = tmp_path / "results.csv"
    _write_csv(csv_path, [_dataset_row("zonal")], _FIELDS + ["bucket_type"])
    egress = [
        _TimeSeries(
            [_point(800.0, end=1020, int64=True), _point(200.0, end=1080, int64=True)]
        )
    ]
    reqs = [
        _TimeSeries(
            [_point(9.0, end=1020, int64=True), _point(0.0, end=1080, int64=True)]
        )
    ]

    result = amplification.enrich_csv(
        str(csv_path), "proj", client=_SequencedClient([egress, reqs])
    )

    assert result.missing_buckets == ()
    row = _read_row(csv_path)
    assert row["dataset_read_bytes"] == "1000"
    assert row["dataset_read_request_count"] == "9"
    assert float(row["dataset_read_amplification_ratio"]) == 2.0


class _CacheSeries(_TimeSeries):
    def __init__(self, hit, points):
        super().__init__(points)
        self.metric = types.SimpleNamespace(
            labels={"anywhere_cache_hit": "true" if hit else "false"}
        )


class _MetricClient:
    """Routes each query to a canned series list by its metric type."""

    def __init__(self, by_metric):
        self.by_metric = by_metric

    def list_time_series(self, request):
        for metric, series in self.by_metric.items():
            if f'metric.type = "{metric}"' in request["filter"]:
                return series
        return []


def _rapid_cache_row(bucket_type):
    return {
        "benchmark_case_id": f"case-{bucket_type}",
        "gcs_bucket_name": f"b-{bucket_type}",
        "bucket_type": bucket_type,
        "measurement_window_start_unix_seconds": "1000",
        "measurement_window_end_unix_seconds": "1060",
        "dataset_size_bytes": "500",
        "measurement_round_count": "1",
    }


def test_enrich_csv_records_rapid_cache_hit_ratio(tmp_path):
    csv_path = tmp_path / "results.csv"
    _write_csv(
        csv_path,
        [_rapid_cache_row("rapid_cache_warm")],
        _FIELDS + ["bucket_type"],
    )
    client = _MetricClient(
        {
            amplification._CACHE_SENT: [
                _CacheSeries(True, [_point(300.0), _point(600.0)]),
                _CacheSeries(False, [_point(100.0)]),
            ],
        }
    )
    amplification.enrich_csv(str(csv_path), "proj", client=client)
    with open(csv_path, newline="") as f:
        row = next(csv.DictReader(f))
    assert float(row["rapid_cache_hit_ratio"]) == 0.9


def test_enrich_csv_leaves_hit_ratio_blank_without_data_or_for_other_buckets(tmp_path):
    csv_path = tmp_path / "results.csv"
    regional = dict(_rapid_cache_row("regional"), gcs_bucket_name="b-reg")
    _write_csv(
        csv_path,
        [_rapid_cache_row("rapid_cache_cold"), regional],
        _FIELDS + ["bucket_type"],
    )
    hits = [_CacheSeries(True, [_point(1.0)])]
    client = _MetricClient(
        {
            amplification._SENT: [_TimeSeries([_point(500.0)])],
            amplification._REQ: [_TimeSeries([_point(5.0)])],
        }
    )
    result = amplification.enrich_csv(str(csv_path), "proj", client=client)
    # A missing hit ratio does not make the amplification columns incomplete.
    assert result.missing_buckets == ()
    with open(csv_path, newline="") as f:
        rows = list(csv.DictReader(f))
    assert [r["rapid_cache_hit_ratio"] for r in rows] == ["", ""]

    client.by_metric[amplification._CACHE_SENT] = hits
    amplification.enrich_csv(str(csv_path), "proj", client=client)
    with open(csv_path, newline="") as f:
        rows = list(csv.DictReader(f))
    assert [r["rapid_cache_hit_ratio"] for r in rows] == ["1.0", ""]


@pytest.mark.parametrize("bucket_type", ["standard_rcu_warm", "hns_rcu_cold"])
def test_enrich_csv_keeps_rcu_rows_without_monitoring_data_missing(
    tmp_path, bucket_type
):
    """RCU reads are ordinary bucket reads, so no data is missing data, not 0."""
    csv_path = tmp_path / "results.csv"
    _write_csv(csv_path, [_rapid_cache_row(bucket_type)], _FIELDS + ["bucket_type"])
    client = _RecordingClient()

    result = amplification.enrich_csv(str(csv_path), "proj", client=client)

    assert result.missing_buckets == (f"b-{bucket_type}",)
    # No Anywhere Cache hit-ratio query either.
    assert not [r for r in client.requests if amplification._CACHE_SENT in r["filter"]]


def test_hit_ratio_skipped_for_write_rows(tmp_path):
    csv_path = tmp_path / "results.csv"
    fields = [
        "benchmark_case_id",
        "workload_scenario",
        "gcs_bucket_name",
        "bucket_type",
        "measurement_window_start_unix_seconds",
        "measurement_window_end_unix_seconds",
        "checkpoint_physical_size_bytes",
        "checkpoint_read_throughput_mean_bytes_per_second",
        "checkpoint_write_throughput_mean_bytes_per_second",
        "measurement_round_count",
    ]
    _write_csv(
        csv_path,
        [
            {
                "benchmark_case_id": "read-case",
                "workload_scenario": "checkpoint_read",
                "gcs_bucket_name": "b-read",
                "bucket_type": "rapid_cache_warm",
                "measurement_window_start_unix_seconds": "1000",
                "measurement_window_end_unix_seconds": "1060",
                "checkpoint_physical_size_bytes": "500",
                "checkpoint_read_throughput_mean_bytes_per_second": "250",
                "checkpoint_write_throughput_mean_bytes_per_second": "N/A",
                "measurement_round_count": "1",
            },
            {
                "benchmark_case_id": "write-case",
                "workload_scenario": "checkpoint_write",
                "gcs_bucket_name": "b-write",
                "bucket_type": "rapid_cache_warm",
                "measurement_window_start_unix_seconds": "1000",
                "measurement_window_end_unix_seconds": "1060",
                "checkpoint_physical_size_bytes": "500",
                "checkpoint_read_throughput_mean_bytes_per_second": "N/A",
                "checkpoint_write_throughput_mean_bytes_per_second": "250",
                "measurement_round_count": "1",
            },
        ],
        fields,
    )
    client = _MetricClient(
        {
            amplification._CACHE_SENT: [
                _CacheSeries(True, [_point(900.0)]),
                _CacheSeries(False, [_point(100.0)]),
            ],
        }
    )

    result = amplification.enrich_csv(str(csv_path), "proj", client=client)

    assert result.eligible == 1
    assert result.enriched == 1
    assert result.missing_buckets == ()
    with open(csv_path, newline="") as f:
        rows = list(csv.DictReader(f))
    assert float(rows[0]["rapid_cache_hit_ratio"]) == 0.9
    assert rows[1]["rapid_cache_hit_ratio"] == ""


def test_has_eligible_rows(tmp_path):
    fields = [
        "benchmark_case_id",
        "workload_scenario",
        "gcs_bucket_name",
        "bucket_type",
        "measurement_window_start_unix_seconds",
        "measurement_window_end_unix_seconds",
        "checkpoint_physical_size_bytes",
        "checkpoint_read_throughput_mean_bytes_per_second",
        "checkpoint_write_throughput_mean_bytes_per_second",
        "measurement_round_count",
    ]
    write_row = {
        "benchmark_case_id": "write-case",
        "workload_scenario": "checkpoint_write",
        "gcs_bucket_name": "b-write",
        "bucket_type": "regional",
        "measurement_window_start_unix_seconds": "1000",
        "measurement_window_end_unix_seconds": "1060",
        "checkpoint_physical_size_bytes": "500",
        "checkpoint_read_throughput_mean_bytes_per_second": "N/A",
        "checkpoint_write_throughput_mean_bytes_per_second": "250",
        "measurement_round_count": "1",
    }
    read_row = {
        "benchmark_case_id": "read-case",
        "workload_scenario": "checkpoint_read",
        "gcs_bucket_name": "b-read",
        "bucket_type": "regional",
        "measurement_window_start_unix_seconds": "1000",
        "measurement_window_end_unix_seconds": "1060",
        "checkpoint_physical_size_bytes": "500",
        "checkpoint_read_throughput_mean_bytes_per_second": "250",
        "checkpoint_write_throughput_mean_bytes_per_second": "N/A",
        "measurement_round_count": "1",
    }

    ckpt_csv = tmp_path / "ckpt.csv"
    _write_csv(ckpt_csv, [write_row], fields)
    assert amplification.has_eligible_rows(ckpt_csv) is False

    _write_csv(ckpt_csv, [write_row, read_row], fields)
    assert amplification.has_eligible_rows(ckpt_csv) is True

    dataset_csv = tmp_path / "dataset.csv"
    _write_csv(
        dataset_csv,
        [
            {
                "benchmark_case_id": "case-a",
                "gcs_bucket_name": "b1",
                "measurement_window_start_unix_seconds": "1000",
                "measurement_window_end_unix_seconds": "1060",
                "dataset_size_bytes": "500",
                "measurement_round_count": "1",
            }
        ],
        _FIELDS,
    )
    assert amplification.has_eligible_rows(dataset_csv) is True
