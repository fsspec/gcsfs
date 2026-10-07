import csv
import logging
import types

import pytest

from gcsfs.tests.perf.subsystembenchmarks.dataloading import amplification


def _point(value):
    return types.SimpleNamespace(
        value=types.SimpleNamespace(double_value=value, int64_value=0)
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
