import csv
import logging
import types

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
