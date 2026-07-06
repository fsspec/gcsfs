from types import SimpleNamespace

from metrics import monitoring


def _point(value):
    return SimpleNamespace(value=SimpleNamespace(double_value=value, int64_value=0))


def _series(pod_name, values):
    return SimpleNamespace(
        resource=SimpleNamespace(labels={"pod_name": pod_name}),
        points=[_point(v) for v in values],
    )


class _FakeClient:
    """Returns a canned series list keyed by the full metric.type in the filter.

    Matches the exact ``metric.type = "<type>"`` token rather than a loose
    substring so a pod series (kubernetes.io/pod/network/sent_bytes_count) does
    not accidentally satisfy a bucket series query
    (storage.googleapis.com/network/sent_bytes_count) or vice-versa.
    """

    def __init__(self, by_metric_type):
        self.by_metric_type = by_metric_type
        self.requests = []

    def list_time_series(self, request):
        self.requests.append(request)
        for metric_type, series in self.by_metric_type.items():
            if f'metric.type = "{metric_type}"' in request["filter"]:
                return iter(series)
        return iter(())


def test_reduce_points():
    assert monitoring.reduce_points([1.0, 3.0, 2.0]) == (3.0, 2.0)
    assert monitoring.reduce_points([]) == (None, None)


def test_point_value_reads_int64_when_double_zero():
    # Test fallback to int64_value when double_value is 0.0.
    point = SimpleNamespace(
        value=SimpleNamespace(double_value=0.0, int64_value=1073741824)
    )
    assert monitoring._point_value(point) == 1073741824.0


def test_build_request_shape():
    # Verify request shape matches MetricServiceClient expectations.
    cpu = monitoring.SERIES[0]
    req = monitoring._build_request("proj", cpu, "run", 100, 700, 60)
    assert req["name"] == "projects/proj"
    assert (
        'metric.type = "kubernetes.io/container/cpu/core_usage_time"' in req["filter"]
    )
    assert 'resource.type = "k8s_container"' in req["filter"]
    assert 'starts_with("run-workload-0-")' in req["filter"]
    assert req["interval"]["start_time"]["seconds"] == 100
    assert req["interval"]["end_time"]["seconds"] == 700
    assert req["aggregation"]["alignment_period"]["seconds"] == 60
    assert req["aggregation"]["per_series_aligner"] == "ALIGN_RATE"


def test_build_request_bucket_filter_and_method():
    req_count = [s for s in monitoring.GCS_BUCKET_SERIES if s.method][0]
    req = monitoring._build_request("proj", req_count, "my-bucket", 0, 600, 60)
    assert 'resource.type = "gcs_bucket"' in req["filter"]
    assert 'resource.labels.bucket_name = "my-bucket"' in req["filter"]
    assert f'metric.labels.method = "{req_count.method}"' in req["filter"]
    assert req["aggregation"]["per_series_aligner"] == "ALIGN_DELTA"


def _bucket_series(values):
    # A gcs_bucket series carries no pod_name label; only points matter here.
    return SimpleNamespace(
        resource=SimpleNamespace(labels={}), points=[_point(v) for v in values]
    )


def test_collect_bucket_totals_sums_all_points():
    client = _FakeClient(
        {
            "storage.googleapis.com/network/sent_bytes_count": [
                _bucket_series([100.0, 200.0]),
                _bucket_series([50.0]),
            ],
            "storage.googleapis.com/api/request_count": [_bucket_series([3.0, 4.0])],
        }
    )
    rows = monitoring.collect_bucket_totals(
        client,
        project="proj",
        bucket="ckpt",
        prefix="checkpoint",
        start_epoch=0,
        end_epoch=600,
    )
    by_metric = {r.metric: r for r in rows}
    assert by_metric["checkpoint_read_bytes"].peak == 350.0
    assert by_metric["checkpoint_read_bytes"].pod_name == "ckpt"
    assert by_metric["checkpoint_read_request_count"].peak == 7.0


def test_collect_bucket_totals_omits_empty_series():
    client = _FakeClient({})  # nothing returned
    rows = monitoring.collect_bucket_totals(
        client,
        project="proj",
        bucket="ds",
        prefix="dataset",
        start_epoch=0,
        end_epoch=600,
    )
    assert rows == []


def test_to_epoch_handles_zulu():

    assert monitoring._to_epoch("1970-01-01T00:01:00Z") == 60


def test_collect_emits_one_row_per_pod_and_series():
    client = _FakeClient(
        {
            "kubernetes.io/container/cpu/core_usage_time": [
                _series("p0", [1.0, 5.0]),
                _series("p1", [2.0, 2.0]),
            ],
            "kubernetes.io/container/memory/used_bytes": [_series("p0", [1024.0])],
            "kubernetes.io/pod/network/received_bytes_count": [
                _series("p0", [10.0, 20.0])
            ],
            "kubernetes.io/pod/network/sent_bytes_count": [_series("p0", [4.0, 6.0])],
        }
    )
    rows = monitoring.collect(
        client, project="proj", run_id="run", start_epoch=0, end_epoch=600
    )
    by_key = {(r.pod_name, r.metric): r for r in rows}
    assert by_key[("p0", "cpu")].peak == 5.0
    assert by_key[("p0", "cpu")].mean == 3.0
    assert by_key[("p1", "cpu")].peak == 2.0
    assert by_key[("p0", "memory")].peak == 1024.0
    assert by_key[("p0", "network_received")].peak == 20.0
    assert by_key[("p0", "network_sent")].peak == 6.0

    assert 'starts_with("run-workload-0-")' in client.requests[0]["filter"]


def test_collect_writes_via_raw_store(tmp_path):
    from metrics import raw_store

    client = _FakeClient(
        {"kubernetes.io/container/cpu/core_usage_time": [_series("p0", [2.0, 4.0])]}
    )
    rows = monitoring.collect(
        client, project="proj", run_id="run", start_epoch=0, end_epoch=600
    )
    raw_store.write_system_metrics(rows, str(tmp_path))
    tables = raw_store.read_raw_metrics(str(tmp_path))
    cpu = [r for r in tables.system_rows if r["metric"] == "cpu"]
    assert cpu and cpu[0]["peak"] == 4.0 and cpu[0]["mean"] == 3.0


def test_assemble_rows_combines_pod_bucket_and_sizes():
    client = _FakeClient(
        {
            "kubernetes.io/container/cpu/core_usage_time": [
                _series("run-workload-0-a", [2.0, 4.0])
            ],
            "storage.googleapis.com/network/sent_bytes_count": [
                _bucket_series([10.0, 20.0])
            ],
            "storage.googleapis.com/api/request_count": [_bucket_series([5.0])],
        }
    )

    class _FakeStorage:
        def list_blobs(self, bucket_name, prefix=""):
            data = {"ds": [("f", 1000)], "ckpt": [("checkpoints/s/x", 400)]}
            for name, size in data.get(bucket_name, []):
                if name.startswith(prefix):
                    yield SimpleNamespace(size=size)

    restore_rows = [{"checkpoint_location": "gs://ckpt/checkpoints/s", "end_time": 1.0}]
    rows = monitoring.assemble_rows(
        client,
        _FakeStorage(),
        project="p",
        run_id="run",
        checkpoint_bucket="ckpt",
        dataset_bucket="ds",
        restore_rows=restore_rows,
        start_epoch=0,
        end_epoch=600,
    )
    metrics = {r.metric for r in rows}
    assert "cpu" in metrics
    assert "checkpoint_read_bytes" in metrics
    assert "dataset_read_bytes" in metrics
    assert "dataset_size_bytes" in metrics
    assert "checkpoint_restored_bytes" in metrics


def test_assemble_rows_without_storage_client_skips_sizes():
    client = _FakeClient(
        {
            "kubernetes.io/container/cpu/core_usage_time": [
                _series("run-workload-0-a", [1.0])
            ]
        }
    )
    rows = monitoring.assemble_rows(
        client,
        None,
        project="p",
        run_id="run",
        checkpoint_bucket=None,
        dataset_bucket=None,
        restore_rows=[],
        start_epoch=0,
        end_epoch=600,
    )
    assert {r.metric for r in rows} == {"cpu"}


class _RaisingClient:
    """Raises for one metric.type, returns a canned series for another."""

    def __init__(self, raise_on, return_for, series):
        self.raise_on = raise_on
        self.return_for = return_for
        self.series = series

    def list_time_series(self, request):
        if f'metric.type = "{self.raise_on}"' in request["filter"]:
            raise RuntimeError("boom")
        if f'metric.type = "{self.return_for}"' in request["filter"]:
            return iter(self.series)
        return iter(())


def test_collect_isolates_a_failing_series():
    # A failure querying one metric type drops only that series, not the rest.
    client = _RaisingClient(
        raise_on="kubernetes.io/container/cpu/core_usage_time",
        return_for="kubernetes.io/container/memory/used_bytes",
        series=[_series("p0", [1024.0])],
    )
    rows = monitoring.collect(
        client, project="proj", run_id="run", start_epoch=0, end_epoch=600
    )
    metrics = {r.metric for r in rows}
    assert "memory" in metrics
    assert "cpu" not in metrics


def test_collect_bucket_totals_isolates_a_failing_series():
    client = _RaisingClient(
        raise_on="storage.googleapis.com/api/request_count",
        return_for="storage.googleapis.com/network/sent_bytes_count",
        series=[_bucket_series([10.0, 20.0])],
    )
    rows = monitoring.collect_bucket_totals(
        client,
        project="proj",
        bucket="ckpt",
        prefix="checkpoint",
        start_epoch=0,
        end_epoch=600,
    )
    by_metric = {r.metric: r for r in rows}
    assert by_metric["checkpoint_read_bytes"].peak == 30.0
    assert "checkpoint_read_request_count" not in by_metric


def test_assemble_rows_isolates_du_failure():
    # A du (list_blobs) failure must not discard the pod metrics already collected.
    client = _FakeClient(
        {
            "kubernetes.io/container/cpu/core_usage_time": [
                _series("run-workload-0-a", [1.0, 2.0])
            ]
        }
    )

    class _BoomStorage:
        def list_blobs(self, *args, **kwargs):
            raise RuntimeError("denied")

    rows = monitoring.assemble_rows(
        client,
        _BoomStorage(),
        project="p",
        run_id="run",
        checkpoint_bucket=None,
        dataset_bucket="ds",
        restore_rows=[],
        start_epoch=0,
        end_epoch=600,
    )
    assert {r.metric for r in rows} == {"cpu"}
