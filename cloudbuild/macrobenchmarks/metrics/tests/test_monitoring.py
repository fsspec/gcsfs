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
    """Returns a canned series list per metric.type in the request filter."""

    def __init__(self, by_metric_type):
        self.by_metric_type = by_metric_type
        self.requests = []

    def list_time_series(self, request):
        self.requests.append(request)
        for metric_type, series in self.by_metric_type.items():
            if metric_type in request["filter"]:
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
    req = monitoring._build_request("proj", "run", cpu, 100, 700, 60)
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


def test_to_epoch_handles_zulu():

    assert monitoring._to_epoch("1970-01-01T00:01:00Z") == 60


def test_collect_emits_one_row_per_pod_and_series():
    client = _FakeClient(
        {
            "core_usage_time": [_series("p0", [1.0, 5.0]), _series("p1", [2.0, 2.0])],
            "memory/used_bytes": [_series("p0", [1024.0])],
            "network/received_bytes_count": [_series("p0", [10.0, 20.0])],
            "network/sent_bytes_count": [_series("p0", [4.0, 6.0])],
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

    client = _FakeClient({"core_usage_time": [_series("p0", [2.0, 4.0])]})
    rows = monitoring.collect(
        client, project="proj", run_id="run", start_epoch=0, end_epoch=600
    )
    raw_store.write_system_metrics(rows, str(tmp_path))
    tables = raw_store.read_raw_metrics(str(tmp_path))
    cpu = [r for r in tables.system_rows if r["metric"] == "cpu"]
    assert cpu and cpu[0]["peak"] == 4.0 and cpu[0]["mean"] == 3.0
