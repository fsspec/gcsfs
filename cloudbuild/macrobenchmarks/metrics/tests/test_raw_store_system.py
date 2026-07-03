from metrics import raw_store, schema


def test_system_metrics_roundtrip(tmp_path):
    rows = [
        schema.SystemMetric(pod_name="p0", metric="cpu", peak=3.0, mean=1.5),
        schema.SystemMetric(pod_name="p1", metric="memory", peak=1024.0, mean=None),
    ]
    raw_store.write_system_metrics(rows, str(tmp_path))
    tables = raw_store.read_raw_metrics(str(tmp_path))
    assert tables.system_rows == [
        {"pod_name": "p0", "metric": "cpu", "peak": 3.0, "mean": 1.5},
        {"pod_name": "p1", "metric": "memory", "peak": 1024.0, "mean": None},
    ]


def test_absent_system_metrics_read_as_empty(tmp_path):
    tables = raw_store.read_raw_metrics(str(tmp_path))
    assert tables.system_rows == []
