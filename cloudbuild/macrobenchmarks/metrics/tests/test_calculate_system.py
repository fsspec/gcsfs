from metrics import calculate


def test_max_across_pods_per_metric():
    rows = [
        {"pod_name": "p0", "metric": "cpu", "peak": 3.0, "mean": 1.0},
        {"pod_name": "p1", "metric": "cpu", "peak": 5.0, "mean": 4.0},
        {"pod_name": "p0", "metric": "memory", "peak": 1024.0, "mean": None},
        {"pod_name": "p1", "metric": "memory", "peak": 2048.0, "mean": None},
        {"pod_name": "p0", "metric": "network_received", "peak": 10.0, "mean": 2.0},
        {"pod_name": "p1", "metric": "network_received", "peak": 8.0, "mean": 3.0},
        {"pod_name": "p0", "metric": "network_sent", "peak": 7.0, "mean": 1.0},
        {"pod_name": "p1", "metric": "network_sent", "peak": 9.0, "mean": 5.0},
    ]
    m = calculate.calc_system_metrics(rows)
    assert m["cpu_usage_peak_cores"] == 5.0
    assert m["cpu_usage_mean_cores"] == 4.0  # max of per-pod means
    assert m["memory_usage_peak_bytes"] == 2048  # int
    assert isinstance(m["memory_usage_peak_bytes"], int)
    assert m["network_received_peak_bytes_per_sec"] == 10.0
    assert m["network_received_mean_bytes_per_sec"] == 3.0
    assert m["network_sent_peak_bytes_per_sec"] == 9.0
    assert m["network_sent_mean_bytes_per_sec"] == 5.0


def test_empty_rows_yield_no_keys():
    assert calculate.calc_system_metrics([]) == {}


def test_missing_series_omits_its_columns():
    rows = [{"pod_name": "p0", "metric": "cpu", "peak": 2.0, "mean": 1.0}]
    m = calculate.calc_system_metrics(rows)
    assert m["cpu_usage_peak_cores"] == 2.0
    assert "memory_usage_peak_bytes" not in m
    assert "network_received_peak_bytes_per_sec" not in m
