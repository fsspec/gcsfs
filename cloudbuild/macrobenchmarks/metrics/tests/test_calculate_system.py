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


def test_maps_limit_utilization_and_mean_memory():
    rows = [
        {"pod_name": "p0", "metric": "memory", "peak": 4096.0, "mean": 2048.0},
        {"pod_name": "p1", "metric": "memory", "peak": 8192.0, "mean": 1024.0},
        {"pod_name": "p0", "metric": "cpu_limit_utilization", "peak": 0.7, "mean": 0.3},
        {"pod_name": "p1", "metric": "cpu_limit_utilization", "peak": 0.9, "mean": 0.4},
        {
            "pod_name": "p0",
            "metric": "memory_limit_utilization",
            "peak": 0.5,
            "mean": None,
        },
    ]
    m = calculate.calc_system_metrics(rows)
    assert m["memory_usage_mean_bytes"] == 2048  # max of per-pod means, int
    assert isinstance(m["memory_usage_mean_bytes"], int)
    assert m["cpu_limit_utilization_peak"] == 0.9
    assert m["memory_limit_utilization_peak"] == 0.5


def test_checkpoint_amplification_ratio_and_raw_dataset_columns():
    # The checkpoint ratio is derivable from system rows alone, so it stays in
    # calc_system_metrics. The dataset ratio needs run-shape inputs (steps,
    # global_batch_size) that live outside system rows, so here we only surface
    # the raw dataset columns (read/size/sample_count) that feed it downstream.
    rows = [
        {
            "pod_name": "ckpt-bkt",
            "metric": "checkpoint_read_bytes",
            "peak": 800.0,
            "mean": None,
        },
        {
            "pod_name": "gs://ckpt",
            "metric": "checkpoint_restored_bytes",
            "peak": 100.0,
            "mean": None,
        },
        {
            "pod_name": "ds-bkt",
            "metric": "dataset_read_bytes",
            "peak": 3000.0,
            "mean": None,
        },
        {
            "pod_name": "ds-bkt",
            "metric": "dataset_size_bytes",
            "peak": 1000.0,
            "mean": None,
        },
        {
            "pod_name": "ds-bkt",
            "metric": "dataset_sample_count",
            "peak": 100.0,
            "mean": None,
        },
        {
            "pod_name": "ckpt-bkt",
            "metric": "checkpoint_read_request_count",
            "peak": 42.0,
            "mean": None,
        },
    ]
    m = calculate.calc_system_metrics(rows)
    assert m["checkpoint_read_bytes"] == 800
    assert isinstance(m["checkpoint_read_bytes"], int)
    assert m["checkpoint_restored_bytes"] == 100
    assert m["checkpoint_read_amplification_ratio"] == 8.0
    assert m["checkpoint_read_request_count"] == 42
    # Raw dataset columns surfaced (int-typed); the ratio is computed later.
    assert m["dataset_read_bytes"] == 3000
    assert m["dataset_size_bytes"] == 1000
    assert m["dataset_sample_count"] == 100
    assert isinstance(m["dataset_sample_count"], int)
    assert "dataset_read_amplification_ratio" not in m


def test_checkpoint_amplification_ratio_omitted_when_denominator_missing():
    rows = [
        {
            "pod_name": "b",
            "metric": "checkpoint_read_bytes",
            "peak": 500.0,
            "mean": None,
        },
    ]
    m = calculate.calc_system_metrics(rows)
    assert "checkpoint_read_amplification_ratio" not in m  # no restored_bytes
