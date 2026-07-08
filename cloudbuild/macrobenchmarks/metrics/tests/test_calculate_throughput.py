from metrics import calculate


def test_samples_per_second_mean_and_stable_window():
    step_rows = [
        {
            "step": s,
            "step_duration": 1.0,
            "step_end_time": float(s),
            "samples_per_second": float(s),
        }
        for s in range(0, 12)
    ]
    m = calculate.calc_step_time_metrics(step_rows)
    assert m["mean_samples_per_second"] == sum(range(0, 12)) / 12
    assert m["stable_window_avg_samples_per_second"] == (10 + 11) / 2


def test_samples_per_second_absent_yields_no_keys():
    step_rows = [{"step": 0, "step_duration": 1.0, "step_end_time": 0.0}]
    m = calculate.calc_step_time_metrics(step_rows)
    assert "mean_samples_per_second" not in m
    assert "stable_window_avg_samples_per_second" not in m


def test_write_throughput_joins_size_by_step():
    write_rows = [
        {
            "checkpoint_step": 25,
            "checkpoint_location": "gs://b/ckpt",
            "start_time": 0.0,
            "end_time": 10.0,
        }
    ]
    size_rows = [{"checkpoint_step": 25, "size_bytes": 1000}]
    m = calculate.calc_throughput_metrics(write_rows, size_rows)
    assert m["checkpoint_size_bytes"] == 1000
    assert m["checkpoint_write_throughput_avg_bytes_per_sec"] == 100.0


def test_restore_throughput_from_restored_bytes_and_duration():
    assert calculate._restore_throughput(2000, 5.0) == 400.0


def test_restore_throughput_none_when_size_or_duration_absent_or_zero():
    assert calculate._restore_throughput(None, 5.0) is None
    assert calculate._restore_throughput(2000, None) is None
    assert calculate._restore_throughput(2000, 0.0) is None


def test_throughput_missing_sizes_yields_no_throughput_keys():
    write_rows = [
        {
            "checkpoint_step": 25,
            "checkpoint_location": "gs://b/ckpt",
            "start_time": 0.0,
            "end_time": 10.0,
        }
    ]
    m = calculate.calc_throughput_metrics(write_rows, [])
    assert "checkpoint_write_throughput_avg_bytes_per_sec" not in m
    assert "checkpoint_size_bytes" not in m
