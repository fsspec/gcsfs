from metrics import calculate


def _w(step, start, end, loc="gs://b/ckpt"):
    return {
        "checkpoint_step": step,
        "checkpoint_location": loc,
        "start_time": start,
        "end_time": end,
    }


def test_write_metrics_duration_per_step_then_stats():
    # two checkpoint steps: durations 10 and 20 -> stats over [10, 20]
    rows = [_w(25, 100.0, 110.0), _w(50, 200.0, 220.0)]
    m = calculate.calc_write_metrics(rows)
    assert m["num_checkpoint_write_datapoints"] == 2
    assert m["checkpoint_write_time_min"] == 10.0
    assert m["checkpoint_write_time_max"] == 20.0
    assert m["checkpoint_write_time_avg"] == 15.0


def test_write_metrics_groups_multiple_rows_per_step():
    # same step, two rows -> duration = max(end) - min(start) = 140 - 100 = 40
    rows = [_w(25, 100.0, 130.0), _w(25, 110.0, 140.0)]
    m = calculate.calc_write_metrics(rows)
    assert m["num_checkpoint_write_datapoints"] == 1
    assert m["checkpoint_write_time_max"] == 40.0


def test_delete_metrics_prefix():
    rows = [
        {
            "checkpoint_step": 50,
            "checkpoint_location": "gs://b/ckpt",
            "start_time": 1.0,
            "end_time": 4.0,
        }
    ]
    m = calculate.calc_delete_metrics(rows)
    assert m["num_checkpoint_delete_datapoints"] == 1
    assert m["checkpoint_delete_time_max"] == 3.0


def test_restore_initial_is_earliest_ending_datapoint():
    # Two restores of distinct checkpoints -> two datapoints; checkpoint_restore_time_initial
    # is the duration of the earliest-ending one, and it is also counted in the
    # main stats (there is no separate "initial cached" exclusion).
    rows = [
        {
            "checkpoint_step": 0,
            "checkpoint_location": "gs://b/ckpt/a.ckpt",
            "start_time": 0.0,
            "end_time": 5.0,
        },
        {
            "checkpoint_step": 0,
            "checkpoint_location": "gs://b/ckpt/b.ckpt",
            "start_time": 10.0,
            "end_time": 18.0,
        },
    ]
    m = calculate.calc_restore_metrics(rows)
    assert m["checkpoint_restore_time_initial"] == 5.0
    assert m["num_checkpoint_restore_datapoints"] == 2
    assert m["checkpoint_restore_time_min"] == 5.0
    assert m["checkpoint_restore_time_max"] == 8.0


def test_empty():
    assert calculate.calc_write_metrics([]) == {}
    assert calculate.calc_restore_metrics([]) == {}
    assert calculate.calc_delete_metrics([]) == {}
