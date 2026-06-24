from metrics import calculate


def _rows(durations):
    # step_end_time is cumulative; step starts at 0.
    rows, t = [], 0.0
    for i, d in enumerate(durations):
        t += d
        rows.append({"step": i, "step_duration": d, "step_end_time": t})
    return rows


def test_mean_step_time_is_mean_of_all_steps():
    # 12 steps of 1.0s -> mean 1.0 (per_step stabilization = 0).
    m = calculate.calc_step_time_metrics(_rows([1.0] * 12))
    assert m["mean_step_time"] == 1.0


def test_training_window_skips_zero_stable_window_skips_ten():
    # durations 1..12; stable window skips first 10 (steps 0-9), keeps 10,11.
    rows = _rows([float(x) for x in range(1, 13)])
    m = calculate.calc_step_time_metrics(rows)
    # training window keeps all 12; total = last_end - first_end + first_dur
    # first step dur=1 end=1; last end=78 -> 78 - 1 + 1 = 78; avg = 78/12
    assert m["training_window_total_step_duration"] == 78.0
    assert m["training_window_avg_step_time"] == 78.0 / 12
    # stable window keeps steps 10,11 (dur 11,12); first end=66 (sum 1..11),
    # last end=78; total = 78 - 66 + 11 = 23; avg = 23/2
    assert m["stable_window_total_step_duration"] == 23.0
    assert m["stable_window_avg_step_time"] == 23.0 / 2


def test_empty_rows():
    assert calculate.calc_step_time_metrics([]) == {}
