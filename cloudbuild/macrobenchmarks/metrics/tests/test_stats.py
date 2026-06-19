import math
from metrics import stats


def test_duration_stats_basic():
    s = stats.duration_stats([1.0, 2.0, 3.0, 4.0])
    assert s["min"] == 1.0
    assert s["max"] == 4.0
    assert s["avg"] == 2.5
    assert math.isclose(s["stddev"], 1.2909944487358056)
    assert s["p50"] == 2.5    # numpy linear interpolation
    assert s["p100"] == 4.0


def test_duration_stats_single_value_stddev_zero():
    s = stats.duration_stats([5.0])
    assert s["stddev"] == 0
    assert s["min"] == s["max"] == s["p50"] == s["p100"] == 5.0


def test_duration_stats_empty():
    assert stats.duration_stats([]) == {}


def test_mean():
    assert stats.mean([2.0, 4.0]) == 3.0
    assert stats.mean([]) is None
