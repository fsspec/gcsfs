"""Percentile/stat helpers mirroring tessellations metric calculators.

Uses numpy.percentile + statistics so results match tessellations exactly:
metrics_calculators/common_metrics/checkpointing_metrics.py and utils.py.
"""

import statistics
from typing import List, Optional

import numpy as np


def mean(values: List[float]) -> Optional[float]:
    """statistics.mean, or None for an empty list."""
    if not values:
        return None
    return statistics.mean(values)


def duration_stats(durations: List[float]) -> dict:
    """min/max/avg/stddev/p50/p90/p99/p100 for a list of durations.

    stddev is statistics.stdev (sample), 0 when fewer than two datapoints --
    matching tessellations _set_checkpoint_duration_metrics. Empty input -> {}.
    """
    n = len(durations)
    if n == 0:
        return {}
    p = np.percentile(durations, [50, 90, 99, 100])
    return {
        "min": float(min(durations)),
        "max": float(max(durations)),
        "avg": float(statistics.mean(durations)),
        "stddev": float(statistics.stdev(durations)) if n > 1 else 0,
        "p50": float(p[0]),
        "p90": float(p[1]),
        "p99": float(p[2]),
        "p100": float(p[3]),
    }
