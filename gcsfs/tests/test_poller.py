import math

import pytest

from gcsfs.poller import (
    DEFAULT_LRO_POLL_CAP,
    MIN_SAFE_LRO_POLL_FLOOR,
    PollSchedule,
    PollStatus,
    get_default_hns_lro_cadence,
)


class TestPollStatus:
    """Unit tests for PollStatus validation."""

    @pytest.mark.parametrize("bad_elapsed", [-0.1, math.nan, math.inf, -math.inf])
    def test_poll_status_rejects_invalid_elapsed(self, bad_elapsed):
        with pytest.raises(
            ValueError, match="total_elapsed must be a non-negative finite number"
        ):
            PollStatus(total_elapsed=bad_elapsed, attempt=1)

    @pytest.mark.parametrize("bad_attempt", [0, -1, -10, True, False, 1.5])
    def test_poll_status_rejects_invalid_attempt(self, bad_attempt):
        with pytest.raises(ValueError, match="attempt"):
            PollStatus(total_elapsed=0.0, attempt=bad_attempt)


class TestPollSchedule:
    """Unit tests for PollSchedule combinators and default HNS cadence."""

    @pytest.mark.parametrize("bad_slope", [0.0, -0.05, math.nan, math.inf])
    def test_linear_elapsed_rejects_invalid_slope(self, bad_slope):
        with pytest.raises(ValueError, match="slope must be a positive finite number"):
            PollSchedule.linear_elapsed(slope=bad_slope)

    @pytest.mark.parametrize("bad_floor", [0.01, 0.0, -0.01, math.nan, math.inf])
    def test_floor_rejects_invalid_min_delay(self, bad_floor):
        sched = PollSchedule.linear_elapsed()
        with pytest.raises(
            ValueError,
            match=f"min_delay must be a finite number >= {MIN_SAFE_LRO_POLL_FLOOR}",
        ):
            sched.floor(bad_floor)

    def test_floor_clamps_low_delay_and_applies_jitter(self):
        # At total_elapsed=0.0, linear_elapsed produces 0.0s, which .floor(0.05)
        # clamps up to MIN_SAFE_LRO_POLL_FLOOR (50ms).
        sched = PollSchedule.linear_elapsed(slope=1.0).floor(MIN_SAFE_LRO_POLL_FLOOR)
        assert sched(PollStatus(total_elapsed=0.0)) == pytest.approx(
            MIN_SAFE_LRO_POLL_FLOOR
        )

        # Chaining .with_jitter(0.75, 1.25) after .floor(0.05) randomizes the
        # 50ms floor across [37.5ms, 62.5ms] to prevent synchronized polling spikes.
        jittered_sched = sched.with_jitter(0.75, 1.25)
        for _ in range(20):
            delay = jittered_sched(PollStatus(total_elapsed=0.0))
            assert delay is not None
            assert 0.0375 <= delay <= 0.0625

    @pytest.mark.parametrize("bad_cap", [0.01, 0.0, -1.0, math.nan, math.inf])
    def test_cap_rejects_invalid_max_delay(self, bad_cap):
        sched = PollSchedule.linear_elapsed()
        with pytest.raises(
            ValueError,
            match=f"max_delay must be a finite number >= {MIN_SAFE_LRO_POLL_FLOOR}",
        ):
            sched.cap(bad_cap)

    @pytest.mark.parametrize(
        "min_f, max_f", [(-0.1, 1.0), (1.2, 0.8), (math.nan, 1.0), (0.8, math.inf)]
    )
    def test_with_jitter_rejects_invalid_bounds(self, min_f, max_f):
        sched = PollSchedule.linear_elapsed()
        with pytest.raises(ValueError, match="Invalid jitter bounds"):
            sched.with_jitter(min_f, max_f)

    @pytest.mark.parametrize("bad_max", [0.0, -5.0, math.nan, math.inf])
    def test_max_duration_rejects_invalid_seconds(self, bad_max):
        sched = PollSchedule.linear_elapsed()
        with pytest.raises(
            ValueError, match="max_seconds must be a positive finite number"
        ):
            sched.max_duration(bad_max)

    def test_default_cadence_initial_delay_and_linear_growth(self):
        sched = get_default_hns_lro_cadence()

        # At t=0, base floor is 200ms, jittered by [0.75, 1.25] -> [150ms, 250ms]
        for _ in range(50):
            d0 = sched(PollStatus(total_elapsed=0.0))
            assert d0 is not None
            assert 0.150 <= d0 <= 0.250

        # At t=10s, 5% linear slope is 0.500s, jittered -> [0.375s, 0.625s]
        for _ in range(50):
            d10 = sched(PollStatus(total_elapsed=10.0))
            assert d10 is not None
            assert 0.375 <= d10 <= 0.625

        # At t=1000s, pre-jitter cap is 24s (30s / 1.25), jittered -> [18.0s, 30.0s]
        for _ in range(50):
            d_large = sched(PollStatus(total_elapsed=1000.0))
            assert d_large is not None
            assert 18.0 <= d_large <= 30.0
            assert d_large <= DEFAULT_LRO_POLL_CAP
