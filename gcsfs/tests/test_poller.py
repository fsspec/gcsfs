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

    @pytest.mark.parametrize("bad_fn", [None, 42, "not_callable", 3.14])
    def test_init_rejects_non_callable(self, bad_fn):
        with pytest.raises(TypeError, match="schedule_fn must be callable"):
            PollSchedule(bad_fn)

    @pytest.mark.parametrize("bad_delay", [-0.1, -10.0, math.nan, math.inf, -math.inf])
    def test_call_rejects_invalid_calculated_delay(self, bad_delay):
        sched = PollSchedule(lambda s: bad_delay)
        with pytest.raises(
            ValueError, match="Calculated delay must be a non-negative finite number"
        ):
            sched(PollStatus(total_elapsed=1.0))

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
        assert (
            PollSchedule(lambda s: None).floor(MIN_SAFE_LRO_POLL_FLOOR)(
                PollStatus(total_elapsed=0.0)
            )
            is None
        )
        # Chaining .floor(0.2) onto a schedule that produces a negative raw value
        # calls self._fn(s) directly and clamps to 0.2 before outer __call__ validation.
        assert PollSchedule(lambda s: -1.0).floor(0.2)(
            PollStatus(total_elapsed=1.0)
        ) == pytest.approx(0.2)
        # NaN raw delays propagate through .floor() and fail outer __call__ validation.
        with pytest.raises(
            ValueError, match="Calculated delay must be a non-negative finite number"
        ):
            PollSchedule(lambda s: math.nan).floor(0.2)(PollStatus(total_elapsed=1.0))

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

    def test_cap_behavior(self):
        sched = PollSchedule.linear_elapsed(slope=1.0).cap(10.0)
        assert sched(PollStatus(total_elapsed=5.0)) == pytest.approx(5.0)
        assert sched(PollStatus(total_elapsed=15.0)) == pytest.approx(10.0)
        assert (
            PollSchedule(lambda s: None).cap(10.0)(PollStatus(total_elapsed=5.0))
            is None
        )
        with pytest.raises(
            ValueError, match="Calculated delay must be a non-negative finite number"
        ):
            PollSchedule(lambda s: math.nan).cap(10.0)(PollStatus(total_elapsed=5.0))

    @pytest.mark.parametrize(
        "min_f, max_f", [(-0.1, 1.0), (1.2, 0.8), (math.nan, 1.0), (0.8, math.inf)]
    )
    def test_with_jitter_rejects_invalid_bounds(self, min_f, max_f):
        sched = PollSchedule.linear_elapsed()
        with pytest.raises(ValueError, match="Invalid jitter bounds"):
            sched.with_jitter(min_f, max_f)

    def test_with_jitter_behavior(self):
        sched = PollSchedule.linear_elapsed(slope=1.0).with_jitter(
            min_factor=0.5, max_factor=1.5, random_fn=lambda a, b: b
        )
        assert sched(PollStatus(total_elapsed=10.0)) == pytest.approx(15.0)
        assert (
            PollSchedule(lambda s: None).with_jitter(
                min_factor=0.5, max_factor=1.5, random_fn=lambda a, b: b
            )(PollStatus(total_elapsed=10.0))
            is None
        )

    @pytest.mark.parametrize("bad_max", [0.0, -5.0, math.nan, math.inf])
    def test_max_duration_rejects_invalid_seconds(self, bad_max):
        sched = PollSchedule.linear_elapsed()
        with pytest.raises(
            ValueError, match="max_seconds must be a positive finite number"
        ):
            sched.max_duration(bad_max)

    def test_max_duration_behavior(self):
        base_sched = PollSchedule(lambda s: 5.0)
        sched = base_sched.max_duration(10.0)
        assert sched(PollStatus(total_elapsed=8.0)) == pytest.approx(2.0)
        assert sched(PollStatus(total_elapsed=10.0)) is None
        assert sched(PollStatus(total_elapsed=12.0)) is None
        assert (
            PollSchedule(lambda s: None).max_duration(10.0)(
                PollStatus(total_elapsed=5.0)
            )
            is None
        )

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
