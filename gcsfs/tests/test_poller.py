import asyncio
import math
from unittest import mock

import pytest
from google.api_core import exceptions as api_exceptions

from gcsfs.poller import (
    DEFAULT_LRO_POLL_CAP,
    MIN_SAFE_LRO_POLL_FLOOR,
    PollSchedule,
    PollStatus,
    get_default_hns_lro_cadence,
    poll_until,
)


class FakeVirtualClock:
    """Deterministic virtual clock and sleep recorder for hermetic polling tests."""

    def __init__(self, start: float = 0.0):
        self.now = start
        self.sleeps: list[float] = []

    def time(self) -> float:
        return self.now

    async def sleep(self, delay: float) -> None:
        self.sleeps.append(delay)
        self.now += delay


class TestPollStatus:
    """Unit tests for PollStatus validation."""

    @pytest.mark.parametrize(
        "bad_elapsed", [-0.1, math.nan, math.inf, -math.inf, True, False]
    )
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

    @pytest.mark.parametrize(
        "bad_delay", [-0.1, -10.0, math.nan, math.inf, -math.inf, True, False]
    )
    def test_call_rejects_invalid_calculated_delay(self, bad_delay):
        sched = PollSchedule(lambda s: bad_delay)
        with pytest.raises(
            ValueError, match="Calculated delay must be a non-negative finite number"
        ):
            sched(PollStatus(total_elapsed=1.0))

    @pytest.mark.parametrize("bad_slope", [0.0, -0.05, math.nan, math.inf, True, False])
    def test_linear_elapsed_rejects_invalid_slope(self, bad_slope):
        with pytest.raises(ValueError, match="slope must be a positive finite number"):
            PollSchedule.linear_elapsed(slope=bad_slope)

    @pytest.mark.parametrize(
        "bad_floor", [0.01, 0.0, -0.01, math.nan, math.inf, True, False]
    )
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
        # Non-finite raw delays propagate through .floor() and fail outer __call__ validation.
        for bad_raw in (math.nan, math.inf, -math.inf):
            with pytest.raises(
                ValueError,
                match="Calculated delay must be a non-negative finite number",
            ):
                PollSchedule(lambda s, val=bad_raw: val).floor(0.2)(
                    PollStatus(total_elapsed=1.0)
                )

        # Chaining .with_jitter(0.75, 1.25) after .floor(0.05) randomizes the
        # 50ms floor across [37.5ms, 62.5ms] to prevent synchronized polling spikes.
        jittered_sched = sched.with_jitter(0.75, 1.25)
        for _ in range(20):
            delay = jittered_sched(PollStatus(total_elapsed=0.0))
            assert delay is not None
            assert 0.0375 <= delay <= 0.0625

    @pytest.mark.parametrize(
        "bad_cap", [0.01, 0.0, -1.0, math.nan, math.inf, True, False]
    )
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
        for bad_raw in (math.nan, math.inf, -math.inf):
            with pytest.raises(
                ValueError,
                match="Calculated delay must be a non-negative finite number",
            ):
                PollSchedule(lambda s, val=bad_raw: val).cap(10.0)(
                    PollStatus(total_elapsed=5.0)
                )

    @pytest.mark.parametrize(
        "min_f, max_f",
        [
            (-0.1, 1.0),
            (1.2, 0.8),
            (math.nan, 1.0),
            (0.8, math.inf),
            (False, 1.0),
            (0.5, True),
        ],
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
        for bad_raw in (math.nan, math.inf, -math.inf):
            with pytest.raises(
                ValueError,
                match="Calculated delay must be a non-negative finite number",
            ):
                PollSchedule(lambda s, val=bad_raw: val).with_jitter(
                    min_factor=0.5, max_factor=1.5, random_fn=lambda a, b: b
                )(PollStatus(total_elapsed=10.0))

    @pytest.mark.parametrize("bad_max", [0.0, -5.0, math.nan, math.inf, True, False])
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
        for bad_raw in (math.nan, math.inf, -math.inf):
            with pytest.raises(
                ValueError,
                match="Calculated delay must be a non-negative finite number",
            ):
                PollSchedule(lambda s, val=bad_raw: val).max_duration(10.0)(
                    PollStatus(total_elapsed=5.0)
                )
            with pytest.raises(
                ValueError,
                match="Calculated delay must be a non-negative finite number",
            ):
                (
                    PollSchedule(lambda s, val=bad_raw: val)
                    .floor(0.2)
                    .cap(10.0)
                    .with_jitter(
                        min_factor=0.75, max_factor=1.25, random_fn=lambda a, b: 1.0
                    )
                    .max_duration(30.0)
                )(PollStatus(total_elapsed=5.0))

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


class TestPollRunners:
    """Virtual-time unit tests for poll_until."""

    @pytest.mark.asyncio
    async def test_poll_until_absorbs_transient_transport_glitches(self):
        clock = FakeVirtualClock()
        calls = 0

        async def flaky_check(status: PollStatus):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise api_exceptions.ServiceUnavailable("503 backend glitch")
            if calls == 2:
                raise api_exceptions.TooManyRequests("429 quota spike")
            if calls == 3:
                raise api_exceptions.DeadlineExceeded("504 gateway deadline")
            if calls == 4:
                raise api_exceptions.InternalServerError("500 internal error")
            if calls == 5:
                raise asyncio.TimeoutError("per-RPC deadline")
            return True, "recovered"

        sched = PollSchedule.linear_elapsed(0.05).floor(0.200)
        res = await poll_until(
            flaky_check,
            schedule=sched,
            operation_id="op-flaky",
            time_fn=clock.time,
            sleep_fn=clock.sleep,
        )

        assert res == "recovered"
        assert calls == 6
        assert len(clock.sleeps) == 6

    @pytest.mark.asyncio
    async def test_poll_until_keeps_polling_after_unknown(self):
        clock = FakeVirtualClock()
        check = mock.AsyncMock(
            side_effect=[api_exceptions.Unknown("transport reset"), (True, "ok")]
        )

        res = await poll_until(
            check,
            schedule=PollSchedule.linear_elapsed(0.05).floor(0.200),
            time_fn=clock.time,
            sleep_fn=clock.sleep,
        )

        assert res == "ok"
        assert check.await_count == 2

    @pytest.mark.asyncio
    async def test_poll_until_reraises_non_transient_error(self):
        clock = FakeVirtualClock()
        check = mock.AsyncMock(side_effect=api_exceptions.NotFound("no such op"))

        with pytest.raises(api_exceptions.NotFound, match="no such op"):
            await poll_until(
                check,
                schedule=PollSchedule.linear_elapsed(0.05).floor(0.200),
                time_fn=clock.time,
                sleep_fn=clock.sleep,
            )

        check.assert_awaited_once()
        assert len(clock.sleeps) == 1

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "max_seconds, expected_checks, expected_desc",
        [
            (0.5, 1, "1 status check"),
            (1.0, 2, "2 status checks"),
        ],
    )
    async def test_poll_until_timeout_message_counts_status_checks(
        self, max_seconds, expected_checks, expected_desc
    ):
        clock = FakeVirtualClock()
        check = mock.AsyncMock(return_value=(False, None))

        with pytest.raises(asyncio.TimeoutError) as exc_info:
            await poll_until(
                check,
                schedule=PollSchedule(lambda s: 0.5).max_duration(max_seconds),
                operation_id="op-1",
                time_fn=clock.time,
                sleep_fn=clock.sleep,
            )

        assert check.await_count == expected_checks
        assert str(exc_info.value) == (
            f"Polling timed out for 'op-1' after {max_seconds:.2f}s "
            f"and {expected_desc}."
        )

    @pytest.mark.asyncio
    async def test_poll_until_timeout_chains_last_transient_error(self):
        clock = FakeVirtualClock()
        last = api_exceptions.ServiceUnavailable("still down")
        check = mock.AsyncMock(
            side_effect=[api_exceptions.TooManyRequests("slow down"), last, last]
        )

        with pytest.raises(asyncio.TimeoutError) as exc_info:
            await poll_until(
                check,
                schedule=PollSchedule(lambda s: 0.5).max_duration(1.2),
                time_fn=clock.time,
                sleep_fn=clock.sleep,
            )

        assert exc_info.value.__cause__ is last

    @pytest.mark.asyncio
    async def test_poll_until_timeout_has_no_cause_after_successful_check(self):
        clock = FakeVirtualClock()
        check = mock.AsyncMock(
            side_effect=[
                api_exceptions.ServiceUnavailable("blip"),
                (False, None),
                (False, None),
            ]
        )

        with pytest.raises(asyncio.TimeoutError) as exc_info:
            await poll_until(
                check,
                schedule=PollSchedule(lambda s: 0.5).max_duration(1.2),
                time_fn=clock.time,
                sleep_fn=clock.sleep,
            )

        assert exc_info.value.__cause__ is None

    @pytest.mark.asyncio
    @pytest.mark.parametrize("bad_schedule", ["not_callable", lambda s: 0.2, None])
    async def test_poll_until_rejects_non_poll_schedule(self, bad_schedule):
        clock = FakeVirtualClock()
        check = mock.AsyncMock(return_value=(True, "ok"))

        with pytest.raises(TypeError, match="schedule must be a PollSchedule"):
            await poll_until(
                check,
                schedule=bad_schedule,  # type: ignore[arg-type]
                time_fn=clock.time,
                sleep_fn=clock.sleep,
            )

        check.assert_not_awaited()
        assert clock.sleeps == []
