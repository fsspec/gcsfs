import asyncio
import math
from unittest import mock

import pytest
from google.api_core import exceptions as api_exceptions
from google.cloud import storage_control_v2
from google.rpc import code_pb2, status_pb2

from gcsfs.poller import (
    _BACKGROUND_TASKS,
    DEFAULT_LRO_POLL_CAP,
    DEFAULT_LRO_POLL_FLOOR,
    MIN_SAFE_LRO_POLL_FLOOR,
    PollSchedule,
    PollStatus,
    _unwrap_operation_result,
    get_default_hns_lro_cadence,
    poll_lro,
    poll_until,
)
from gcsfs.tests.lro_fakes import FakeLroFactory, operation_pb


@pytest.fixture
def fake_lro():
    """Same as the conftest fixture, so this module also runs with --noconftest."""
    return FakeLroFactory()


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
    """Virtual-time unit tests for poll_until, _unwrap_operation_result, and poll_lro."""

    @pytest.mark.asyncio
    async def test_zero_rpc_in_memory_check_completes_at_t0(self, fake_lro):
        clock = FakeVirtualClock()
        folder = storage_control_v2.Folder(name="projects/_/buckets/b/folders/dst/")
        lro = fake_lro.succeeded(folder, name="projects/_/buckets/b/operations/op-fast")

        res = await poll_lro(
            lro.op,
            time_fn=clock.time,
            sleep_fn=clock.sleep,
        )

        assert res == folder
        assert clock.sleeps == []
        assert clock.now == 0.0
        lro.get_operation.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_poll_lro_polls_with_virtual_clock_until_done(self, fake_lro):
        clock = FakeVirtualClock()
        folder = storage_control_v2.Folder(name="projects/_/buckets/b/folders/dst/")
        # Complete on the 3rd check
        lro = fake_lro.succeeded(
            folder, after=3, name="projects/_/buckets/b/operations/op-1"
        )

        sched = PollSchedule.linear_elapsed(0.05).floor(DEFAULT_LRO_POLL_FLOOR)
        res = await poll_lro(
            lro.op,
            schedule=sched,
            timeout=300.0,
            path1="b/src",
            path2="b/dst",
            time_fn=clock.time,
            sleep_fn=clock.sleep,
        )

        assert res == folder
        assert len(clock.sleeps) == 3
        assert clock.sleeps[0] == pytest.approx(0.200)
        assert lro.get_operation.await_count == 3
        # Every poll bypasses the GAPIC default retry; poll_until is the only
        # retry loop.
        for call in lro.get_operation.await_args_list:
            assert call.kwargs["retry"] is None

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
    async def test_poll_lro_timeout_raises_timeout_error_with_operation_id(
        self, fake_lro
    ):
        clock = FakeVirtualClock()
        lro = fake_lro.pending(name="projects/_/buckets/b/operations/op-stalled")

        sched = PollSchedule.linear_elapsed(0.05).floor(0.500)
        with pytest.raises(asyncio.TimeoutError) as exc_info:
            await poll_lro(
                lro.op,
                schedule=sched,
                timeout=1.2,
                path1="b/src",
                path2="b/dst",
                time_fn=clock.time,
                sleep_fn=clock.sleep,
            )

        assert clock.now == pytest.approx(1.2)
        # Polls at 0.5 s, 1.0 s and 1.2 s; the message names the operation
        # once, without the paths.
        assert lro.get_operation.await_count == 3
        assert str(exc_info.value) == (
            "Polling timed out for 'projects/_/buckets/b/operations/op-stalled' "
            "after 1.20s and 3 status checks."
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "bad_timeout", [0, 0.0, -1.0, math.inf, math.nan, True, "300"]
    )
    @pytest.mark.parametrize("done_at_t0", [True, False])
    async def test_poll_lro_rejects_invalid_timeout_before_any_rpc(
        self, fake_lro, bad_timeout, done_at_t0
    ):
        clock = FakeVirtualClock()
        lro = fake_lro.succeeded() if done_at_t0 else fake_lro.pending()

        with pytest.raises(ValueError, match="timeout must be a positive finite"):
            await poll_lro(
                lro.op,
                timeout=bad_timeout,
                time_fn=clock.time,
                sleep_fn=clock.sleep,
            )

        lro.get_operation.assert_not_awaited()
        assert clock.sleeps == []

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "code, expected_cls",
        [
            (code_pb2.ALREADY_EXISTS, api_exceptions.AlreadyExists),
            (code_pb2.ABORTED, api_exceptions.Aborted),
            (code_pb2.NOT_FOUND, api_exceptions.NotFound),
        ],
    )
    async def test_unwrap_operation_result_maps_grpc_status_to_typed_exception(
        self, code, expected_cls
    ):
        operation = mock.AsyncMock()
        # A failed LRO carries its error as a google.rpc.Status, whose code is
        # always an int.
        raw_err = api_exceptions.GoogleAPICallError(
            "LRO failed",
            errors=[status_pb2.Status(code=code, message="LRO failed")],
        )
        operation.result.side_effect = raw_err

        with pytest.raises(expected_cls):
            await _unwrap_operation_result(operation)

    @pytest.mark.asyncio
    async def test_poll_lro_cancellation_dispatches_server_cancel(self, fake_lro):
        clock = FakeVirtualClock()
        # The 1st poll is cancelled; the real AsyncOperation.cancel() then polls
        # once more (still pending) before sending CancelOperation.
        name = "projects/_/buckets/b/operations/op-cancel"
        lro = fake_lro.sequence(
            [asyncio.CancelledError(), operation_pb(name=name)], name=name
        )

        sched = PollSchedule.linear_elapsed(0.05).floor(0.200)
        with pytest.raises(asyncio.CancelledError):
            await poll_lro(
                lro.op,
                schedule=sched,
                path1="b/src",
                path2="b/dst",
                time_fn=clock.time,
                sleep_fn=clock.sleep,
            )

        await asyncio.gather(*list(_BACKGROUND_TASKS), return_exceptions=True)
        lro.cancel_operation.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_poll_lro_cancellation_bounds_hung_server_cancel(
        self, fake_lro, monkeypatch
    ):
        monkeypatch.setattr("gcsfs.poller.PER_POLL_RPC_TIMEOUT", 0.01)
        clock = FakeVirtualClock()
        name = "projects/_/buckets/b/operations/op-cancel-hung"
        lro = fake_lro.sequence(
            [asyncio.CancelledError(), operation_pb(name=name)], name=name
        )

        async def hung_cancel(*_args, **_kwargs):
            await asyncio.sleep(10.0)

        lro.cancel_operation.side_effect = hung_cancel

        sched = PollSchedule.linear_elapsed(0.05).floor(0.200)
        with pytest.raises(asyncio.CancelledError):
            await poll_lro(
                lro.op,
                schedule=sched,
                time_fn=clock.time,
                sleep_fn=clock.sleep,
            )

        await asyncio.wait_for(
            asyncio.gather(*list(_BACKGROUND_TASKS), return_exceptions=True),
            timeout=5.0,
        )
        lro.cancel_operation.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_poll_lro_keeps_cancel_that_races_a_finished_status_check(
        self, fake_lro
    ):
        """A cancel that lands in the same loop step as a status check finishing
        must still raise CancelledError and send CancelOperation.

        asyncio.wait_for on Python 3.10 and 3.11 returns the check's result in
        that case and drops the cancel, so polling would run on until timeout.
        """
        clock = FakeVirtualClock()
        lro = fake_lro.pending(name="projects/_/buckets/b/operations/op-race")
        replay = lro.get_operation.side_effect
        tasks = {}

        def cancel_poller_then_report_pending(*args, **kwargs):
            if lro.get_operation.await_count == 1:
                tasks["poller"].cancel()
            return replay(*args, **kwargs)

        async def sleep_and_yield(delay):
            # Yield like asyncio.sleep, so a cancel still pending on the
            # poller is delivered here, as it would be in production.
            await clock.sleep(delay)
            await asyncio.sleep(0)

        lro.get_operation.side_effect = cancel_poller_then_report_pending
        tasks["poller"] = asyncio.create_task(
            poll_lro(
                lro.op,
                schedule=PollSchedule.linear_elapsed(0.05).floor(0.200),
                timeout=1.0,
                time_fn=clock.time,
                sleep_fn=sleep_and_yield,
            )
        )

        with pytest.raises(asyncio.CancelledError):
            await tasks["poller"]

        await asyncio.wait_for(
            asyncio.gather(*list(_BACKGROUND_TASKS), return_exceptions=True),
            timeout=5.0,
        )
        # One status check by the poller, then the one AsyncOperation.cancel()
        # makes before it sends CancelOperation.
        assert lro.get_operation.await_count == 2
        lro.cancel_operation.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_poll_lro_retries_after_per_poll_timeout(self, fake_lro, monkeypatch):
        """A status check that exceeds the per-poll deadline is cancelled and
        counts as a transient error, so the next poll runs."""
        monkeypatch.setattr("gcsfs.poller.PER_POLL_RPC_TIMEOUT", 0.05)
        clock = FakeVirtualClock()
        lro = fake_lro.succeeded(after=1)
        replay = lro.get_operation.side_effect
        hung_check_cancelled = asyncio.Event()

        async def first_check_hangs(*args, **kwargs):
            if lro.get_operation.await_count == 1:
                try:
                    await asyncio.sleep(10.0)
                except asyncio.CancelledError:
                    hung_check_cancelled.set()
                    raise
            return replay(*args, **kwargs)

        lro.get_operation.side_effect = first_check_hangs

        await poll_lro(
            lro.op,
            schedule=PollSchedule.linear_elapsed(0.05).floor(0.200),
            timeout=10.0,
            time_fn=clock.time,
            sleep_fn=clock.sleep,
        )

        await asyncio.wait_for(hung_check_cancelled.wait(), timeout=5.0)
        assert lro.get_operation.await_count == 2
        assert clock.sleeps == [pytest.approx(0.2), pytest.approx(0.2)]
        lro.cancel_operation.assert_not_awaited()

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

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "override_kwarg",
        [
            {"schedule": "not_callable"},
            {"schedule": lambda s: 0.2, "timeout": 10.0},
            {"schedule": 0, "timeout": 10.0},
        ],
    )
    async def test_poll_lro_rejects_non_poll_schedule(self, fake_lro, override_kwarg):
        clock = FakeVirtualClock()
        # Already done at t=0: without the check, poll_lro would return its
        # result instead of raising.
        lro = fake_lro.succeeded(name="projects/_/buckets/b/operations/op-invalid-arg")

        kwargs = {
            "operation": lro.op,
            "time_fn": clock.time,
            "sleep_fn": clock.sleep,
        }
        kwargs.update(override_kwarg)

        with pytest.raises(TypeError, match="schedule must be a PollSchedule"):
            await poll_lro(**kwargs)  # type: ignore[arg-type]

        lro.get_operation.assert_not_awaited()
        assert clock.sleeps == []
