import asyncio
import math
from types import SimpleNamespace
from unittest import mock

import pytest
from google.api_core import exceptions as api_exceptions
from google.cloud import storage_control_v2
from google.rpc import code_pb2, status_pb2

from gcsfs.poller import (
    _BACKGROUND_TASKS,
    _calculate_poll_delay,
    _poll_until,
    poll_lro,
    unwrap_operation_result,
)
from gcsfs.tests.lro_fakes import operation_pb


def linear_delay(floor):
    """Unjittered cadence: 5% of the elapsed time, at least ``floor`` seconds."""
    return lambda elapsed: max(0.05 * elapsed, floor)


def set_jitter(monkeypatch, uniform):
    """Makes the poller's random.uniform return ``uniform(a, b)``.

    Swaps gcsfs.poller's reference to the random module, so the global
    random.uniform stays untouched.
    """
    monkeypatch.setattr("gcsfs.poller.random", SimpleNamespace(uniform=uniform))


class TestCalculatePollDelay:
    """Unit tests for the default polling cadence."""

    def test_short_elapsed_time_returns_floor_delay(self, monkeypatch):
        set_jitter(monkeypatch, lambda a, b: 1.0)
        assert _calculate_poll_delay(0.0) == 0.2
        assert _calculate_poll_delay(0.5) == 0.2

    def test_grows_linearly_with_elapsed_time(self, monkeypatch):
        set_jitter(monkeypatch, lambda a, b: 1.0)
        assert _calculate_poll_delay(10.0) == 2.0

    def test_cap_keeps_full_jitter_spread_below_cap(self, monkeypatch):
        set_jitter(monkeypatch, lambda a, b: b)
        assert _calculate_poll_delay(1000.0) == 30.0
        set_jitter(monkeypatch, lambda a, b: a)
        assert _calculate_poll_delay(1000.0) == 18.0


class TestPollRunners:
    """Virtual-time unit tests for _poll_until, unwrap_operation_result, and poll_lro."""

    @pytest.mark.asyncio
    async def test_zero_rpc_in_memory_check_completes_at_t0(self, fake_lro, fake_clock):
        folder = storage_control_v2.Folder(name="projects/_/buckets/b/folders/dst/")
        lro = fake_lro.succeeded(folder, name="projects/_/buckets/b/operations/op-fast")

        res = await poll_lro(lro.op, timeout=math.inf)

        assert res == folder
        assert fake_clock.sleeps == []
        assert fake_clock.now == 0.0
        lro.get_operation.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_poll_lro_polls_with_virtual_clock_until_done(
        self, fake_lro, fake_clock, monkeypatch
    ):
        monkeypatch.setattr("gcsfs.poller._calculate_poll_delay", linear_delay(0.2))
        folder = storage_control_v2.Folder(name="projects/_/buckets/b/folders/dst/")
        # Complete on the 3rd check
        lro = fake_lro.succeeded(
            folder, after=3, name="projects/_/buckets/b/operations/op-1"
        )

        res = await poll_lro(lro.op, timeout=300.0)

        assert res == folder
        assert len(fake_clock.sleeps) == 3
        assert fake_clock.sleeps[0] == pytest.approx(0.200)
        assert lro.get_operation.await_count == 3
        # Every poll bypasses the GAPIC default retry; _poll_until is the only
        # retry loop.
        for call in lro.get_operation.await_args_list:
            assert call.kwargs["retry"] is None

    @pytest.mark.asyncio
    async def test_poll_lro_without_limit_keeps_full_per_poll_deadline(
        self, fake_lro, fake_clock, monkeypatch
    ):
        monkeypatch.setattr("gcsfs.poller._calculate_poll_delay", lambda elapsed: 1e3)
        lro = fake_lro.succeeded(after=3)

        await poll_lro(lro.op, timeout=math.inf)

        # math.inf never clamps a sleep.
        assert fake_clock.sleeps == [1e3] * 3

    @pytest.mark.asyncio
    async def test_poll_until_absorbs_transient_transport_glitches(
        self, fake_clock, monkeypatch
    ):
        monkeypatch.setattr("gcsfs.poller._calculate_poll_delay", linear_delay(0.2))
        check = mock.AsyncMock(
            side_effect=[
                api_exceptions.ServiceUnavailable("503 backend glitch"),
                api_exceptions.TooManyRequests("429 quota spike"),
                api_exceptions.DeadlineExceeded("504 gateway deadline"),
                api_exceptions.InternalServerError("500 internal error"),
                asyncio.TimeoutError("per-RPC deadline"),
                (True, "recovered"),
            ]
        )

        res = await _poll_until(check, timeout=math.inf, description="op-flaky")

        assert res == "recovered"
        assert check.await_count == 6
        assert len(fake_clock.sleeps) == 6

    @pytest.mark.asyncio
    async def test_poll_until_backs_off_on_consecutive_transient_errors(
        self, fake_clock, monkeypatch
    ):
        monkeypatch.setattr("gcsfs.poller._calculate_poll_delay", lambda elapsed: 0.1)
        set_jitter(monkeypatch, lambda a, b: 1.0)
        unavailable = api_exceptions.ServiceUnavailable("503")
        check = mock.AsyncMock(
            side_effect=[unavailable] * 3 + [(False, None), unavailable, (True, "ok")]
        )

        res = await _poll_until(check, timeout=math.inf)

        assert res == "ok"
        # Doubles per consecutive error; the (False, None) check resets it.
        assert fake_clock.sleeps == [0.1, 0.5, 1.0, 2.0, 0.1, 0.5]

    @pytest.mark.asyncio
    async def test_poll_until_transient_backoff_is_capped(
        self, fake_clock, monkeypatch
    ):
        monkeypatch.setattr("gcsfs.poller._calculate_poll_delay", lambda elapsed: 0.1)
        set_jitter(monkeypatch, lambda a, b: b)
        check = mock.AsyncMock(
            side_effect=[api_exceptions.TooManyRequests("429")] * 12 + [(True, "ok")]
        )

        await _poll_until(check, timeout=math.inf)

        # Even at the top of the jitter range the wait stops at the 30 s cap.
        assert fake_clock.sleeps[-1] == 30.0
        assert max(fake_clock.sleeps) == 30.0

    @pytest.mark.asyncio
    async def test_poll_until_transient_backoff_is_clamped_to_the_deadline(
        self, fake_clock, monkeypatch
    ):
        monkeypatch.setattr("gcsfs.poller._calculate_poll_delay", lambda elapsed: 0.1)
        check = mock.AsyncMock(side_effect=api_exceptions.ServiceUnavailable("503"))

        with pytest.raises(asyncio.TimeoutError):
            await _poll_until(check, timeout=3.0)

        assert fake_clock.now == pytest.approx(3.0)

    @pytest.mark.asyncio
    async def test_poll_until_non_transient_error_after_transient_error_raises(
        self, fake_clock, monkeypatch
    ):
        monkeypatch.setattr("gcsfs.poller._calculate_poll_delay", lambda elapsed: 0.1)
        check = mock.AsyncMock(
            side_effect=[
                api_exceptions.ServiceUnavailable("503"),
                api_exceptions.PermissionDenied("403"),
            ]
        )

        with pytest.raises(api_exceptions.PermissionDenied):
            await _poll_until(check, timeout=math.inf)

        assert check.await_count == 2
        assert len(fake_clock.sleeps) == 2

    @pytest.mark.asyncio
    async def test_poll_until_keeps_polling_after_unknown(
        self, fake_clock, monkeypatch
    ):
        monkeypatch.setattr("gcsfs.poller._calculate_poll_delay", linear_delay(0.2))
        check = mock.AsyncMock(
            side_effect=[api_exceptions.Unknown("transport reset"), (True, "ok")]
        )

        res = await _poll_until(check, timeout=math.inf)

        assert res == "ok"
        assert check.await_count == 2

    @pytest.mark.asyncio
    async def test_poll_until_reraises_non_transient_error(
        self, fake_clock, monkeypatch
    ):
        monkeypatch.setattr("gcsfs.poller._calculate_poll_delay", linear_delay(0.2))
        check = mock.AsyncMock(side_effect=api_exceptions.NotFound("no such op"))

        with pytest.raises(api_exceptions.NotFound, match="no such op"):
            await _poll_until(check, timeout=math.inf)

        check.assert_awaited_once()
        assert len(fake_clock.sleeps) == 1

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "max_seconds, expected_checks, expected_desc",
        [
            (0.5, 1, "1 status check"),
            (1.0, 2, "2 status checks"),
        ],
    )
    async def test_poll_until_timeout_message_counts_status_checks(
        self, fake_clock, monkeypatch, max_seconds, expected_checks, expected_desc
    ):
        monkeypatch.setattr("gcsfs.poller._calculate_poll_delay", lambda elapsed: 0.5)
        check = mock.AsyncMock(return_value=(False, None))

        with pytest.raises(asyncio.TimeoutError) as exc_info:
            await _poll_until(check, timeout=max_seconds, description="op-1")

        assert check.await_count == expected_checks
        assert str(exc_info.value) == (
            f"Polling timed out for 'op-1' after {max_seconds:.2f}s "
            f"and {expected_desc}."
        )

    @pytest.mark.asyncio
    async def test_poll_until_timeout_chains_last_transient_error(
        self, fake_clock, monkeypatch
    ):
        monkeypatch.setattr("gcsfs.poller._calculate_poll_delay", lambda elapsed: 0.5)
        last = api_exceptions.ServiceUnavailable("still down")
        check = mock.AsyncMock(
            side_effect=[api_exceptions.TooManyRequests("slow down"), last, last]
        )

        with pytest.raises(asyncio.TimeoutError) as exc_info:
            await _poll_until(check, timeout=1.2)

        assert exc_info.value.__cause__ is last

    @pytest.mark.asyncio
    async def test_poll_until_timeout_has_no_cause_after_successful_check(
        self, fake_clock, monkeypatch
    ):
        monkeypatch.setattr("gcsfs.poller._calculate_poll_delay", lambda elapsed: 0.5)
        check = mock.AsyncMock(
            side_effect=[
                api_exceptions.ServiceUnavailable("blip"),
                (False, None),
                (False, None),
            ]
        )

        with pytest.raises(asyncio.TimeoutError) as exc_info:
            await _poll_until(check, timeout=1.2)

        assert exc_info.value.__cause__ is None

    @pytest.mark.asyncio
    async def test_poll_lro_timeout_raises_timeout_error_with_operation_id(
        self, fake_lro, fake_clock, monkeypatch
    ):
        monkeypatch.setattr("gcsfs.poller._calculate_poll_delay", linear_delay(0.5))
        lro = fake_lro.pending(name="projects/_/buckets/b/operations/op-stalled")

        with pytest.raises(asyncio.TimeoutError) as exc_info:
            await poll_lro(lro.op, timeout=1.2)

        assert fake_clock.now == pytest.approx(1.2)
        # Polls at 0.5 s, 1.0 s and 1.2 s; the message names the operation.
        assert lro.get_operation.await_count == 3
        assert str(exc_info.value) == (
            "Polling timed out for 'projects/_/buckets/b/operations/op-stalled' "
            "after 1.20s and 3 status checks."
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "bad_timeout", [0, -1.0, math.nan], ids=["zero", "negative", "nan"]
    )
    async def test_poll_lro_rejects_invalid_timeout_before_any_rpc(
        self, fake_lro, fake_clock, bad_timeout
    ):
        lro = fake_lro.pending()

        with pytest.raises(ValueError, match="timeout must be a positive number"):
            await poll_lro(lro.op, timeout=bad_timeout)

        lro.get_operation.assert_not_awaited()
        assert fake_clock.sleeps == []

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
            await unwrap_operation_result(operation)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "error",
        [object(), SimpleNamespace(code="INTERNAL"), SimpleNamespace(code=[13])],
        ids=["no_code", "str_code", "unhashable_code"],
    )
    async def test_unwrap_operation_result_reraises_error_without_int_code(self, error):
        operation = mock.AsyncMock()
        raw_err = api_exceptions.GoogleAPICallError("LRO failed", errors=[error])
        operation.result.side_effect = raw_err

        with pytest.raises(api_exceptions.GoogleAPICallError) as exc_info:
            await unwrap_operation_result(operation)

        assert exc_info.value is raw_err

    @pytest.mark.asyncio
    async def test_poll_lro_cancellation_dispatches_server_cancel(
        self, fake_lro, fake_clock, caplog, monkeypatch
    ):
        monkeypatch.setattr("gcsfs.poller._calculate_poll_delay", linear_delay(0.2))
        # The 1st poll is cancelled; the real AsyncOperation.cancel() then polls
        # once more (still pending) before sending CancelOperation.
        name = "projects/_/buckets/b/operations/op-cancel"
        lro = fake_lro.sequence(
            [asyncio.CancelledError(), operation_pb(name=name)], name=name
        )

        with caplog.at_level("WARNING", logger="gcsfs"):
            with pytest.raises(asyncio.CancelledError):
                await poll_lro(lro.op, timeout=math.inf)

        await asyncio.gather(*list(_BACKGROUND_TASKS), return_exceptions=True)
        lro.cancel_operation.assert_awaited_once()
        [warning] = [r.getMessage() for r in caplog.records if "cancelled" in r.msg]
        assert warning.count(name) == 1

    @pytest.mark.asyncio
    async def test_poll_lro_cancellation_bounds_hung_server_cancel(
        self, fake_lro, fake_clock, monkeypatch
    ):
        monkeypatch.setattr("gcsfs.poller._PER_POLL_RPC_TIMEOUT", 0.01)
        monkeypatch.setattr("gcsfs.poller._calculate_poll_delay", linear_delay(0.2))
        name = "projects/_/buckets/b/operations/op-cancel-hung"
        lro = fake_lro.sequence(
            [asyncio.CancelledError(), operation_pb(name=name)], name=name
        )

        async def hung_cancel(*_args, **_kwargs):
            await asyncio.sleep(10.0)

        lro.cancel_operation.side_effect = hung_cancel

        with pytest.raises(asyncio.CancelledError):
            await poll_lro(lro.op, timeout=math.inf)

        await asyncio.wait_for(
            asyncio.gather(*list(_BACKGROUND_TASKS), return_exceptions=True),
            timeout=5.0,
        )
        lro.cancel_operation.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_poll_lro_keeps_cancel_that_races_a_finished_status_check(
        self, fake_lro, fake_clock, monkeypatch
    ):
        """A cancel that lands in the same loop step as a status check finishing
        must still raise CancelledError and send CancelOperation.

        asyncio.wait_for on Python 3.10 and 3.11 returns the check's result in
        that case and drops the cancel, so polling would run on until timeout.
        """
        monkeypatch.setattr("gcsfs.poller._calculate_poll_delay", linear_delay(0.2))
        lro = fake_lro.pending(name="projects/_/buckets/b/operations/op-race")
        replay = lro.get_operation.side_effect
        tasks = {}

        def cancel_poller_then_report_pending(*args, **kwargs):
            if lro.get_operation.await_count == 1:
                tasks["poller"].cancel()
            return replay(*args, **kwargs)

        lro.get_operation.side_effect = cancel_poller_then_report_pending
        tasks["poller"] = asyncio.create_task(poll_lro(lro.op, timeout=1.0))

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
    async def test_poll_lro_retries_after_per_poll_timeout(
        self, fake_lro, fake_clock, monkeypatch
    ):
        """A status check that exceeds the per-poll deadline is cancelled and
        counts as a transient error, so the next poll runs."""
        monkeypatch.setattr("gcsfs.poller._PER_POLL_RPC_TIMEOUT", 0.05)
        monkeypatch.setattr("gcsfs.poller._calculate_poll_delay", linear_delay(0.2))
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

        # With no overall limit, a hung check is still cut off per poll.
        await poll_lro(lro.op, timeout=math.inf)

        await asyncio.wait_for(hung_check_cancelled.wait(), timeout=5.0)
        assert lro.get_operation.await_count == 2
        # The timed-out check counts as a transient error, so the next sleep
        # is the first backoff step (0.5 s +-25%).
        assert fake_clock.sleeps[0] == pytest.approx(0.2)
        assert 0.375 <= fake_clock.sleeps[1] <= 0.625
        lro.cancel_operation.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_poll_until_clamps_last_sleep_to_the_deadline(
        self, fake_clock, monkeypatch
    ):
        monkeypatch.setattr("gcsfs.poller._calculate_poll_delay", lambda elapsed: 10.0)
        check = mock.AsyncMock(return_value=(False, None))

        with pytest.raises(asyncio.TimeoutError):
            await _poll_until(check, timeout=1.5)

        assert fake_clock.sleeps == [1.5]
        check.assert_awaited_once()


class TestPollLatencyBudget:
    """Deterministic guard on how late the poller notices a finished operation."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "jitter",
        [
            pytest.param(lambda a, b: a, id="shortest-waits"),
            pytest.param(lambda a, b: b, id="longest-waits"),
        ],
    )
    @pytest.mark.parametrize(
        "server_seconds, max_checks",
        [
            pytest.param(2.0, 12, id="2s-rename"),
            pytest.param(30.0, 31, id="30s-rename"),
            pytest.param(300.0, 51, id="300s-rename"),
        ],
    )
    async def test_poll_until_notices_completion_within_budget(
        self, fake_clock, monkeypatch, server_seconds, max_checks, jitter
    ):
        """The poller notices completion within 25% of the operation's run
        time (slope 0.20 at the top of the jitter range), never later than
        the 30 s cap, and stays within a fixed number of status checks."""
        set_jitter(monkeypatch, jitter)

        async def check():
            return fake_clock.now >= server_seconds, None

        await _poll_until(check, timeout=math.inf)

        lag = fake_clock.now - server_seconds
        assert lag <= min(max(0.25 * server_seconds, 0.25), 30.0)
        assert len(fake_clock.sleeps) <= max_checks
