import asyncio
import logging
import math
import random
import time
from dataclasses import dataclass
from typing import Awaitable, Callable, Optional, Tuple, TypeVar

from gcsfs.retry import is_transient_exception

logger = logging.getLogger("gcsfs")
#: Generic return payload type for polled operations.
T = TypeVar("T")

#: Linear growth rate of poll delay relative to elapsed time (5% of elapsed seconds).
DEFAULT_LRO_POLL_SLOPE: float = 0.05
#: Default minimum delay between polling attempts in seconds (200 ms).
DEFAULT_LRO_POLL_FLOOR: float = 0.200
#: Hard minimum safety floor in seconds (50 ms) to prevent busy-looping or DoS-ing the server.
MIN_SAFE_LRO_POLL_FLOOR: float = 0.050
#: Maximum delay cap between polling attempts in seconds (30 s).
DEFAULT_LRO_POLL_CAP: float = 30.0
#: Lower multiplier bound for uniform random jitter (-25%).
DEFAULT_LRO_JITTER_MIN: float = 0.75
#: Upper multiplier bound for uniform random jitter (+25%).
DEFAULT_LRO_JITTER_MAX: float = 1.25
#: Elapsed duration threshold in seconds above which completed LROs log at INFO instead of DEBUG.
SLOW_LRO_LOG_THRESHOLD: float = 2.0


@dataclass(frozen=True)
class PollStatus:
    """Tracks the progress of an active polling loop (elapsed time and attempt count).

    Attributes:
        total_elapsed: Cumulative monotonic wall-clock seconds elapsed since the
            polling loop began (excluding any initial in-memory check #0). Must be a
            finite, non-negative number.
        attempt: 1-based index of the current polling attempt. Starts at 1 for the
            first scheduled network check and increments after each status query.
    """

    total_elapsed: float
    attempt: int = 1

    def __post_init__(self) -> None:
        if (
            isinstance(self.total_elapsed, bool)
            or not math.isfinite(self.total_elapsed)
            or self.total_elapsed < 0.0
        ):
            raise ValueError(
                f"total_elapsed must be a non-negative finite number, got {self.total_elapsed}"
            )
        # In Python, bool is a subclass of int (isinstance(True, int) is True),
        # so bool must be explicitly rejected before checking isinstance(..., int).
        if (
            isinstance(self.attempt, bool)
            or not isinstance(self.attempt, int)
            or self.attempt < 1
        ):
            raise ValueError(
                f"attempt must be a positive integer >= 1, got {self.attempt}"
            )


class PollSchedule:
    """Calculates how long to wait between polling attempts and when to stop polling.

    Wraps a scheduling function ``Callable[[PollStatus], Optional[float]]`` that
    maps the current polling state to either a delay or termination signal.

    Contract:
        - Returning ``float``: The calculated delay in seconds to sleep before the
          next poll check. Must be non-negative and finite.
        - Returning ``None``: Signals that the schedule recommends terminating the
          polling loop (e.g., maximum duration budget exhausted). When ``None``
          is received, the polling runner aborts and raises
          ``asyncio.TimeoutError``.
    """

    def __init__(self, schedule_fn: Callable[[PollStatus], Optional[float]]):
        """Initializes schedule with underlying delay calculation function."""
        if not callable(schedule_fn):
            raise TypeError("schedule_fn must be callable")
        self._fn = schedule_fn

    def __call__(self, status: PollStatus) -> Optional[float]:
        """Computes the delay in seconds for the next poll attempt, or None to abort."""
        delay = self._fn(status)
        if delay is not None:
            # 0.0 is intentionally permitted here because unfloored schedules (such as
            # linear_elapsed at t=0 or with_jitter when min_factor=0.0) legitimately
            # produce a 0.0s delay before .floor() is chained.
            if isinstance(delay, bool) or not math.isfinite(delay) or delay < 0.0:
                raise ValueError(
                    f"Calculated delay must be a non-negative finite number, got {delay}"
                )
        return delay

    @classmethod
    def linear_elapsed(cls, slope: float = DEFAULT_LRO_POLL_SLOPE) -> "PollSchedule":
        """Linear schedule where delay grows relative to elapsed wall-clock time:

        delay(t) = slope * t.

        Warning:
            At t=0 (the first poll attempt), the calculated delay is 0.0.
            To prevent a busy loop, this schedule should typically be chained
            with a floor, e.g., floor(min_delay).

        Example:
            >>> sched = PollSchedule.linear_elapsed(slope=1.0)
            >>> sched(PollStatus(total_elapsed=10.0))
            10.0
        """
        if isinstance(slope, bool) or not math.isfinite(slope) or slope <= 0.0:
            raise ValueError(f"slope must be a positive finite number, got {slope}")
        return cls(lambda s: slope * max(0.0, s.total_elapsed))

    def floor(self, min_delay: float) -> "PollSchedule":
        """Clamps any calculated delay up to at least min_delay (requiring min_delay >= MIN_SAFE_LRO_POLL_FLOOR).

        Example:
            >>> sched = PollSchedule.linear_elapsed(slope=1.0).floor(2.0)
            >>> sched(PollStatus(total_elapsed=1.0))  # 1.0s clamped up to 2.0s
            2.0
            >>> sched(PollStatus(total_elapsed=5.0))  # 5.0s > 2.0s, unchanged
            5.0
        """
        # Enforce a 50ms hard minimum floor so a caller cannot configure a near-zero
        # delay that would busy-loop and overwhelm (DoS) the Storage Control server.
        if (
            isinstance(min_delay, bool)
            or not math.isfinite(min_delay)
            or min_delay < MIN_SAFE_LRO_POLL_FLOOR
        ):
            raise ValueError(
                f"min_delay must be a finite number >= {MIN_SAFE_LRO_POLL_FLOOR} "
                f"(50ms minimum to prevent server overload), got {min_delay}"
            )

        def _schedule(s: PollStatus) -> Optional[float]:
            d = self._fn(s)
            if d is None or isinstance(d, bool) or not math.isfinite(d):
                return d
            return max(d, min_delay)

        return type(self)(_schedule)

    def cap(self, max_delay: float) -> "PollSchedule":
        """Clamps any calculated delay above max_delay down to max_delay.

        Example:
            >>> sched = PollSchedule.linear_elapsed(slope=1.0).cap(30.0)
            >>> sched(PollStatus(total_elapsed=10.0))  # 10.0s < 30.0s, unchanged
            10.0
            >>> sched(PollStatus(total_elapsed=50.0))  # 50.0s clamped down to 30.0s
            30.0
        """
        # Because .cap() is typically chained after .floor() and computes
        # min(d, max_delay), max_delay must also be >= MIN_SAFE_LRO_POLL_FLOOR (50ms)
        # so a downstream .cap() cannot override an upstream .floor() below 50ms.
        if (
            isinstance(max_delay, bool)
            or not math.isfinite(max_delay)
            or max_delay < MIN_SAFE_LRO_POLL_FLOOR
        ):
            raise ValueError(
                f"max_delay must be a finite number >= {MIN_SAFE_LRO_POLL_FLOOR}, got {max_delay}"
            )

        def _schedule(s: PollStatus) -> Optional[float]:
            d = self._fn(s)
            if d is None or isinstance(d, bool) or not math.isfinite(d):
                return d
            return min(d, max_delay)

        return type(self)(_schedule)

    def with_jitter(
        self,
        min_factor: float = DEFAULT_LRO_JITTER_MIN,
        max_factor: float = DEFAULT_LRO_JITTER_MAX,
        random_fn: Callable[[float, float], float] = random.uniform,
    ) -> "PollSchedule":
        """Applies uniform multiplicative random jitter to the calculated delay.

        Example:
            >>> sched = PollSchedule.linear_elapsed(slope=1.0).with_jitter(0.75, 1.25)
            >>> delay = sched(PollStatus(total_elapsed=10.0))
            >>> 7.5 <= delay <= 12.5
            True
        """
        if (
            isinstance(min_factor, bool)
            or isinstance(max_factor, bool)
            or not (
                math.isfinite(min_factor)
                and math.isfinite(max_factor)
                and 0.0 <= min_factor <= max_factor
            )
        ):
            raise ValueError(
                f"Invalid jitter bounds: [{min_factor}, {max_factor}] "
                "(must be finite numbers satisfying 0 <= min <= max)"
            )

        def _schedule(s: PollStatus) -> Optional[float]:
            d = self._fn(s)
            if d is None or isinstance(d, bool) or not math.isfinite(d):
                return d
            return d * random_fn(min_factor, max_factor)

        return type(self)(_schedule)

    def max_duration(self, max_seconds: float) -> "PollSchedule":
        """Aborts (returns None) once total elapsed time exceeds max_seconds.

        Example:
            >>> sched = PollSchedule.linear_elapsed(slope=1.0).max_duration(10.0)
            >>> sched(PollStatus(total_elapsed=9.5))  # 9.5s clamped to 0.5s remaining budget
            0.5
            >>> sched(PollStatus(total_elapsed=10.0)) is None  # budget exhausted -> stop polling
            True
        """
        if (
            isinstance(max_seconds, bool)
            or not math.isfinite(max_seconds)
            or max_seconds <= 0.0
        ):
            raise ValueError(
                f"max_seconds must be a positive finite number, got {max_seconds}"
            )

        def _schedule(s: PollStatus) -> Optional[float]:
            remaining = max_seconds - s.total_elapsed
            if remaining <= 0:
                return None
            d = self._fn(s)
            if d is None or isinstance(d, bool) or not math.isfinite(d):
                return d
            return min(d, remaining)

        return type(self)(_schedule)


def get_default_hns_lro_cadence() -> PollSchedule:
    """Constructs the default HNS LRO cadence with safety clamps and jitter.

    Multiplicative jitter is applied after ``.floor(DEFAULT_LRO_POLL_FLOOR)``
    and ``.cap(DEFAULT_LRO_POLL_CAP / DEFAULT_LRO_JITTER_MAX)``. This ensures
    that the nominal baseline floor is randomized across distributed clients
    (150-250 ms for the 200 ms floor) and that the upper delay bound retains
    its full jitter spread (18.0-30.0 s at the 24.0 s pre-jitter cap) without
    exceeding ``DEFAULT_LRO_POLL_CAP`` (30.0 s), preventing synchronized
    thundering herds against the control plane.
    """
    return (
        PollSchedule.linear_elapsed(slope=DEFAULT_LRO_POLL_SLOPE)
        .floor(DEFAULT_LRO_POLL_FLOOR)
        .cap(DEFAULT_LRO_POLL_CAP / DEFAULT_LRO_JITTER_MAX)
        .with_jitter(DEFAULT_LRO_JITTER_MIN, DEFAULT_LRO_JITTER_MAX)
    )


def _is_transient_poll_exception(exc: Exception) -> bool:
    """Returns whether a status check error should be retried on the next poll.

    Uses the same predicate as the Storage Control retry policy
    (``gcsfs.retry.is_transient_exception``), plus ``asyncio.TimeoutError``
    raised when a single status check exceeds its per-poll deadline.
    """
    return isinstance(exc, asyncio.TimeoutError) or is_transient_exception(exc)


async def poll_until(
    check_fn: Callable[[PollStatus], Awaitable[Tuple[bool, T]]],
    schedule: PollSchedule,
    operation_id: Optional[str] = None,
    time_fn: Callable[[], float] = time.monotonic,
    sleep_fn: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> T:
    """Polls an async check function until it reports completion or the schedule expires.

    This runner follows a sleep-then-check cadence on every iteration. Callers
    that can satisfy completion synchronously at ``t=0`` should perform that
    initial check before invoking ``poll_until``, or else incur the overhead of
    the first scheduled sleep.

    Args:
        check_fn: Async callback accepting the ``PollStatus`` that the preceding
            delay was computed from (its ``total_elapsed`` excludes that sleep)
            and returning a ``(is_done, result)`` tuple. Transient errors (those
            matched by ``gcsfs.retry.is_transient_exception``, plus
            ``asyncio.TimeoutError``) are caught and retried on the next
            scheduled attempt; any other exception is re-raised.
        schedule: ``PollSchedule`` returning the delay in seconds before the
            next attempt, or ``None`` to abort polling.
        operation_id: Optional identifier included in debug and warning logs.
        time_fn: Monotonic clock function returning current time in seconds.
        sleep_fn: Async sleep function accepting a delay in seconds.

    Returns:
        The final payload ``T`` returned by ``check_fn`` when ``is_done`` is True.

    Raises:
        TypeError: If ``schedule`` is not a ``PollSchedule``.
        asyncio.TimeoutError: If ``schedule`` returns ``None`` before ``check_fn``
            reports completion. If the most recent status check failed with
            a transient error, that error is chained as ``__cause__``.
    """
    if not isinstance(schedule, PollSchedule):
        raise TypeError(
            f"schedule must be a PollSchedule, got {type(schedule).__name__}"
        )

    start_time = time_fn()
    op_desc = f" for '{operation_id}'" if operation_id else ""
    logger.debug("Starting polling%s...", op_desc)

    attempts = 1
    # Transient error from the most recent status check, if it failed; chained
    # onto the timeout so callers can see why polling never succeeded.
    last_exc: Optional[Exception] = None

    # Sleep before each status check; callers should perform any t=0 pre-check
    # prior to calling poll_until to avoid an immediate redundant network RPC.
    while True:
        now = time_fn()
        elapsed = now - start_time
        status = PollStatus(
            total_elapsed=elapsed,
            attempt=attempts,
        )

        delay = schedule(status)
        if delay is None:
            # ``attempts`` is the number of the check that would have run next.
            checks = attempts - 1
            checks_desc = f"{checks} status check{'' if checks == 1 else 's'}"
            logger.warning(
                "Polling timed out%s after %.2fs and %s.",
                op_desc,
                elapsed,
                checks_desc,
            )
            raise asyncio.TimeoutError(
                f"Polling timed out{op_desc} after {elapsed:.2f}s and {checks_desc}."
            ) from last_exc

        logger.debug(
            "Poll attempt #%d%s: elapsed=%.3fs, sleeping %.3fs before check",
            attempts,
            op_desc,
            elapsed,
            delay,
        )
        await sleep_fn(delay)

        try:
            is_done, result = await check_fn(status)
            last_exc = None
            if is_done:
                final_elapsed = time_fn() - start_time
                log_level = (
                    logging.INFO
                    if final_elapsed >= SLOW_LRO_LOG_THRESHOLD
                    else logging.DEBUG
                )
                logger.log(
                    log_level,
                    "Polling completed%s in %.3fs across %d attempts.",
                    op_desc,
                    final_elapsed,
                    attempts,
                )
                return result
        except Exception as e:
            if not _is_transient_poll_exception(e):
                raise
            last_exc = e
            logger.debug(
                "Transient transport error during status check #%d%s: %s",
                attempts,
                op_desc,
                e,
            )

        attempts += 1
