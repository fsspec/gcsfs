import math
import random
from dataclasses import dataclass
from typing import Callable, Optional

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
        if not math.isfinite(self.total_elapsed) or self.total_elapsed < 0.0:
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
        self._fn = schedule_fn

    def __call__(self, status: PollStatus) -> Optional[float]:
        """Computes the delay in seconds for the next poll attempt, or None to abort."""
        return self._fn(status)

    @classmethod
    def linear_elapsed(cls, slope: float = DEFAULT_LRO_POLL_SLOPE) -> "PollSchedule":
        """Linear schedule where delay grows relative to elapsed wall-clock time:

        delay(t) = slope * t.

        Example:
            >>> sched = PollSchedule.linear_elapsed(slope=1.0)
            >>> sched(PollStatus(total_elapsed=10.0))
            10.0
        """
        if not math.isfinite(slope) or slope <= 0.0:
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
        if not math.isfinite(min_delay) or min_delay < MIN_SAFE_LRO_POLL_FLOOR:
            raise ValueError(
                f"min_delay must be a finite number >= {MIN_SAFE_LRO_POLL_FLOOR} "
                f"(50ms minimum to prevent server overload), got {min_delay}"
            )
        return type(self)(
            lambda s: max(min_delay, d) if (d := self(s)) is not None else None
        )

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
        # min(max_delay, d), max_delay must also be >= MIN_SAFE_LRO_POLL_FLOOR (50ms)
        # so a downstream .cap() cannot override an upstream .floor() below 50ms.
        if not math.isfinite(max_delay) or max_delay < MIN_SAFE_LRO_POLL_FLOOR:
            raise ValueError(
                f"max_delay must be a finite number >= {MIN_SAFE_LRO_POLL_FLOOR}, got {max_delay}"
            )
        return type(self)(
            lambda s: min(max_delay, d) if (d := self(s)) is not None else None
        )

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
        if not (
            math.isfinite(min_factor)
            and math.isfinite(max_factor)
            and 0.0 <= min_factor <= max_factor
        ):
            raise ValueError(
                f"Invalid jitter bounds: [{min_factor}, {max_factor}] "
                "(must be finite numbers satisfying 0 <= min <= max)"
            )
        return type(self)(
            lambda s: (
                (d * random_fn(min_factor, max_factor))
                if (d := self(s)) is not None
                else None
            )
        )

    def max_duration(self, max_seconds: float) -> "PollSchedule":
        """Aborts (returns None) once total elapsed time exceeds max_seconds.

        Example:
            >>> sched = PollSchedule.linear_elapsed(slope=1.0).max_duration(10.0)
            >>> sched(PollStatus(total_elapsed=9.5))  # 9.5s clamped to 0.5s remaining budget
            0.5
            >>> sched(PollStatus(total_elapsed=10.0)) is None  # budget exhausted -> stop polling
            True
        """
        if not math.isfinite(max_seconds) or max_seconds <= 0.0:
            raise ValueError(
                f"max_seconds must be a positive finite number, got {max_seconds}"
            )

        def _schedule(s: PollStatus) -> Optional[float]:
            remaining = max_seconds - s.total_elapsed
            if remaining <= 0:
                return None
            delay = self(s)
            return min(delay, remaining) if delay is not None else None

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
