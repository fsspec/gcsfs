import asyncio
import logging
import random
import time
from typing import Any, Awaitable, Callable, Optional, Tuple, TypeVar

from google.api_core import exceptions as api_exceptions
from google.api_core.operation_async import AsyncOperation

from gcsfs.retry import is_transient_exception

logger = logging.getLogger("gcsfs")
T = TypeVar("T")
#: Strong references, so fire-and-forget cancel tasks are not garbage collected.
_BACKGROUND_TASKS: set[asyncio.Task[Any]] = set()

#: Default overall timeout in seconds for HNS folder rename LROs.
DEFAULT_HNS_LRO_TIMEOUT: float = 300.0
_PER_POLL_RPC_TIMEOUT: float = 15.0
_MIN_PER_POLL_RPC_TIMEOUT: float = 1.0
_SLOW_LRO_LOG_THRESHOLD: float = 2.0
_POLL_DELAY_SLOPE: float = 0.20
_POLL_DELAY_FLOOR: float = 0.2
_POLL_DELAY_CAP: float = 30.0
_JITTER_MIN: float = 0.75
_JITTER_MAX: float = 1.25
_TRANSIENT_ERROR_BACKOFF_BASE: float = 0.5
_TRANSIENT_ERROR_BACKOFF_CAP: float = 30.0

# Alias the clock and delay calls here so unit tests can fast-forward
# waits and leave the event loop untouched.
_monotonic = time.monotonic
_sleep = asyncio.sleep


def _calculate_poll_delay(elapsed: float) -> float:
    """Returns the jittered delay in seconds before the next status check."""
    # Capping at cap / max jitter keeps the jittered delay at or below the cap.
    delay = min(
        max(elapsed * _POLL_DELAY_SLOPE, _POLL_DELAY_FLOOR),
        _POLL_DELAY_CAP / _JITTER_MAX,
    )
    return delay * random.uniform(_JITTER_MIN, _JITTER_MAX)


def _is_transient_poll_exception(exc: Exception) -> bool:
    """Returns whether a failed status check should be retried on the next poll."""
    return isinstance(exc, asyncio.TimeoutError) or is_transient_exception(exc)


async def _poll_until(
    check_fn: Callable[[], Awaitable[Tuple[bool, T]]],
    *,
    timeout: float,
    description: Optional[str] = None,
) -> T:
    """Sleeps, then calls ``check_fn``, until it reports done or ``timeout`` passes."""
    start_time = _monotonic()
    op_desc = f" for '{description}'" if description else ""
    logger.debug("Starting polling%s...", op_desc)

    attempts = 1
    transient_backoff = 0.0
    last_exc: Optional[Exception] = None
    while True:
        elapsed = _monotonic() - start_time
        remaining = timeout - elapsed
        if remaining <= 0:
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

        delay = _calculate_poll_delay(elapsed)
        if transient_backoff:
            delay = max(
                delay, transient_backoff * random.uniform(_JITTER_MIN, _JITTER_MAX)
            )
        delay = min(delay, remaining)
        logger.debug(
            "Poll attempt #%d%s: elapsed=%.3fs, sleeping %.3fs before check",
            attempts,
            op_desc,
            elapsed,
            delay,
        )
        await _sleep(delay)

        try:
            is_done, result = await check_fn()
            last_exc = None
            transient_backoff = 0.0
            if is_done:
                final_elapsed = _monotonic() - start_time
                log_level = (
                    logging.INFO
                    if final_elapsed >= _SLOW_LRO_LOG_THRESHOLD
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
            transient_backoff = min(
                max(transient_backoff * 2, _TRANSIENT_ERROR_BACKOFF_BASE),
                _TRANSIENT_ERROR_BACKOFF_CAP / _JITTER_MAX,
            )
            logger.debug(
                "Transient transport error during status check #%d%s: %s",
                attempts,
                op_desc,
                e,
            )

        attempts += 1


async def unwrap_operation_result(operation: AsyncOperation) -> Any:
    """Awaits ``operation.result()``, raising a typed error if the operation failed.

    Args:
        operation: The operation to wait for.

    Raises:
        google.api_core.exceptions.GoogleAPICallError: The subclass for the
            operation's error code (for example ``NotFound``), or the original
            error if it has no int code.
    """
    try:
        return await operation.result()
    except api_exceptions.GoogleAPICallError as e:
        if type(e) is api_exceptions.GoogleAPICallError and e.errors:
            code = getattr(e.errors[0], "code", None)
            if isinstance(code, int):
                raise api_exceptions.from_grpc_status(
                    code,
                    e.message,
                    errors=e.errors,
                    response=e.response,
                ) from e
        raise


def _get_operation_name(operation: AsyncOperation) -> Optional[str]:
    """Returns the server-side operation name, or ``None`` if empty."""
    return operation.operation.name or None


async def _wait_keeping_cancel(aw: Awaitable[T], timeout: float) -> T:
    """Awaits ``aw`` for at most ``timeout`` seconds without losing a cancel."""
    # On Python 3.10 and 3.11, asyncio.wait_for returns the result instead of
    # raising CancelledError when the calling task is cancelled in the same
    # loop step in which aw finishes, so the cancel is lost. asyncio.wait does
    # not catch the cancel, so it always propagates.
    task = asyncio.ensure_future(aw)

    def _retrieve_outcome(abandoned: "asyncio.Future[T]") -> None:
        # Read the abandoned task's error so asyncio does not log it as unretrieved.
        if not abandoned.cancelled():
            abandoned.exception()

    try:
        done, _ = await asyncio.wait({task}, timeout=timeout)
    finally:
        if not task.done():
            task.cancel()
            task.add_done_callback(_retrieve_outcome)
    if not done:
        raise asyncio.TimeoutError(f"Status check exceeded {timeout:.2f}s.")
    return task.result()


async def poll_lro(
    operation: AsyncOperation,
    *,
    timeout: float,
    path1: Optional[str] = None,
    path2: Optional[str] = None,
    request_id: Optional[str] = None,
) -> Any:
    """Waits for a GAPIC ``AsyncOperation`` to finish and returns its result.

    Args:
        operation: The operation to wait for.
        timeout: Overall limit in seconds; use ``math.inf`` for no limit.
        path1: Optional source path for diagnostic logging context.
        path2: Optional destination path for diagnostic logging context.
        request_id: Optional fallback identifier when the operation has no name.

    Raises:
        ValueError: If ``timeout`` is not a positive number.
        asyncio.TimeoutError: If ``timeout`` passes before the operation ends.
        asyncio.CancelledError: If cancelled; ``operation.cancel()`` is then
            sent in the background.
        google.api_core.exceptions.GoogleAPICallError: If the operation failed.
    """
    if not timeout > 0:
        raise ValueError(
            "timeout must be a positive number of seconds, or math.inf for no "
            f"limit; got {timeout!r}"
        )

    op_id = _get_operation_name(operation) or request_id or "LRO"
    path_ctx = f"'{path1}' -> '{path2}'" if path1 and path2 else op_id

    if operation.operation.done:
        logger.debug(
            "LRO %s completed synchronously in memory at t=0; skipping polling.",
            path_ctx,
        )
        return await unwrap_operation_result(operation)

    start_time = _monotonic()

    async def lro_complete() -> Tuple[bool, None]:
        remaining_budget = max(0.0, timeout - (_monotonic() - start_time))
        rpc_timeout = min(
            _PER_POLL_RPC_TIMEOUT, max(_MIN_PER_POLL_RPC_TIMEOUT, remaining_budget)
        )

        # retry=None: the GAPIC default retry would add a second, slower retry
        # loop inside a single poll; _poll_until already retries transient errors.
        # A per-poll timeout raises asyncio.TimeoutError, which _poll_until
        # treats as transient.
        is_done = await _wait_keeping_cancel(
            operation.done(retry=None), timeout=rpc_timeout
        )
        return bool(is_done), None

    try:
        await _poll_until(lro_complete, timeout=timeout, description=op_id)
    except asyncio.CancelledError:
        elapsed = _monotonic() - start_time
        logger.warning(
            "LRO %s polling cancelled after %.3fs; "
            "dispatching server-side cancellation signal.",
            op_id,
            elapsed,
        )

        async def _send_cancel() -> None:
            # AsyncOperation.cancel() first awaits done() with api-core's
            # default retry, so this can back off and retry; wait_for bounds
            # the whole call, including that extra poll.
            try:
                await asyncio.wait_for(
                    operation.cancel(), timeout=_PER_POLL_RPC_TIMEOUT
                )
            except Exception as exc:
                logger.debug(
                    "Failed to send LRO cancellation signal for %s: %s",
                    op_id,
                    exc,
                )

        cancel_task = asyncio.create_task(_send_cancel())
        _BACKGROUND_TASKS.add(cancel_task)
        cancel_task.add_done_callback(_BACKGROUND_TASKS.discard)
        raise

    return await unwrap_operation_result(operation)
