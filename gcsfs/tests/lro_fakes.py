"""Long-running operation (LRO) test doubles shaped like GAPIC's.

Each ``FakeLroFactory`` method returns a ``FakeLro`` whose ``op`` is a real
``AsyncOperation`` over a real ``google.longrunning.Operation`` proto; only the
GetOperation and CancelOperation transport calls are mocked::

    lro = fake_lro.succeeded(after=3)          # done on the 3rd poll
    lro = fake_lro.failed(code_pb2.ABORTED)    # failed at t=0
    lro = fake_lro.pending()                   # never finishes
    lro = fake_lro.sequence([ServiceUnavailable("blip"), operation_pb(done=True)])

An ``AsyncOperation`` is bound to the event loop it was built on, so await it
there: the factory builds on the running loop in async tests and on fsspec's IO
loop (where sync ``gcsfs`` calls run) otherwise.
"""

import asyncio
from dataclasses import dataclass
from typing import Callable, Optional, Sequence, Union
from unittest import mock

from fsspec import asyn
from google.api_core import operation_async
from google.longrunning import operations_pb2
from google.protobuf import empty_pb2
from google.rpc import code_pb2, status_pb2

#: Server-side name given to fake operations unless ``name=`` is passed.
DEFAULT_LRO_NAME = "projects/_/buckets/test-bucket/operations/op-test"

#: An LRO error: a ``google.rpc.code_pb2`` int or a full ``status_pb2.Status``.
ErrorSpec = Union[int, status_pb2.Status]
#: One GetOperation outcome: the returned proto, or an exception to raise.
Step = Union[operations_pb2.Operation, BaseException]


def _to_status(error: ErrorSpec) -> status_pb2.Status:
    """Converts an ``ErrorSpec`` into the ``google.rpc.Status`` an LRO carries."""
    if isinstance(error, status_pb2.Status):
        return error
    # google.rpc.Status.code is a plain code_pb2 int. bool is an int subclass
    # and grpc.StatusCode is an Enum; both are rejected so tests can't build a
    # shape the server never sends.
    if isinstance(error, bool) or not isinstance(error, int):
        raise TypeError(
            "error must be a google.rpc.code_pb2 int (e.g. code_pb2.ABORTED) or a "
            f"status_pb2.Status, got {type(error).__name__}"
        )
    try:
        label = code_pb2.Code.Name(error)
    except ValueError:
        label = str(error)
    return status_pb2.Status(code=error, message=f"LRO failed: {label}")


def operation_pb(
    *,
    name: str = DEFAULT_LRO_NAME,
    done: bool = False,
    response=None,
    error: Optional[ErrorSpec] = None,
) -> operations_pb2.Operation:
    """Builds a ``google.longrunning.Operation`` proto, e.g. for ``sequence()``.

    ``response`` and ``error`` both imply ``done=True``. A done operation always
    carries a payload (``Empty`` by default), as a real server's does.

    Args:
        name: Server-side operation name.
        done: Whether the operation is finished.
        response: Response message (protobuf or proto-plus) for a successful
            operation.
        error: ``code_pb2`` int or ``status_pb2.Status`` for a failed operation.

    Raises:
        ValueError: If both ``response`` and ``error`` are given.
        TypeError: If ``error`` is neither a ``code_pb2`` int nor a ``Status``.
    """
    if response is not None and error is not None:
        raise ValueError("pass response or error, not both")
    is_done = bool(done or response is not None or error is not None)
    pb = operations_pb2.Operation(name=name, done=is_done)
    if error is not None:
        pb.error.CopyFrom(_to_status(error))
    elif is_done:
        payload = empty_pb2.Empty() if response is None else response
        # proto-plus messages (e.g. storage_control_v2.Folder) wrap a raw proto.
        if callable(getattr(type(payload), "pb", None)):
            payload = type(payload).pb(payload)
        pb.response.Pack(payload)
    return pb


@dataclass(frozen=True)
class FakeLro:
    """A real ``AsyncOperation`` plus the mocks for its transport calls.

    Attributes:
        op: The ``AsyncOperation`` to hand to the code under test.
        get_operation: GetOperation mock; each await is one server poll.
        cancel_operation: CancelOperation mock.
    """

    op: operation_async.AsyncOperation
    get_operation: mock.AsyncMock
    cancel_operation: mock.AsyncMock


def _replay(steps: Sequence[Step]) -> Callable[..., operations_pb2.Operation]:
    """Returns a side effect that yields ``steps`` in order, repeating the last."""
    calls = 0

    def _next(*_args, **_kwargs) -> operations_pb2.Operation:
        nonlocal calls
        step = steps[min(calls, len(steps) - 1)]
        calls += 1
        if isinstance(step, BaseException):
            raise step
        return step

    return _next


def _check_after(after: int) -> None:
    if isinstance(after, bool) or not isinstance(after, int) or after < 0:
        raise ValueError(f"after must be an int >= 0, got {after!r}")


class FakeLroFactory:
    """Builds ``FakeLro`` doubles; exposed to tests as the ``fake_lro`` fixture.

    All constructors accept ``name=`` (the server-side operation name) and
    ``result_type=`` (the response message type ``op.result()`` unpacks;
    ``Empty`` unless inferred from ``response``).
    """

    def pending(
        self, *, name: str = DEFAULT_LRO_NAME, result_type: Optional[type] = None
    ) -> FakeLro:
        """An operation that stays pending on every poll."""
        pending = operation_pb(name=name)
        return self._build(pending, [pending], result_type)

    def succeeded(
        self,
        response=None,
        after: int = 0,
        *,
        name: str = DEFAULT_LRO_NAME,
        result_type: Optional[type] = None,
    ) -> FakeLro:
        """An operation that succeeds with ``response`` (``Empty`` by default).

        Args:
            response: Response message (protobuf or proto-plus).
            after: Polls until success; 0 means already done at t=0.
            name: Server-side operation name.
            result_type: Response type; defaults to ``type(response)``.
        """
        terminal = operation_pb(name=name, done=True, response=response)
        if result_type is None and response is not None:
            result_type = type(response)
        return self._terminal(terminal, after, name, result_type)

    def failed(
        self,
        error: ErrorSpec,
        after: int = 0,
        *,
        name: str = DEFAULT_LRO_NAME,
        result_type: Optional[type] = None,
    ) -> FakeLro:
        """An operation that fails with ``error``.

        Args:
            error: ``code_pb2`` int (e.g. ``code_pb2.ABORTED``) or a
                ``status_pb2.Status``. ``grpc.StatusCode`` and ``bool`` are
                rejected.
            after: Polls until failure; 0 means already failed at t=0.
            name: Server-side operation name.
            result_type: Response type of the (never produced) result.
        """
        terminal = operation_pb(name=name, error=error)
        return self._terminal(terminal, after, name, result_type)

    def sequence(
        self,
        steps: Sequence[Step],
        *,
        name: str = DEFAULT_LRO_NAME,
        result_type: Optional[type] = None,
    ) -> FakeLro:
        """An operation that starts pending and replays ``steps``, one per poll.

        Args:
            steps: Non-empty GetOperation outcomes: ``operation_pb(...)`` protos
                or exceptions to raise (e.g. transient RPC errors). The last
                step repeats once exhausted.
            name: Server-side name of the initial (pending) operation.
            result_type: Response type; ``Empty`` by default.

        Raises:
            ValueError: If ``steps`` is empty or a done step has no payload.
            TypeError: If a step is neither an ``Operation`` nor an exception.
        """
        steps = list(steps)
        if not steps:
            raise ValueError("steps must not be empty")
        for step in steps:
            if isinstance(step, BaseException):
                continue
            if not isinstance(step, operations_pb2.Operation):
                raise TypeError(
                    "steps must be operations_pb2.Operation protos or exceptions, "
                    f"got {type(step).__name__}"
                )
            if step.done and step.WhichOneof("result") is None:
                raise ValueError(
                    "done steps must carry a response or error; use operation_pb()"
                )
        return self._build(operation_pb(name=name), steps, result_type)

    def _terminal(
        self,
        terminal: operations_pb2.Operation,
        after: int,
        name: str,
        result_type: Optional[type],
    ) -> FakeLro:
        _check_after(after)
        if after == 0:
            # api-core never polls an operation that is already done.
            return self._build(terminal, [terminal], result_type)
        pending = operation_pb(name=name)
        return self._build(pending, [pending] * (after - 1) + [terminal], result_type)

    @staticmethod
    def _build(
        initial: operations_pb2.Operation,
        steps: Sequence[Step],
        result_type: Optional[type],
    ) -> FakeLro:
        get_operation = mock.AsyncMock(side_effect=_replay(steps))
        # The real AsyncOperation.cancel() awaits done() first: one extra
        # get_operation call, and cancel_operation is only awaited while the
        # op is still pending.
        cancel_operation = mock.AsyncMock(return_value=None)

        def _new_op() -> operation_async.AsyncOperation:
            return operation_async.AsyncOperation(
                initial,
                get_operation,
                cancel_operation,
                result_type=result_type or empty_pb2.Empty,
            )

        try:
            asyncio.get_running_loop()
        except RuntimeError:
            # AsyncFuture.__init__ binds its future to the current loop, and a
            # done op resolves it immediately, so build on the loop that sync
            # gcsfs calls run on.
            async def _new_op_async() -> operation_async.AsyncOperation:
                return _new_op()

            op = asyn.sync(asyn.get_loop(), _new_op_async)
        else:
            op = _new_op()
        return FakeLro(
            op=op, get_operation=get_operation, cancel_operation=cancel_operation
        )
