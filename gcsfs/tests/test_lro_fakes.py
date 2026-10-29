"""Unit tests for the fake_lro fixture (gcsfs/tests/lro_fakes.py)."""

import asyncio

import grpc
import pytest
from fsspec import asyn
from google.api_core import exceptions as api_exceptions
from google.cloud import storage_control_v2
from google.longrunning import operations_pb2
from google.protobuf import empty_pb2
from google.rpc import code_pb2, status_pb2

from gcsfs.tests.lro_fakes import DEFAULT_LRO_NAME, FakeLroFactory, operation_pb


@pytest.fixture
def fake_lro():
    """Same as the conftest fixture, so this module also runs with --noconftest."""
    return FakeLroFactory()


class TestOperationPb:
    def test_pending_by_default(self):
        pb = operation_pb()
        assert pb.name == DEFAULT_LRO_NAME
        assert not pb.done
        assert pb.WhichOneof("result") is None

    def test_done_carries_empty_payload(self):
        pb = operation_pb(done=True)
        assert pb.done
        assert pb.response.Is(empty_pb2.Empty.DESCRIPTOR)

    def test_error_implies_done(self):
        pb = operation_pb(error=code_pb2.ABORTED)
        assert pb.done
        assert pb.error.code == code_pb2.ABORTED

    def test_rejects_response_and_error(self):
        with pytest.raises(ValueError, match="not both"):
            operation_pb(response=empty_pb2.Empty(), error=code_pb2.ABORTED)


class TestFakeLroConstructors:
    @pytest.mark.asyncio
    async def test_pending_never_finishes(self, fake_lro):
        lro = fake_lro.pending()

        assert not lro.op.operation.done
        for _ in range(3):
            assert not await lro.op.done(retry=None)
        assert lro.get_operation.await_count == 3

    @pytest.mark.asyncio
    async def test_succeeded_at_t0_needs_no_poll(self, fake_lro):
        lro = fake_lro.succeeded()

        assert lro.op.operation.done
        assert lro.op.operation.name == DEFAULT_LRO_NAME
        assert await lro.op.result() == empty_pb2.Empty()
        lro.get_operation.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_succeeded_with_proto_plus_response(self, fake_lro):
        folder = storage_control_v2.Folder(name="projects/_/buckets/b/folders/dst/")
        lro = fake_lro.succeeded(folder, name="projects/_/buckets/b/operations/op-1")

        assert lro.op.operation.name == "projects/_/buckets/b/operations/op-1"
        assert await lro.op.result() == folder

    @pytest.mark.asyncio
    async def test_succeeded_after_n_polls(self, fake_lro):
        lro = fake_lro.succeeded(after=3)

        assert not lro.op.operation.done
        assert [await lro.op.done(retry=None) for _ in range(3)] == [
            False,
            False,
            True,
        ]
        assert await lro.op.result() == empty_pb2.Empty()
        assert lro.get_operation.await_count == 3

    @pytest.mark.asyncio
    async def test_failed_at_t0_with_code(self, fake_lro):
        lro = fake_lro.failed(code_pb2.ABORTED)

        assert lro.op.operation.done
        with pytest.raises(api_exceptions.GoogleAPICallError) as exc_info:
            await lro.op.result()
        # api-core surfaces LRO errors as a bare GoogleAPICallError whose
        # errors[0] is the google.rpc.Status from the operation.
        assert type(exc_info.value) is api_exceptions.GoogleAPICallError
        assert exc_info.value.errors[0].code == code_pb2.ABORTED
        lro.get_operation.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_failed_after_n_polls_with_status(self, fake_lro):
        status = status_pb2.Status(code=code_pb2.NOT_FOUND, message="gone")
        lro = fake_lro.failed(status, after=2)

        assert [await lro.op.done(retry=None) for _ in range(2)] == [False, True]
        with pytest.raises(api_exceptions.GoogleAPICallError, match="gone"):
            await lro.op.result()

    @pytest.mark.parametrize("bad_error", [grpc.StatusCode.ABORTED, True, "ABORTED"])
    def test_failed_rejects_non_code_pb2_errors(self, fake_lro, bad_error):
        with pytest.raises(TypeError, match="code_pb2"):
            fake_lro.failed(bad_error)

    @pytest.mark.parametrize("bad_after", [-1, True, 1.5])
    def test_rejects_invalid_after(self, fake_lro, bad_after):
        with pytest.raises(ValueError, match="after must be an int >= 0"):
            fake_lro.succeeded(after=bad_after)
        with pytest.raises(ValueError, match="after must be an int >= 0"):
            fake_lro.failed(code_pb2.ABORTED, after=bad_after)

    @pytest.mark.asyncio
    async def test_sequence_replays_steps_and_repeats_last(self, fake_lro):
        lro = fake_lro.sequence(
            [
                api_exceptions.ServiceUnavailable("blip"),
                operation_pb(),
                operation_pb(done=True),
            ]
        )

        assert not lro.op.operation.done
        with pytest.raises(api_exceptions.ServiceUnavailable):
            await lro.op.done(retry=None)
        assert not await lro.op.done(retry=None)
        assert await lro.op.done(retry=None)
        # Once done, api-core stops polling; result() reads the cached proto.
        assert await lro.op.result() == empty_pb2.Empty()
        assert lro.get_operation.await_count == 3

    @pytest.mark.asyncio
    async def test_sequence_repeats_last_exception(self, fake_lro):
        lro = fake_lro.sequence([api_exceptions.ServiceUnavailable("down")])

        for _ in range(3):
            with pytest.raises(api_exceptions.ServiceUnavailable):
                await lro.op.done(retry=None)

    def test_sequence_rejects_empty(self, fake_lro):
        with pytest.raises(ValueError, match="must not be empty"):
            fake_lro.sequence([])

    def test_sequence_rejects_done_step_without_payload(self, fake_lro):
        with pytest.raises(ValueError, match="response or error"):
            fake_lro.sequence([operations_pb2.Operation(done=True)])

    def test_sequence_rejects_non_proto_steps(self, fake_lro):
        with pytest.raises(TypeError, match="operations_pb2.Operation"):
            fake_lro.sequence([True])


class TestFakeLroCancel:
    @pytest.mark.asyncio
    async def test_cancel_pending_polls_then_cancels(self, fake_lro):
        lro = fake_lro.pending()

        assert await lro.op.cancel() is True
        lro.get_operation.assert_awaited_once()
        lro.cancel_operation.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_cancel_done_is_noop(self, fake_lro):
        lro = fake_lro.succeeded()

        assert await lro.op.cancel() is False
        lro.cancel_operation.assert_not_awaited()


class TestFakeLroEventLoop:
    @pytest.mark.asyncio
    async def test_builds_on_running_loop(self, fake_lro):
        lro = fake_lro.pending()

        assert lro.op._future.get_loop() is asyncio.get_running_loop()

    def test_builds_on_fsspec_loop_without_running_loop(self, fake_lro):
        lro = fake_lro.succeeded()

        assert lro.op._future.get_loop() is asyn.get_loop()
        assert asyn.sync(asyn.get_loop(), lro.op.result) == empty_pb2.Empty()

    def test_failed_op_is_readable_from_sync_code(self, fake_lro):
        lro = fake_lro.failed(code_pb2.ABORTED)

        with pytest.raises(api_exceptions.GoogleAPICallError):
            asyn.sync(asyn.get_loop(), lro.op.result)
