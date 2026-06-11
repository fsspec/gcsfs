import asyncio
import collections
import concurrent.futures
import logging
from unittest import mock

import pytest
from google.api_core.exceptions import NotFound

from gcsfs import zb_hns_utils
from gcsfs.zb_hns_utils import DirectMemmoveBuffer, MRDPool, MRDPoolCache, _close_mrds

mock_grpc_client = mock.Mock()
bucket_name = "test-bucket"
object_name = "test-object"
generation = "12345"


@pytest.mark.asyncio
async def test_shared_mrd_closed_only_by_last_holder():
    class FakeMRD:
        def __init__(self):
            self.persisted_size = 0
            self.close_count = 0

        async def close(self):
            self.close_count += 1

    pool = MRDPool(mock.Mock(), "b", "o", 1, pool_size=1, cache=None)
    pool.mrd_supports_multi_request = True
    pool._create_mrd = mock.AsyncMock(side_effect=lambda: FakeMRD())

    await pool.initialize()

    cm_a = pool.get_mrd()
    mrd_a = await cm_a.__aenter__()  # exclusive
    cm_b = pool.get_mrd()
    mrd_b = await cm_b.__aenter__()  # round-robin share
    assert mrd_a is mrd_b  # same MRD shared

    await pool.close()
    assert mrd_a.close_count == 0  # still in use -> not closed

    await cm_a.__aexit__(None, None, None)
    assert mrd_a.close_count == 0  # B still holds it

    await cm_b.__aexit__(None, None, None)
    assert mrd_a.close_count == 1  # last holder closes it exactly once


@pytest.mark.asyncio
async def test_download_range():
    """
    Tests that download_range calls mrd.download_ranges with the correct
    parameters and returns the data written to the buffer.
    """
    offset = 10
    length = 20
    mock_mrd = mock.AsyncMock()
    expected_data = b"test data from download"

    # Simulate the download_ranges method writing data to the buffer
    async def mock_download_ranges(ranges):
        _offset, _length, buffer = ranges[0]
        buffer.write(expected_data)

    mock_mrd.download_ranges.side_effect = mock_download_ranges

    result = await zb_hns_utils.download_range(offset, length, mock_mrd)

    mock_mrd.download_ranges.assert_called_once_with([(offset, length, mock.ANY)])
    assert result == expected_data


@pytest.mark.asyncio
async def test_init_aaow():
    """
    Tests that init_aaow calls the underlying AsyncAppendableObjectWriter.open
    method and returns its result.
    """
    mock_writer_instance = mock.AsyncMock()
    with mock.patch(
        "gcsfs.zb_hns_utils.AsyncAppendableObjectWriter",
        new_callable=mock.Mock,
        return_value=mock_writer_instance,
    ) as mock_writer_class:
        result = await zb_hns_utils.init_aaow(
            mock_grpc_client, bucket_name, object_name, generation
        )

        mock_writer_class.assert_called_once_with(
            client=mock_grpc_client,
            bucket_name=bucket_name,
            object_name=object_name,
            generation=generation,
            writer_options={},
        )
        mock_writer_instance.open.assert_awaited_once()
        assert result is mock_writer_instance


@pytest.mark.asyncio
async def test_init_aaow_with_flush_interval_bytes():
    """
    Tests that init_aaow correctly passes the flush_interval_bytes
    parameter to the AsyncAppendableObjectWriter.
    """
    mock_writer_instance = mock.AsyncMock()
    with mock.patch(
        "gcsfs.zb_hns_utils.AsyncAppendableObjectWriter",
        new_callable=mock.Mock,
        return_value=mock_writer_instance,
    ) as mock_writer_class:
        result = await zb_hns_utils.init_aaow(
            mock_grpc_client,
            bucket_name,
            object_name,
            generation,
            flush_interval_bytes=1024,
        )

        mock_writer_class.assert_called_once_with(
            client=mock_grpc_client,
            bucket_name=bucket_name,
            object_name=object_name,
            generation=generation,
            writer_options={"FLUSH_INTERVAL_BYTES": 1024},
        )
        mock_writer_instance.open.assert_awaited_once()
        assert result is mock_writer_instance


@pytest.mark.asyncio
async def test_init_mrd_success():
    """Tests successful initialization of MRD."""
    mock_mrd_instance = mock.Mock()
    with mock.patch(
        "gcsfs.zb_hns_utils.AsyncMultiRangeDownloader.create_mrd",
        new_callable=mock.AsyncMock,
        return_value=mock_mrd_instance,
    ) as mock_create_mrd:
        result = await zb_hns_utils.init_mrd(
            mock_grpc_client, bucket_name, object_name, generation
        )

        mock_create_mrd.assert_awaited_once_with(
            mock_grpc_client, bucket_name, object_name, generation
        )
        assert result is mock_mrd_instance


@pytest.mark.asyncio
async def test_init_mrd_not_found():
    """Tests that init_mrd raises FileNotFoundError when object is not found."""

    with mock.patch(
        "gcsfs.zb_hns_utils.AsyncMultiRangeDownloader.create_mrd",
        new_callable=mock.AsyncMock,
    ) as mock_create_mrd:
        mock_create_mrd.side_effect = NotFound("Object not found")

        with pytest.raises(FileNotFoundError) as excinfo:
            await zb_hns_utils.init_mrd(
                mock_grpc_client, bucket_name, object_name, generation
            )

        assert f"{bucket_name}/{object_name}" in str(excinfo.value)


@pytest.mark.asyncio
async def test_close_aaow(caplog):
    """Tests all graceful closing scenarios for AsyncAppendableObjectWriter."""
    # 1. Handles None gracefully
    await zb_hns_utils.close_aaow(None)

    # 2. Closes successfully
    mock_aaow = mock.AsyncMock()
    await zb_hns_utils.close_aaow(mock_aaow, finalize_on_close=True)
    mock_aaow.close.assert_awaited_once_with(finalize_on_close=True)

    # 3. Catches exceptions and logs a warning
    mock_aaow.reset_mock()
    mock_aaow.bucket_name = "test-bucket"
    mock_aaow.object_name = "test-object"
    mock_aaow.close.side_effect = Exception("Close failed")

    with caplog.at_level(logging.WARNING, logger="gcsfs"):
        await zb_hns_utils.close_aaow(mock_aaow, finalize_on_close=False)

    mock_aaow.close.assert_awaited_once_with(finalize_on_close=False)
    assert (
        "Error closing AsyncAppendableObjectWriter for test-bucket/test-object: Close failed"
        in caplog.text
    )


@pytest.mark.asyncio
async def test_close_mrd(caplog):
    """Tests all graceful closing scenarios for AsyncMultiRangeDownloader."""
    # 1. Handles None gracefully
    await zb_hns_utils.close_mrd(None)

    # 2. Closes successfully
    mock_mrd = mock.AsyncMock()
    await zb_hns_utils.close_mrd(mock_mrd)
    mock_mrd.close.assert_awaited_once()

    # 3. Catches exceptions and logs a warning
    mock_mrd.reset_mock()
    mock_mrd.bucket_name = "test-bucket"
    mock_mrd.object_name = "test-object"
    mock_mrd.close.side_effect = Exception("Close failed")

    with caplog.at_level(logging.WARNING, logger="gcsfs"):
        await zb_hns_utils.close_mrd(mock_mrd)

    mock_mrd.close.assert_awaited_once()
    assert (
        "Error closing AsyncMultiRangeDownloader for test-bucket/test-object: Close failed"
        in caplog.text
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "ranges, expected_call_count",
    [
        ([(0, 5), (10, 3)], 1),  # Basic case
        ([(0, 4), (5, 0), (10, 3)], 1),  # Mixed empty (should filter middle)
        ([(0, 0), (10, 0)], 0),  # All empty (should not call MRD)
        ([], 0),  # Empty list
    ],
    ids=["basic", "mixed_empty", "all_empty", "empty_list"],
)
async def test_download_ranges_unified(ranges, expected_call_count):
    """Unified test for download_ranges success scenarios."""
    mock_mrd = mock.AsyncMock()

    # Writes distinct data like b"0-5" to verify mapping
    async def side_effect(req_ranges):
        for offset, length, buf in req_ranges:
            buf.write(f"{offset}-{length}".encode())

    mock_mrd.download_ranges.side_effect = side_effect

    # Execute
    results = await zb_hns_utils.download_ranges(ranges, mock_mrd)

    # 1. Verify Results
    # Expect empty bytes for 0-length, otherwise expect encoded "{offset}-{length}"
    expected_results = [f"{off}-{ln}".encode() if ln > 0 else b"" for off, ln in ranges]
    assert results == expected_results

    # 2. Verify MRD Interaction
    assert mock_mrd.download_ranges.call_count == expected_call_count

    if expected_call_count > 0:
        # Verify it only received non-zero length ranges
        actual_args = mock_mrd.download_ranges.call_args[0][0]
        non_empty_ranges = [r for r in ranges if r[1] > 0]

        assert len(actual_args) == len(non_empty_ranges)
        for (act_off, act_len, act_buf), (exp_off, exp_len) in zip(
            actual_args, non_empty_ranges
        ):
            assert act_off == exp_off
            assert act_len == exp_len
            assert hasattr(act_buf, "write")


@pytest.mark.asyncio
async def test_download_ranges_exception():
    """Test exception propagation (Keep separate as it changes control flow)."""
    mock_mrd = mock.AsyncMock()
    mock_mrd.download_ranges.side_effect = ValueError("Fail")

    with pytest.raises(ValueError, match="Fail"):
        await zb_hns_utils.download_ranges([(0, 5)], mock_mrd)


@pytest.mark.asyncio
async def test_download_ranges_validation_limit():
    """
    Tests that download_ranges raises a ValueError if the number of ranges
    exceeds 1000.
    """
    mock_mrd = mock.AsyncMock()
    ranges = [(i, 10) for i in range(1001)]

    with pytest.raises(
        ValueError,
        match="Invalid input - number of ranges cannot be more than 1000",
    ):
        await zb_hns_utils.download_ranges(ranges, mock_mrd)


@pytest.mark.asyncio
async def test_mrd_pool_close():
    gcsfs_mock = mock.Mock()
    gcsfs_mock._get_grpc_client = mock.AsyncMock()

    mrd_instance_mock = mock.AsyncMock()

    with mock.patch(
        "google.cloud.storage.asyncio.async_multi_range_downloader.AsyncMultiRangeDownloader.create_mrd",
        return_value=mrd_instance_mock,
    ):
        pool = MRDPool(gcsfs_mock, "bucket", "obj", "123", pool_size=1)
        await pool.initialize()

        await pool.close()
        mrd_instance_mock.close.assert_awaited_once()
        assert len(pool._all_mrds) == 0


@pytest.fixture
def mock_gcsfs():
    gcsfs_mock = mock.Mock()
    gcsfs_mock._get_grpc_client = mock.AsyncMock()
    return gcsfs_mock


@pytest.mark.asyncio
@mock.patch(
    "google.cloud.storage.asyncio.async_multi_range_downloader.AsyncMultiRangeDownloader.create_mrd",
    new_callable=mock.AsyncMock,
)
async def test_mrd_pool_scaling(create_mrd_mock, mock_gcsfs):
    def mock_mrd_factory(*args, **kwargs):
        m = mock.AsyncMock()
        m.persisted_size = 1024
        return m

    create_mrd_mock.side_effect = mock_mrd_factory

    pool = MRDPool(mock_gcsfs, "bucket", "obj", "123", pool_size=2)

    await pool.initialize()
    assert pool.persisted_size == 1024
    assert pool._active_count == 1
    create_mrd_mock.assert_awaited_once()

    async with pool.get_mrd() as mrd1:
        # Since mrd1 is in use, getting another one should spawn a new MRD
        async with pool.get_mrd() as mrd2:
            assert pool._active_count == 2
            assert create_mrd_mock.call_count == 2
            assert mrd1 is not mrd2

    # Both should have been returned to the free queue
    assert pool._free_mrds.qsize() == 2


@pytest.mark.asyncio
@mock.patch(
    "google.cloud.storage.asyncio.async_multi_range_downloader.AsyncMultiRangeDownloader.create_mrd",
    new_callable=mock.AsyncMock,
)
async def test_mrd_pool_double_initialize(create_mrd_mock, mock_gcsfs):
    pool = MRDPool(mock_gcsfs, "bucket", "obj", "123", pool_size=2)

    await pool.initialize()
    await pool.initialize()  # Second call should be a no-op

    assert pool._active_count == 1
    create_mrd_mock.assert_awaited_once()


@pytest.mark.asyncio
async def test_mrd_pool_initialize_after_close(mock_gcsfs):
    pool = MRDPool(mock_gcsfs, "bucket", "obj", "123", pool_size=1)
    await pool.close()

    with pytest.raises(RuntimeError, match="Cannot initialize a closed MRDPool"):
        await pool.initialize()


@pytest.mark.asyncio
@mock.patch(
    "google.cloud.storage.asyncio.async_multi_range_downloader.AsyncMultiRangeDownloader.create_mrd",
    new_callable=mock.AsyncMock,
)
async def test_mrd_pool_get_mrd_creation_error(create_mrd_mock, mock_gcsfs):
    # First creation succeeds during initialization
    valid_mrd = mock.AsyncMock()

    # Second creation fails when pool tries to scale
    create_mrd_mock.side_effect = [valid_mrd, Exception("Network Error")]

    pool = MRDPool(mock_gcsfs, "bucket", "obj", "123", pool_size=2)
    await pool.initialize()

    # Consume the initialized MRD
    async def consume_and_error():
        async with pool.get_mrd() as _:
            # Try to get a second one, which forces a spawn that will fail
            with pytest.raises(Exception, match="Network Error"):
                async with pool.get_mrd() as _:
                    pass

    await consume_and_error()

    # Active count should remain 1 because the second creation failed and rolled back
    assert pool._active_count == 1


@pytest.mark.asyncio
@mock.patch(
    "google.cloud.storage.asyncio.async_multi_range_downloader.AsyncMultiRangeDownloader.create_mrd",
    new_callable=mock.AsyncMock,
)
async def test_mrd_pool_close_with_exceptions(create_mrd_mock, mock_gcsfs):
    bad_mrd_instance = mock.AsyncMock()
    bad_mrd_instance.close.side_effect = RuntimeError("Close failed")
    create_mrd_mock.return_value = bad_mrd_instance

    pool = MRDPool(mock_gcsfs, "bucket", "obj", "123", pool_size=1)
    await pool.initialize()

    with pytest.raises(RuntimeError, match="Close failed"):
        await pool.close()

    bad_mrd_instance.close.assert_awaited_once()
    assert len(pool._all_mrds) == 0


@pytest.mark.asyncio
async def test_mrd_pool_queue_filled_during_lock_wait(mock_gcsfs):
    pool = MRDPool(mock_gcsfs, "bucket", "obj", "123", pool_size=1)
    mrd_mock = mock.AsyncMock()

    # Simulate _create_mrd so we correctly populate _all_mrds
    async def fake_create_mrd():
        return mrd_mock

    with mock.patch.object(pool, "_create_mrd", side_effect=fake_create_mrd):
        await pool.initialize()

        side_effects = [True] + [False] * 10
        with mock.patch.object(pool._free_mrds, "empty", side_effect=side_effects):
            async with pool.get_mrd() as mrd:
                assert mrd == mrd_mock

        # We should not have spawned a new MRD
        assert pool._active_count == 1


@pytest.mark.asyncio
async def test_mrd_pool_round_robin_multi_request(mock_gcsfs):
    pool = MRDPool(mock_gcsfs, "bucket", "obj", "123", pool_size=2)
    mrd1 = mock.AsyncMock()
    mrd2 = mock.AsyncMock()

    mrd_mocks = [mrd1, mrd2]

    # Ensure our mock actually appends to _all_mrds so the round-robin
    # logic sees that there are available active MRDs to share.
    async def fake_create_mrd():
        mrd = mrd_mocks.pop(0)
        return mrd

    # Enable the multi-request feature manually for this test
    pool.mrd_supports_multi_request = True

    with mock.patch.object(pool, "_create_mrd", side_effect=fake_create_mrd):
        await pool.initialize()

        # Keep both MRDs checked out to force the pool to its maximum size
        # and keep the free queue empty.
        async with pool.get_mrd() as active_mrd1:
            async with pool.get_mrd() as active_mrd2:
                assert active_mrd1 == mrd1
                assert active_mrd2 == mrd2
                assert pool._free_mrds.empty()
                assert pool._active_count == 2
                assert pool._rr_index == 0

                # Requesting a 3rd MRD should trigger the round-robin logic
                async with pool.get_mrd() as shared_mrd1:
                    assert shared_mrd1 == mrd1
                    assert pool._rr_index == 1

                # Requesting a 4th MRD should continue the round-robin
                async with pool.get_mrd() as shared_mrd2:
                    assert shared_mrd2 == mrd2
                    assert pool._rr_index == 0

                # Requesting a 5th MRD should wrap around back to the first
                async with pool.get_mrd() as shared_mrd3:
                    assert shared_mrd3 == mrd1
                    assert pool._rr_index == 1


@mock.patch("gcsfs.zb_hns_utils.ctypes.memmove")
def test_direct_memmove_buffer_error_handling(mock_memmove):
    # Use a size > 128KB to trigger the executor background path
    size = 130 * 1024 + 10
    data1 = b"a" * (130 * 1024)
    data2 = b"b" * 10

    # Simulate an access violation or similar error during memory copy
    mock_memmove.side_effect = MemoryError("Segfault simulated")

    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    buf = DirectMemmoveBuffer(size, executor, max_pending=2)
    view = buf.get_view(0, size)

    # First write triggers the background error (slow path)
    future = view.write(data1)

    # Wait for the background thread to actually fail
    with pytest.raises(MemoryError):
        future.result()

    # Subsequent writes should raise the stored error immediately
    with pytest.raises(MemoryError, match="Segfault simulated"):
        view.write(data2)

    # Close should also raise the stored error.
    with pytest.raises(MemoryError, match="Segfault simulated"):
        buf.close()

    executor.shutdown()


def test_direct_memmove_buffer():
    data1 = b"hello"
    data2 = b"world"
    size = len(data1) + len(data2)

    executor = concurrent.futures.ThreadPoolExecutor(max_workers=2)
    buf = DirectMemmoveBuffer(size, executor, max_pending=2)
    view = buf.get_view(0, size)

    future1 = view.write(data1)
    future2 = view.write(data2)

    future1.result()
    future2.result()

    view.close()
    buf.close()

    result_bytes = buf.get_value()
    assert result_bytes == b"helloworld"

    executor.shutdown()


def test_direct_memmove_buffer_overflow():
    """Tests that writing past the view boundaries raises a BufferError."""
    size = 10
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    buf = DirectMemmoveBuffer(size, executor, max_pending=2)
    view = buf.get_view(0, size)

    # Fill the buffer exactly to capacity
    view.write(b"1234567890")

    # Attempting to write even 1 more byte should trigger the overflow protection
    with pytest.raises(BufferError, match="Attempted to write"):
        view.write(b"1")

    view.close()
    buf.close()
    executor.shutdown()


def test_direct_memmove_buffer_underflow():
    """Tests that closing an incompletely filled view/buffer raises a BufferError."""
    size = 10
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    buf = DirectMemmoveBuffer(size, executor, max_pending=2)
    view = buf.get_view(0, size)

    # Write fewer bytes than the expected capacity
    view.write(b"12345")

    # Closing the view should detect that current_offset (5) < expected size (10)
    with pytest.raises(BufferError, match="Buffer contains uninitialized data"):
        view.close()

    # Calling get_value after an incompletely filled buffer should also error
    buf.close()
    with pytest.raises(BufferError, match="Buffer incomplete"):
        buf.get_value()

    executor.shutdown()


@mock.patch("gcsfs.zb_hns_utils.ctypes.memmove")
def test_direct_memmove_buffer_submit_failure(mock_memmove):
    """
    Tests that if executor.submit fails synchronously (e.g., executor is closed),
    the internal locks, semaphores, and events are properly reset, and close()
    does not hang.
    """
    # 1. Chunk > 128KB to force executor scheduling (skip the synchronous fast path)
    chunk_size = 130 * 1024

    # 2. Expected size > chunk_size to skip the Zero-Copy optimization
    expected_size = 140 * 1024

    data = b"a" * chunk_size

    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    buf = DirectMemmoveBuffer(expected_size, executor, max_pending=2)
    view = buf.get_view(0, expected_size)

    # Mock the submit method to simulate a closed executor throwing a RuntimeError
    with mock.patch.object(
        executor, "submit", side_effect=RuntimeError("Executor closed")
    ):
        # The write operation should raise the simulated RuntimeError
        with pytest.raises(RuntimeError, match="Executor closed"):
            view.write(data)

    # Verify that the internal tracking state was correctly rolled back
    assert buf._pending_count == 0
    assert buf._done_event.is_set()

    # Calling close() should NOT hang. It should immediately raise the stored error.
    with pytest.raises(RuntimeError, match="Executor closed"):
        buf.close()

    executor.shutdown()


def test_direct_memmove_buffer_zero_copy():
    """Tests that a perfect aligned single payload avoids memory allocation completely."""
    data = b"exact_size_payload"
    size = len(data)

    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    buf = DirectMemmoveBuffer(size, executor, max_pending=2)
    view = buf.get_view(0, size)

    # Writing a single payload identical to the expected size
    future = view.write(data)
    future.result()

    view.close()
    buf.close()

    # Should be the EXACT same string object returned without copying
    result = buf.get_value()
    assert result is data

    executor.shutdown()


def test_direct_memmove_buffer_overlapping_views():
    """Tests that getting overlapping views raises a ValueError."""
    size = 100
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    buf = DirectMemmoveBuffer(size, executor, max_pending=2)

    # Get a view for the first half
    _ = buf.get_view(0, 50)

    # Attempting to get an overlapping view should fail
    with pytest.raises(ValueError, match="Overlapping view requested"):
        _ = buf.get_view(25, 50)

    # Getting a view for the second half should succeed
    _ = buf.get_view(50, 50)

    buf.close()
    executor.shutdown()


@pytest.mark.asyncio
async def test_close_mrds():
    mrd1 = mock.AsyncMock()
    mrd2 = mock.AsyncMock()
    mrds = [mrd1, mrd2]

    await _close_mrds(mrds)

    mrd1.close.assert_awaited_once()
    mrd2.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_close_mrds_empty():
    await _close_mrds([])  # must not raise


@pytest.mark.asyncio
async def test_close_mrds_propagates_exception():
    bad_mrd = mock.AsyncMock()
    bad_mrd.close.side_effect = RuntimeError("boom")
    good_mrd = mock.AsyncMock()
    mrds = [bad_mrd, good_mrd]

    with pytest.raises(RuntimeError, match="boom"):
        await _close_mrds(mrds, raise_exception=True)

    good_mrd.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_close_mrds_logs_warning(caplog):
    bad_mrd = mock.AsyncMock()
    bad_mrd.close.side_effect = RuntimeError("boom")
    mrds = [bad_mrd]

    with caplog.at_level(logging.WARNING, logger="gcsfs"):
        await _close_mrds(mrds)

    assert "Error closing MRD: boom" in caplog.text


@pytest.fixture
def mock_cache():
    """A mock cache with an internal idle MRD queue."""
    queue = collections.deque()
    cache = mock.AsyncMock()
    cache.get_idle_mrd = mock.Mock(
        side_effect=lambda key: (queue.popleft() if queue else None)
    )
    cache.queue = queue
    return cache


@pytest.mark.asyncio
async def test_mrd_pool_get_mrd_from_local_free_queue(mock_cache):
    mock_mrd = mock.AsyncMock()
    mock_cache.queue.append(mock_mrd)

    mrd_pool = MRDPool(
        mock.Mock(), "bucket", "obj", "123", pool_size=2, cache=mock_cache
    )
    async with mrd_pool.get_mrd() as mrd:
        assert mrd is mock_mrd
        assert mrd_pool._active_count == 1
        assert mrd_pool._all_mrds == [mock_mrd]
        # The MRD has left the queue's free queue while in use
        assert len(mock_cache.queue) == 0
    # After release it stays in the mrd_pool's local free queue, not the shared queue
    assert mrd_pool._free_mrds.qsize() == 1
    assert len(mock_cache.queue) == 0


@pytest.mark.asyncio
@mock.patch("gcsfs.zb_hns_utils.init_mrd", new_callable=mock.AsyncMock)
async def test_mrd_pool_get_mrd_creates_when_empty(
    init_mrd_mock, mock_cache, mock_gcsfs
):
    new_mrd = mock.AsyncMock()
    init_mrd_mock.return_value = new_mrd

    mrd_pool = MRDPool(
        mock_gcsfs, "bucket", "obj", "123", pool_size=2, cache=mock_cache
    )
    async with mrd_pool.get_mrd() as mrd:
        assert mrd is new_mrd

    init_mrd_mock.assert_awaited_once()
    assert mrd_pool._all_mrds == [new_mrd]
    assert mrd_pool._active_count == 1


@pytest.mark.asyncio
@mock.patch("gcsfs.zb_hns_utils.init_mrd", new_callable=mock.AsyncMock)
async def test_mrd_pool_pool_size_cap(init_mrd_mock, mock_cache, mock_gcsfs):
    init_mrd_mock.side_effect = lambda *a, **kw: mock.AsyncMock()

    mrd_pool = MRDPool(
        mock_gcsfs, "bucket", "obj", "123", pool_size=2, cache=mock_cache
    )
    # Hold two slots open concurrently
    async with mrd_pool.get_mrd(), mrd_pool.get_mrd():
        assert mrd_pool._active_count == 2

        # A third concurrent get_mrd must wait, not create a third MRD
        async def third():
            async with mrd_pool.get_mrd():
                pass

        third_task = asyncio.create_task(third())
        await asyncio.sleep(0)  # let third start and block on _free_mrds.get()
        assert init_mrd_mock.await_count == 2  # never grew past 2
        assert not third_task.done()

    await asyncio.wait_for(third_task, timeout=1.0)
    assert init_mrd_mock.await_count == 2  # third reused; no new creation


@pytest.mark.asyncio
@mock.patch("gcsfs.zb_hns_utils.init_mrd", new_callable=mock.AsyncMock)
async def test_mrd_pool_create_failure_decrements_active(
    init_mrd_mock, mock_cache, mock_gcsfs
):
    init_mrd_mock.side_effect = RuntimeError("init failed")
    mrd_pool = MRDPool(
        mock_gcsfs, "bucket", "obj", "123", pool_size=2, cache=mock_cache
    )

    with pytest.raises(RuntimeError, match="init failed"):
        async with mrd_pool.get_mrd():
            pass

    assert mrd_pool._active_count == 0
    assert mrd_pool._all_mrds == []


@pytest.mark.asyncio
async def test_mrd_pool_close_donates_and_repools(mock_cache):
    captured = []

    async def mock_release(key, mrds):
        captured.append((key, list(mrds)))
        for mrd in mrds:
            mock_cache.queue.append(mrd)

    mock_cache.release = mock.AsyncMock(side_effect=mock_release)

    mock_mrd_a = mock.AsyncMock()
    mock_mrd_b = mock.AsyncMock()
    mrd_pool = MRDPool(
        mock.Mock(), "bucket", "obj", "123", pool_size=2, cache=mock_cache
    )
    mrd_pool._free_mrds.put_nowait(mock_mrd_a)
    mrd_pool._free_mrds.put_nowait(mock_mrd_b)
    mrd_pool._all_mrds.extend([mock_mrd_a, mock_mrd_b])
    mrd_pool._initialized = True

    await mrd_pool.close()

    assert mrd_pool._closed is True
    assert mrd_pool._free_mrds.qsize() == 0
    assert len(mock_cache.queue) == 2
    assert captured == [(mrd_pool._key, [mock_mrd_a, mock_mrd_b])]


@pytest.mark.asyncio
async def test_mrd_pool_close_idempotent(mock_cache):
    captured = []

    async def mock_release(key, mrds):
        captured.append((key, mrds))

    mock_cache.release = mock.AsyncMock(side_effect=mock_release)

    mrd_pool = MRDPool(
        mock.Mock(), "bucket", "obj", "123", pool_size=2, cache=mock_cache
    )
    await mrd_pool.close()
    await mrd_pool.close()  # second call is a no-op

    assert captured == [(mrd_pool._key, [])]


@pytest.mark.asyncio
async def test_mrd_pool_get_mrd_after_close_raises(mock_cache):
    mock_cache.release = mock.AsyncMock()

    mrd_pool = MRDPool(
        mock.Mock(), "bucket", "obj", "123", pool_size=2, cache=mock_cache
    )
    await mrd_pool.close()

    with pytest.raises(RuntimeError, match="MRDPool is closed"):
        async with mrd_pool.get_mrd():
            pass


@pytest.mark.asyncio
async def test_mrd_pool_close_concurrently(mock_cache):
    pool = MRDPool(mock.Mock(), "bucket", "obj", "123", pool_size=2, cache=mock_cache)

    # Mock release to take some time to yield control
    async def slow_release(key, mrds):
        await asyncio.sleep(0.1)

    mock_cache.release.side_effect = slow_release

    # Call close concurrently
    tasks = [asyncio.create_task(pool.close()) for _ in range(5)]
    await asyncio.gather(*tasks)

    assert mock_cache.release.call_count == 1


@pytest.mark.asyncio
async def test_mrd_pool_close_exception_handling(mock_cache):
    pool = MRDPool(mock.Mock(), "bucket", "obj", "123", pool_size=2, cache=mock_cache)
    mrd = mock.AsyncMock()
    pool._all_mrds.append(mrd)
    pool._free_mrds.put_nowait(mrd)
    pool._initialized = True

    mock_cache.release.side_effect = RuntimeError("release failed")

    with pytest.raises(RuntimeError, match="release failed"):
        await pool.close()

    assert pool._closed is True
    assert len(pool._all_mrds) == 0


@pytest.mark.asyncio
@mock.patch("gcsfs.zb_hns_utils.init_mrd", new_callable=mock.AsyncMock)
async def test_mrd_pool_cache_get_creates_mrd_queue(init_mrd_mock, mock_gcsfs):
    mock_mrd = mock.AsyncMock()
    mock_mrd.persisted_size = 8
    init_mrd_mock.return_value = mock_mrd

    cache = MRDPoolCache(mock_gcsfs, max_idle_pools=8)
    mrd_pool = await cache.get("bucket", "obj", "123", pool_size=2)

    assert mrd_pool.persisted_size == 8
    assert mrd_pool.pool_size == 2
    assert mrd_pool._cache is cache
    assert cache._refcounts[("bucket", "obj", "123")] == 1


@pytest.mark.asyncio
@mock.patch("gcsfs.zb_hns_utils.init_mrd", new_callable=mock.AsyncMock)
async def test_mrd_pool_cache_get_shares_mrd_queue(init_mrd_mock, mock_gcsfs):
    init_mrd_mock.return_value = mock.AsyncMock(persisted_size=0)

    cache = MRDPoolCache(mock_gcsfs, max_idle_pools=8)
    a = await cache.get("bucket", "obj", "123", pool_size=2)
    a_queue = a._cache._mrd_queues[a._key]
    b = await cache.get("bucket", "obj", "123", pool_size=4)

    assert a_queue is b._cache._mrd_queues[b._key]
    assert cache._refcounts[("bucket", "obj", "123")] == 2
    assert init_mrd_mock.await_count == 2


@pytest.mark.asyncio
@mock.patch("gcsfs.zb_hns_utils.init_mrd", new_callable=mock.AsyncMock)
async def test_mrd_pool_cache_get_distinct_keys(init_mrd_mock, mock_gcsfs):
    init_mrd_mock.return_value = mock.AsyncMock(persisted_size=0)
    cache = MRDPoolCache(mock_gcsfs, max_idle_pools=8)

    await cache.get("bucket", "obj-a", "1", pool_size=1)
    await cache.get("bucket", "obj-b", "1", pool_size=1)

    assert len(cache._mrd_queues) == 2
    assert init_mrd_mock.await_count == 2


@pytest.mark.asyncio
@mock.patch("gcsfs.zb_hns_utils.init_mrd", new_callable=mock.AsyncMock)
async def test_mrd_pool_cache_get_init_failure_drops_entry(init_mrd_mock, mock_gcsfs):
    init_mrd_mock.side_effect = RuntimeError("init boom")
    cache = MRDPoolCache(mock_gcsfs, max_idle_pools=8)

    with pytest.raises(RuntimeError, match="init boom"):
        await cache.get("bucket", "obj", "1", pool_size=1)

    assert ("bucket", "obj", "1") not in cache._mrd_queues

    # A retry succeeds
    init_mrd_mock.side_effect = None
    init_mrd_mock.return_value = mock.AsyncMock(persisted_size=0)
    await cache.get("bucket", "obj", "1", pool_size=1)
    assert cache._refcounts[("bucket", "obj", "1")] == 1


@pytest.mark.asyncio
@mock.patch("gcsfs.zb_hns_utils.init_mrd", new_callable=mock.AsyncMock)
async def test_mrd_pool_cache_get_init_failure_with_max_idle_zero(
    init_mrd_mock, mock_gcsfs
):
    init_mrd_mock.side_effect = RuntimeError("init boom")
    cache = MRDPoolCache(mock_gcsfs, max_idle_pools=0)

    with pytest.raises(RuntimeError, match="init boom"):
        await cache.get("bucket", "obj", "1", pool_size=1)

    assert ("bucket", "obj", "1") not in cache._mrd_queues


@pytest.mark.asyncio
@mock.patch("gcsfs.zb_hns_utils.init_mrd", new_callable=mock.AsyncMock)
async def test_mrd_pool_cache_release_refcount(init_mrd_mock, mock_gcsfs):
    init_mrd_mock.return_value = mock.AsyncMock(persisted_size=0)
    cache = MRDPoolCache(mock_gcsfs, max_idle_pools=8)

    a = await cache.get("bucket", "obj", "1", pool_size=1)
    b = await cache.get("bucket", "obj", "1", pool_size=1)

    await a.close()
    assert cache._refcounts[("bucket", "obj", "1")] == 1
    assert ("bucket", "obj", "1") not in cache._evictable_keys

    await b.close()
    assert ("bucket", "obj", "1") not in cache._refcounts
    assert ("bucket", "obj", "1") in cache._evictable_keys


@pytest.mark.asyncio
@mock.patch("gcsfs.zb_hns_utils.init_mrd", new_callable=mock.AsyncMock)
async def test_mrd_pool_cache_lru_eviction(init_mrd_mock, mock_gcsfs):
    mock_mrds = []

    async def mock_create_mrd(*_a, **_kw):
        m = mock.AsyncMock(persisted_size=0)
        mock_mrds.append(m)
        return m

    init_mrd_mock.side_effect = mock_create_mrd

    cache = MRDPoolCache(mock_gcsfs, max_idle_pools=2)

    # Open + close 3 distinct objects sequentially
    mrd_pools = []
    for i in range(3):
        mrd_pools.append(await cache.get("bucket", f"obj-{i}", "1", pool_size=1))
    for mrd_pool in mrd_pools:
        await mrd_pool.close()

    # Only the most recent 2 should remain
    assert ("bucket", "obj-0", "1") not in cache._mrd_queues
    assert ("bucket", "obj-1", "1") in cache._mrd_queues
    assert ("bucket", "obj-2", "1") in cache._mrd_queues
    assert list(cache._evictable_keys.keys()) == [
        ("bucket", "obj-1", "1"),
        ("bucket", "obj-2", "1"),
    ]
    # The evicted MRD queue's MRDs were torn down
    mock_mrds[0].close.assert_awaited_once()
    mock_mrds[1].close.assert_not_awaited()
    mock_mrds[2].close.assert_not_awaited()


@pytest.mark.asyncio
@mock.patch("gcsfs.zb_hns_utils.init_mrd", new_callable=mock.AsyncMock)
async def test_mrd_pool_cache_pinned_never_evicted(init_mrd_mock, mock_gcsfs):
    init_mrd_mock.side_effect = lambda *a, **kw: mock.AsyncMock(persisted_size=0)

    cache = MRDPoolCache(mock_gcsfs, max_idle_pools=0)
    mrd_pools = [
        await cache.get("bucket", f"obj-{i}", "1", pool_size=1) for i in range(3)
    ]

    # All 3 still resident even with max_idle=0 since they're in use
    assert len(cache._mrd_queues) == 3
    assert len(cache._evictable_keys) == 0

    for mrd_pool in mrd_pools:
        await mrd_pool.close()

    # Now they should all be evicted (each release pushes idle past cap=0)
    assert cache._mrd_queues == {}


@pytest.mark.asyncio
@mock.patch("gcsfs.zb_hns_utils.init_mrd", new_callable=mock.AsyncMock)
async def test_mrd_pool_cache_release_reuses_mrd_on_get(init_mrd_mock, mock_gcsfs):
    mock_mrds = []

    async def mock_create_mrd(*_a, **_kw):
        m = mock.AsyncMock(persisted_size=0)
        mock_mrds.append(m)
        return m

    init_mrd_mock.side_effect = mock_create_mrd

    cache = MRDPoolCache(mock_gcsfs, max_idle_pools=4)
    a = await cache.get("bucket", "obj", "1", pool_size=1)
    a_mrd = a._all_mrds[0]
    await a.close()

    b = await cache.get("bucket", "obj", "1", pool_size=2)

    async with b.get_mrd() as m1:
        assert m1 is mock_mrds[1]  # The one created in b.initialize()

        async with b.get_mrd() as m2:
            assert m2 is a_mrd  # Reused from cache (originally from a)

    assert init_mrd_mock.await_count == 2


@pytest.mark.asyncio
@mock.patch("gcsfs.zb_hns_utils.init_mrd", new_callable=mock.AsyncMock)
async def test_mrd_pool_cache_close_tears_down_all(init_mrd_mock, mock_gcsfs):
    mock_mrds = []

    async def mock_create_mrd(*_a, **_kw):
        m = mock.AsyncMock(persisted_size=0)
        mock_mrds.append(m)
        return m

    init_mrd_mock.side_effect = mock_create_mrd

    cache = MRDPoolCache(mock_gcsfs, max_idle_pools=8)
    a = await cache.get("bucket", "obj-a", "1", pool_size=1)
    assert a is not None
    mrd_pool_b = await cache.get("bucket", "obj-b", "1", pool_size=1)
    await mrd_pool_b.close()  # one idle, one pinned

    await cache.close()

    assert cache._mrd_queues == {}
    assert cache._evictable_keys == collections.OrderedDict()
    assert cache._closed is True

    await a.close()

    for m in mock_mrds:
        m.close.assert_awaited_once()

    # Subsequent get raises
    with pytest.raises(RuntimeError, match="MRDPoolCache is closed"):
        await cache.get("bucket", "obj-c", "1", pool_size=1)

    # Subsequent close is a no-op
    await cache.close()


@pytest.mark.asyncio
@mock.patch("gcsfs.zb_hns_utils.init_mrd", new_callable=mock.AsyncMock)
async def test_mrd_pool_cache_close_no_op_when_already_closed(
    init_mrd_mock, mock_gcsfs
):
    cache = MRDPoolCache(mock_gcsfs)
    await cache.close()
    await cache.close()  # idempotent
    assert cache._closed is True


@pytest.mark.asyncio
async def test_mrd_pool_cache_get_idle_mrd_closed(mock_gcsfs):
    cache = MRDPoolCache(mock_gcsfs)
    await cache.close()
    assert cache.get_idle_mrd(("bucket", "obj", "1")) is None


def test_mrd_pool_cache_get_idle_mrd_not_found(mock_gcsfs):
    cache = MRDPoolCache(mock_gcsfs)
    assert cache.get_idle_mrd(("bucket", "obj", "1")) is None


@pytest.mark.asyncio
async def test_mrd_pool_cache_get_fs_gc(mock_gcsfs):
    cache = MRDPoolCache(mock_gcsfs)
    cache._gcsfs = lambda: None  # Simulate GC
    with pytest.raises(
        RuntimeError, match="ExtendedGcsFileSystem has been garbage collected"
    ):
        await cache.get("bucket", "obj", "1", pool_size=1)


@pytest.mark.asyncio
@mock.patch("gcsfs.zb_hns_utils.init_mrd", new_callable=mock.AsyncMock)
async def test_mrd_pool_cache_max_queue_size_limit(init_mrd_mock, mock_gcsfs):
    init_mrd_mock.side_effect = lambda *a, **kw: mock.AsyncMock(persisted_size=0)
    cache = MRDPoolCache(mock_gcsfs, max_idle_pools=2, max_queue_size=2)

    # Create pools for same key
    pool_a = await cache.get("bucket", "obj", "1", pool_size=2)
    pool_b = await cache.get("bucket", "obj", "1", pool_size=2)

    # Let's get the MRDs from the pools so they scale up
    async with pool_a.get_mrd() as m1, pool_a.get_mrd() as m2:
        async with pool_b.get_mrd() as m3, pool_b.get_mrd() as m4:
            # We have 4 MRDs checked out across the pools for the same key
            mrd_instances = [m1, m2, m3, m4]

    # Now let's close both pools, which releases their MRDs back to the cache
    await pool_a.close()
    await pool_b.close()

    # The queue size should be exactly max_queue_size (2)
    queue = cache._mrd_queues[("bucket", "obj", "1")]
    assert len(queue) == 2

    # Two of the MRDs should have been closed
    closed_count = sum(1 for mrd in mrd_instances if mrd.close.call_count > 0)
    assert closed_count == 2


@pytest.mark.asyncio
@mock.patch("gcsfs.zb_hns_utils.init_mrd", new_callable=mock.AsyncMock)
async def test_mrd_pool_cache_max_queue_size_zero(init_mrd_mock, mock_gcsfs):
    init_mrd_mock.return_value = mock.AsyncMock(persisted_size=0)
    cache = MRDPoolCache(mock_gcsfs, max_idle_pools=2, max_queue_size=0)

    pool = await cache.get("bucket", "obj", "1", pool_size=2)
    async with pool.get_mrd() as m1:
        mrd_instance = m1

    await pool.close()

    queue = cache._mrd_queues[("bucket", "obj", "1")]
    assert len(queue) == 0
    mrd_instance.close.assert_awaited_once()


def test_direct_memmove_buffer_zero_byte_write_after_zero_copy():
    """
    Tests that a zero-byte write (like a gRPC range completion chunk)
    does not crash the buffer, especially after the zero-copy fast path
    has left self._start_address as None.
    """
    data = b"exact_size_payload"
    size = len(data)

    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    buf = DirectMemmoveBuffer(size, executor, max_pending=2)
    view = buf.get_view(0, size)

    # 1. Trigger the zero-copy fast path
    future1 = view.write(data)
    future1.result()

    # 2. Trigger the empty chunk write
    # This should return a completed future and NOT raise a BufferError
    future2 = view.write(b"")
    future2.result()

    view.close()
    buf.close()

    # Verify the payload was still handled via zero-copy successfully
    result = buf.get_value()
    assert result is data

    executor.shutdown()


def test_direct_memmove_buffer_zero_byte_write_closed_state():
    """
    Tests that zero-byte writes still respect the closed state of the buffer.
    """
    size = 10
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    buf = DirectMemmoveBuffer(size, executor, max_pending=2)
    view = buf.get_view(0, size)

    # Force the buffer closed
    buf.close()

    # Even a zero-byte write should fail if the buffer is no longer accepting I/O
    with pytest.raises(ValueError, match="I/O operation on closed buffer."):
        view.write(b"")

    executor.shutdown()


def test_direct_memmove_buffer_zero_byte_write_error_state():
    """
    Tests that zero-byte writes still raise background errors if the buffer
    is in a failed state.
    """
    size = 10
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    buf = DirectMemmoveBuffer(size, executor, max_pending=2)
    view = buf.get_view(0, size)

    # Manually inject a background error
    buf._error = RuntimeError("Simulated background failure")

    # A zero-byte write should surface the pending error
    with pytest.raises(RuntimeError, match="Simulated background failure"):
        view.write(b"")

    # Clean up the error so we can safely close the test
    buf._error = None
    buf._stop_accepting_writes = True
    executor.shutdown()
