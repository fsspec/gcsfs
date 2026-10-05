import asyncio
import collections
import concurrent.futures
import logging
from unittest import mock

import pytest
from google.api_core.exceptions import NotFound

from gcsfs import zb_hns_utils
from gcsfs.zb_hns_utils import DirectMemmoveBuffer, MRDCache, _close_mrds

mock_grpc_client = mock.Mock()
bucket_name = "test-bucket"
object_name = "test-object"
generation = "12345"


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
async def test_init_mrd_with_cache_type():
    """Tests that init_mrd passes cache_type as metadata."""
    mock_mrd_instance = mock.Mock()
    with mock.patch(
        "gcsfs.zb_hns_utils.AsyncMultiRangeDownloader.create_mrd",
        new_callable=mock.AsyncMock,
        return_value=mock_mrd_instance,
    ) as mock_create_mrd:
        result = await zb_hns_utils.init_mrd(
            mock_grpc_client,
            bucket_name,
            object_name,
            generation,
            cache_type="readahead",
            cache_source="explicit",
        )

        mock_create_mrd.assert_awaited_once_with(
            mock_grpc_client,
            bucket_name,
            object_name,
            generation,
            metadata=[("x-goog-api-client", "cache_type/readahead:e")],
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


@pytest.fixture
def mock_gcsfs():
    gcsfs_mock = mock.Mock()
    gcsfs_mock._get_grpc_client = mock.AsyncMock()
    gcsfs_mock._info = mock.AsyncMock(
        return_value={"generation": "123", "timeFinalized": "2026-06-17T00:00:00Z"}
    )
    return gcsfs_mock


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


@pytest.mark.asyncio
@mock.patch("gcsfs.zb_hns_utils.init_mrd", new_callable=mock.AsyncMock)
async def test_mrd_cache_get_creates_mrd(init_mrd_mock, mock_gcsfs):
    mock_mrd = mock.AsyncMock()
    mock_mrd.persisted_size = 8
    init_mrd_mock.return_value = mock_mrd

    cache = MRDCache(mock_gcsfs, max_idle_mrds=8)
    mrd = await cache.get("bucket", "obj", "123", concurrency=2)

    assert mrd.persisted_size == 8
    assert mrd.concurrency == 2
    assert mrd._cache is cache
    assert cache._active[("bucket", "obj", "123")][1] == 1
    init_mrd_mock.assert_awaited_once_with(
        mock_gcsfs.grpc_client,
        "bucket",
        "obj",
        "123",
        cache_type=None,
        cache_source=None,
        concurrency=2,
    )


@pytest.mark.asyncio
@mock.patch("gcsfs.zb_hns_utils.init_mrd", new_callable=mock.AsyncMock)
async def test_mrd_cache_get_cache_type(init_mrd_mock, mock_gcsfs):
    mock_mrd = mock.AsyncMock()
    mock_mrd.persisted_size = 8
    init_mrd_mock.return_value = mock_mrd

    cache = MRDCache(mock_gcsfs, max_idle_mrds=8)
    mrd = await cache.get(
        "bucket",
        "obj",
        "123",
        concurrency=2,
        cache_type="readahead",
        cache_source="explicit",
    )

    assert mrd.cache_type == "readahead"
    assert mrd.cache_source == "explicit"
    init_mrd_mock.assert_awaited_once_with(
        mock_gcsfs.grpc_client,
        "bucket",
        "obj",
        "123",
        concurrency=2,
        cache_type="readahead",
        cache_source="explicit",
    )


@pytest.mark.asyncio
@mock.patch("gcsfs.zb_hns_utils.init_mrd", new_callable=mock.AsyncMock)
async def test_mrd_cache_get_shares_active_mrd(init_mrd_mock, mock_gcsfs):
    init_mrd_mock.return_value = mock.AsyncMock(persisted_size=0)

    cache = MRDCache(mock_gcsfs, max_idle_mrds=8)
    a = await cache.get("bucket", "obj", "123", concurrency=2)
    b = await cache.get("bucket", "obj", "123", concurrency=4)

    assert b is a
    assert a._cache_key == b._cache_key
    assert cache._active[("bucket", "obj", "123")][1] == 2
    assert init_mrd_mock.await_count == 1


@pytest.mark.asyncio
@mock.patch("gcsfs.zb_hns_utils.init_mrd", new_callable=mock.AsyncMock)
async def test_mrd_cache_get_distinct_keys(init_mrd_mock, mock_gcsfs):
    init_mrd_mock.return_value = mock.AsyncMock(persisted_size=0)
    cache = MRDCache(mock_gcsfs, max_idle_mrds=8)

    await cache.get("bucket", "obj-a", "1", concurrency=1)
    await cache.get("bucket", "obj-b", "1", concurrency=1)

    assert len(cache._active) == 2
    assert init_mrd_mock.await_count == 2


@pytest.mark.asyncio
@mock.patch("gcsfs.zb_hns_utils.init_mrd", new_callable=mock.AsyncMock)
async def test_mrd_cache_get_init_failure_drops_entry(init_mrd_mock, mock_gcsfs):
    init_mrd_mock.side_effect = RuntimeError("init boom")
    cache = MRDCache(mock_gcsfs, max_idle_mrds=8)

    with pytest.raises(RuntimeError, match="init boom"):
        await cache.get("bucket", "obj", "1", concurrency=1)

    assert ("bucket", "obj", "1") not in cache._active
    assert ("bucket", "obj", "1") not in cache._inactive

    # A retry succeeds
    init_mrd_mock.side_effect = None
    init_mrd_mock.return_value = mock.AsyncMock(persisted_size=0)
    await cache.get("bucket", "obj", "1", concurrency=1)
    assert cache._active[("bucket", "obj", "1")][1] == 1


@pytest.mark.asyncio
@mock.patch("gcsfs.zb_hns_utils.init_mrd", new_callable=mock.AsyncMock)
async def test_mrd_cache_get_init_failure_with_max_idle_zero(init_mrd_mock, mock_gcsfs):
    init_mrd_mock.side_effect = RuntimeError("init boom")
    cache = MRDCache(mock_gcsfs, max_idle_mrds=0)

    with pytest.raises(RuntimeError, match="init boom"):
        await cache.get("bucket", "obj", "1", concurrency=1)

    assert ("bucket", "obj", "1") not in cache._active
    assert ("bucket", "obj", "1") not in cache._inactive


@pytest.mark.asyncio
@mock.patch("gcsfs.zb_hns_utils.init_mrd", new_callable=mock.AsyncMock)
async def test_mrd_cache_release_refcount(init_mrd_mock, mock_gcsfs):
    init_mrd_mock.side_effect = lambda *a, **kw: mock.AsyncMock(persisted_size=0)
    cache = MRDCache(mock_gcsfs, max_idle_mrds=8)

    a = await cache.get("bucket", "obj", "1", concurrency=1)
    b = await cache.get("bucket", "obj", "1", concurrency=1)

    await a.close()
    assert cache._active[("bucket", "obj", "1")][1] == 1
    assert ("bucket", "obj", "1") not in cache._inactive

    await b.close()
    assert ("bucket", "obj", "1") not in cache._active
    assert ("bucket", "obj", "1") in cache._inactive


@pytest.mark.asyncio
@mock.patch("gcsfs.zb_hns_utils.init_mrd", new_callable=mock.AsyncMock)
async def test_mrd_cache_lru_eviction(init_mrd_mock, mock_gcsfs):
    mock_mrds = []

    async def mock_create_mrd(*_a, **_kw):
        m = mock.AsyncMock(persisted_size=0)
        mock_mrds.append(m)
        return m

    init_mrd_mock.side_effect = mock_create_mrd

    cache = MRDCache(mock_gcsfs, max_idle_mrds=2)

    # Open + close 3 distinct objects sequentially
    mrds = []
    for i in range(3):
        m = await cache.get("bucket", f"obj-{i}", "1", concurrency=1)
        mrds.append(m)
        await m.close()

    # Only the most recent 2 should remain
    assert ("bucket", "obj-0", "1") not in cache._inactive
    assert ("bucket", "obj-1", "1") in cache._inactive
    assert ("bucket", "obj-2", "1") in cache._inactive
    assert list(cache._inactive.keys()) == [
        ("bucket", "obj-1", "1"),
        ("bucket", "obj-2", "1"),
    ]
    # The evicted MRD queue's MRDs were torn down
    mock_mrds[0]._raw_close.assert_awaited_once()
    mock_mrds[1]._raw_close.assert_not_awaited()
    mock_mrds[2]._raw_close.assert_not_awaited()


@pytest.mark.asyncio
@mock.patch("gcsfs.zb_hns_utils.init_mrd", new_callable=mock.AsyncMock)
async def test_mrd_cache_pinned_never_evicted(init_mrd_mock, mock_gcsfs):
    init_mrd_mock.side_effect = lambda *a, **kw: mock.AsyncMock(persisted_size=0)
    cache = MRDCache(mock_gcsfs, max_idle_mrds=1)

    pinned = await cache.get("bucket", "pinned", "1", concurrency=1)

    other = await cache.get("bucket", "other", "1", concurrency=1)
    await other.close()

    assert ("bucket", "pinned", "1") in cache._active
    assert ("bucket", "other", "1") in cache._inactive

    third = await cache.get("bucket", "third", "1", concurrency=1)
    await third.close()

    assert ("bucket", "pinned", "1") in cache._active
    assert ("bucket", "other", "1") not in cache._inactive
    assert ("bucket", "third", "1") in cache._inactive

    await pinned.close()


@pytest.mark.asyncio
@mock.patch("gcsfs.zb_hns_utils.init_mrd", new_callable=mock.AsyncMock)
async def test_mrd_cache_release_reuses_mrd_on_get(init_mrd_mock, mock_gcsfs):
    mock_mrds = []

    async def mock_create_mrd(*_a, **_kw):
        m = mock.AsyncMock(persisted_size=0)
        mock_mrds.append(m)
        return m

    init_mrd_mock.side_effect = mock_create_mrd

    cache = MRDCache(mock_gcsfs, max_idle_mrds=4)
    a = await cache.get("bucket", "obj", "1", concurrency=1)
    await a.close()

    b = await cache.get("bucket", "obj", "1", concurrency=1)
    assert b is a  # Reused from cache
    assert init_mrd_mock.await_count == 1


@pytest.mark.asyncio
@mock.patch("gcsfs.zb_hns_utils.init_mrd", new_callable=mock.AsyncMock)
async def test_mrd_cache_close_tears_down_all(init_mrd_mock, mock_gcsfs):
    mock_mrds = []

    async def mock_create_mrd(*_a, **_kw):
        m = mock.AsyncMock(persisted_size=0)
        mock_mrds.append(m)
        return m

    init_mrd_mock.side_effect = mock_create_mrd

    cache = MRDCache(mock_gcsfs, max_idle_mrds=8)
    a = await cache.get("bucket", "obj-a", "1", concurrency=1)
    assert a is not None
    mrd_b = await cache.get("bucket", "obj-b", "1", concurrency=1)
    await mrd_b.close()  # one idle, one pinned

    await cache.close()

    assert cache._closed is True
    assert cache._active == {}
    assert cache._inactive == collections.OrderedDict()

    await a.close()

    for m in mock_mrds:
        m._raw_close.assert_awaited_once()

    # Subsequent get raises
    with pytest.raises(RuntimeError, match="MRDCache is closed"):
        await cache.get("bucket", "obj-c", "1", concurrency=1)

    # Subsequent close is a no-op
    await cache.close()


@pytest.mark.asyncio
@mock.patch("gcsfs.zb_hns_utils.init_mrd", new_callable=mock.AsyncMock)
async def test_mrd_cache_close_no_op_when_already_closed(init_mrd_mock, mock_gcsfs):
    cache = MRDCache(mock_gcsfs)
    await cache.close()
    await cache.close()  # idempotent
    assert cache._closed is True


@pytest.mark.asyncio
async def test_mrd_cache_get_fs_gc(mock_gcsfs):
    cache = MRDCache(mock_gcsfs)
    cache._gcsfs = lambda: None  # Simulate GC
    with pytest.raises(
        RuntimeError, match="ExtendedGcsFileSystem has been garbage collected"
    ):
        await cache.get("bucket", "obj", "1", concurrency=1)


@pytest.mark.asyncio
@mock.patch("gcsfs.zb_hns_utils.init_mrd", new_callable=mock.AsyncMock)
async def test_mrd_cache_concurrent_readers_share_and_move_to_inactive(
    init_mrd_mock, mock_gcsfs
):
    mock_mrd = mock.AsyncMock(persisted_size=0)
    init_mrd_mock.return_value = mock_mrd
    cache = MRDCache(mock_gcsfs, max_idle_mrds=2)

    # 3 concurrent readers acquire the same key
    m1 = await cache.get("bucket", "obj", "1", concurrency=1)
    m2 = await cache.get("bucket", "obj", "1", concurrency=1)
    m3 = await cache.get("bucket", "obj", "1", concurrency=1)

    assert m1 is m2 is m3
    assert cache._active[("bucket", "obj", "1")][1] == 3
    assert ("bucket", "obj", "1") in cache._active
    assert ("bucket", "obj", "1") not in cache._inactive

    await m1.close()
    assert cache._active[("bucket", "obj", "1")][1] == 2
    assert ("bucket", "obj", "1") in cache._active

    await m2.close()
    assert cache._active[("bucket", "obj", "1")][1] == 1
    assert ("bucket", "obj", "1") in cache._active

    await m3.close()
    assert ("bucket", "obj", "1") not in cache._active
    assert ("bucket", "obj", "1") in cache._inactive
    assert cache._inactive.get(("bucket", "obj", "1")) is m1


@pytest.mark.asyncio
@mock.patch("gcsfs.zb_hns_utils.init_mrd", new_callable=mock.AsyncMock)
async def test_mrd_cache_max_idle_zero_closes_immediately(init_mrd_mock, mock_gcsfs):
    mock_mrd = mock.AsyncMock(persisted_size=0)
    init_mrd_mock.return_value = mock_mrd
    cache = MRDCache(mock_gcsfs, max_idle_mrds=0)

    mrd = await cache.get("bucket", "obj", "1", concurrency=1)
    await mrd.close()

    assert ("bucket", "obj", "1") not in cache._inactive
    mock_mrd._raw_close.assert_awaited_once()


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


@pytest.mark.asyncio
@mock.patch("gcsfs.zb_hns_utils.init_mrd", new_callable=mock.AsyncMock)
async def test_mrd_cache_finalized_reuses_cached_mrd(init_mrd_mock, mock_gcsfs):
    mock_gcsfs._info = mock.AsyncMock(
        return_value={"generation": "123", "timeFinalized": "2026-06-17T00:00:00Z"}
    )
    new_mrd = mock.AsyncMock(persisted_size=100)
    init_mrd_mock.return_value = new_mrd

    cache = MRDCache(mock_gcsfs, max_idle_mrds=4)
    mrd1 = await cache.get("bucket", "obj", "123", concurrency=1)
    assert mrd1 is new_mrd
    assert init_mrd_mock.await_count == 1

    await mrd1.close()  # released to inactive LRU
    assert ("bucket", "obj", "123") in cache._inactive

    mrd2 = await cache.get("bucket", "obj", "123", concurrency=1)
    assert mrd2 is mrd1
    assert init_mrd_mock.await_count == 1  # reused, not re-created


@pytest.mark.asyncio
@mock.patch("gcsfs.zb_hns_utils.init_mrd", new_callable=mock.AsyncMock)
async def test_mrd_cache_unfinalized_does_not_reuse_cached_mrd(
    init_mrd_mock, mock_gcsfs
):
    mock_gcsfs._info = mock.AsyncMock(
        return_value={"generation": "123", "timeFinalized": None}
    )
    mrd_a = mock.AsyncMock(persisted_size=100)
    mrd_b = mock.AsyncMock(persisted_size=200)
    init_mrd_mock.side_effect = [mrd_a, mrd_b]

    cache = MRDCache(mock_gcsfs, max_idle_mrds=4)
    m1 = await cache.get("bucket", "obj", "123", concurrency=1)
    assert m1 is mrd_a

    await m1.close()  # unfinalized MRDs are closed, not cached warm
    assert ("bucket", "obj", "123") not in cache._inactive
    assert ("bucket", "obj", "123") not in cache._active

    m2 = await cache.get("bucket", "obj", "123", concurrency=1)
    assert m2 is mrd_b
    assert init_mrd_mock.await_count == 2


@pytest.mark.asyncio
@mock.patch("gcsfs.zb_hns_utils.init_mrd", new_callable=mock.AsyncMock)
async def test_mrd_cache_concurrent_get_waits_for_first_mrd(init_mrd_mock, mock_gcsfs):
    mock_mrd = mock.AsyncMock(persisted_size=100)
    init_started = asyncio.Event()
    finish_init = asyncio.Event()

    async def slow_init_mrd(*args, **kwargs):
        init_started.set()
        await finish_init.wait()
        return mock_mrd

    init_mrd_mock.side_effect = slow_init_mrd
    cache = MRDCache(mock_gcsfs, max_idle_mrds=4)

    # Coroutine A launches get() and starts slow init
    task_a = asyncio.create_task(cache.get("bucket", "obj", "1", concurrency=1))
    await init_started.wait()

    # While A is creating the first MRD, Coroutines B and C call get() on the same key
    task_b = asyncio.create_task(cache.get("bucket", "obj", "1", concurrency=1))
    task_c = asyncio.create_task(cache.get("bucket", "obj", "1", concurrency=1))

    # Give event loop a cycle; B and C should be waiting on the in-flight creation
    await asyncio.sleep(0.01)
    assert init_mrd_mock.await_count == 1
    assert ("bucket", "obj", "1") in cache._pending

    # Now let the first MRD creation complete
    finish_init.set()
    mrd_a, mrd_b, mrd_c = await asyncio.gather(task_a, task_b, task_c)

    # All three got the exact same MRD instance and shared the active connection
    assert mrd_a is mock_mrd
    assert mrd_b is mock_mrd
    assert mrd_c is mock_mrd
    assert init_mrd_mock.await_count == 1
    assert cache._active[("bucket", "obj", "1")][1] == 3
    assert ("bucket", "obj", "1") not in cache._pending


@pytest.mark.asyncio
@mock.patch("gcsfs.zb_hns_utils.init_mrd", new_callable=mock.AsyncMock)
async def test_mrd_cache_concurrent_get_propagates_init_failure(
    init_mrd_mock, mock_gcsfs
):
    init_started = asyncio.Event()
    fail_init = asyncio.Event()

    async def failing_init_mrd(*args, **kwargs):
        init_started.set()
        await fail_init.wait()
        raise RuntimeError("grpc connection failure")

    init_mrd_mock.side_effect = failing_init_mrd
    cache = MRDCache(mock_gcsfs, max_idle_mrds=4)

    task_a = asyncio.create_task(cache.get("bucket", "obj", "1", concurrency=1))
    await init_started.wait()

    task_b = asyncio.create_task(cache.get("bucket", "obj", "1", concurrency=1))
    await asyncio.sleep(0.01)

    fail_init.set()
    res_a = await asyncio.gather(task_a, return_exceptions=True)
    res_b = await asyncio.gather(task_b, return_exceptions=True)

    assert isinstance(res_a[0], RuntimeError) and "grpc connection failure" in str(
        res_a[0]
    )
    assert isinstance(res_b[0], RuntimeError) and "grpc connection failure" in str(
        res_b[0]
    )
    assert ("bucket", "obj", "1") not in cache._pending
    assert ("bucket", "obj", "1") not in cache._active


@mock.patch("gcsfs.zb_hns_utils.HAS_CPYTHON_API", False)
def test_direct_memmove_buffer_pypy_fallback():
    """
    Tests that when HAS_CPYTHON_API is False (e.g., on PyPy), the buffer correctly
    falls back to using bytearray.
    """
    data1 = b"pypy_"
    data2 = b"fallback"
    size = len(data1) + len(data2)

    executor = concurrent.futures.ThreadPoolExecutor(max_workers=2)
    # Use max_pending=2 and split the write to prevent the zero-copy fast path
    # from skipping the memory allocation phase.
    buf = DirectMemmoveBuffer(size, executor, max_pending=2)
    view = buf.get_view(0, size)

    future1 = view.write(data1)
    future2 = view.write(data2)

    future1.result()
    future2.result()

    view.close()
    buf.close()

    result_bytes = buf.get_value()

    # 1. Verify the data is completely intact
    assert result_bytes == b"pypy_fallback"

    # 2. Verify it is standard Python bytes
    assert isinstance(result_bytes, bytes)

    # 3. Verify the fallback path was ACTUALLY taken by inspecting the internal buffer.
    assert isinstance(buf._result_bytes, bytearray)

    executor.shutdown()


@pytest.mark.asyncio
async def test_mrd_methods_raise_unrelated_type_error():
    """Tests that unrelated TypeErrors are properly re-raised."""
    mock_grpc_client = mock.Mock()

    # Test init_mrd re-raise
    with mock.patch(
        "gcsfs.zb_hns_utils.AsyncMultiRangeDownloader.create_mrd",
        side_effect=TypeError("some other error"),
    ):
        with pytest.raises(TypeError, match="some other error"):
            await zb_hns_utils.init_mrd(
                mock_grpc_client, "b", "o", cache_type="readahead"
            )
