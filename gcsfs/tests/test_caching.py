import asyncio
from unittest import mock

import fsspec.asyn
import pytest

from gcsfs.caching import Prefetcher, ReadAheadChunked


class MockVectorFetcher:
    """Simulates a backend capable of vector reads (accepting chunk_lengths)."""

    def __init__(self, data: bytes):
        self.data = data
        self.call_log = []

    def __call__(self, start, chunk_lengths):
        self.call_log.append({"start": start, "chunk_lengths": chunk_lengths})
        results = []
        current = start
        for length in chunk_lengths:
            end = min(current + length, len(self.data))
            results.append(self.data[current:end])
            current += length
        return results


@pytest.fixture
def source_data():
    """Generates 100 bytes of sequential data."""
    return bytes(range(100))


@pytest.fixture
def cache_setup(source_data):
    """Returns a tuple of (cache_instance, fetcher_mock)."""
    fetcher = MockVectorFetcher(source_data)
    # Blocksize 10, File size 100
    cache = ReadAheadChunked(blocksize=10, fetcher=fetcher, size=100)
    return cache, fetcher


def test_initial_state(cache_setup):
    cache, _ = cache_setup
    assert cache.cache == b""
    assert len(cache.chunks) == 0
    assert cache.hit_count == 0
    assert cache.miss_count == 0


def test_fetch_with_readahead(cache_setup, source_data):
    """Test a basic fetch. Should retrieve requested data + blocksize readahead."""
    cache, fetcher = cache_setup

    # Request bytes 0-5
    result = cache._fetch(0, 5)

    # 1. Verify data correctness
    assert result == source_data[0:5]

    # 2. Verify Fetcher calls
    # Should fetch requested (5) + readahead (10)
    assert len(fetcher.call_log) == 1
    assert fetcher.call_log[0]["start"] == 0
    assert fetcher.call_log[0]["chunk_lengths"] == [5, 10]

    # 3. Verify Internal State (Deque)
    # We expect two chunks: the requested part (0-5) and readahead (5-15)
    assert len(cache.chunks) == 2
    assert cache.chunks[0] == (0, 5, source_data[0:5])
    assert cache.chunks[1] == (5, 15, source_data[5:15])

    # 4. Verify compatibility property
    assert cache.cache == source_data[0:15]


def test_cache_hit_fully_contained(cache_setup, source_data):
    """Test fetching data that is already inside the readahead buffer."""
    cache, fetcher = cache_setup

    # Prime the cache (fetch 0-5, readahead 5-15)
    cache._fetch(0, 5)

    # Reset call log to ensure next fetch doesn't hit backend
    fetcher.call_log = []

    # Request 5-10 (Should be inside the readahead chunk)
    result = cache._fetch(5, 10)

    assert result == source_data[5:10]
    assert len(fetcher.call_log) == 0  # No backend calls
    assert cache.hit_count == 1


def test_cache_hit_spanning_chunks(cache_setup, source_data):
    """Test fetching data that spans across the requested chunk and the readahead chunk."""
    cache, fetcher = cache_setup

    # Prime cache: Chunk 1 (0-5), Chunk 2 (5-15)
    cache._fetch(0, 5)

    # Request 2-8 (Spans Chunk 1 and Chunk 2)
    result = cache._fetch(2, 8)

    assert result == source_data[2:8]
    # Should join parts internally without fetching new data
    assert cache.hit_count == 1
    assert len(fetcher.call_log) == 1  # Only the initial prime call


def test_backward_seek_clears_cache(cache_setup, source_data):
    """Test that seeking backwards (before current window) clears cache and refetches."""
    cache, fetcher = cache_setup

    # Prime cache at 50-60 (Readahead 60-70)
    cache._fetch(50, 60)
    assert cache.chunks[0][0] == 50

    # Seek backwards to 20
    fetcher.call_log = []
    result = cache._fetch(20, 30)

    assert result == source_data[20:30]
    # Cache should have cleared and fetched new
    assert fetcher.call_log[0]["start"] == 20
    assert cache.chunks[0][0] == 20


def test_forward_seek_miss(cache_setup, source_data):
    """Test requesting data far ahead of the current window."""
    cache, fetcher = cache_setup

    # Prime 0-5
    cache._fetch(0, 5)

    # Jump to 50
    fetcher.call_log = []
    result = cache._fetch(50, 55)

    assert result == source_data[50:55]
    # Should clear old chunks and fetch new
    assert len(cache.chunks) == 2  # 50-55 and readahead
    assert cache.chunks[0][0] == 50


def test_zero_copy_optimization(cache_setup, source_data):
    """Verify that if we request a chunk exactly, it returns the original object without slicing (identity check)."""
    cache, _ = cache_setup

    # Prime cache: Chunks will be (0, 5, data) and (5, 15, data)
    cache._fetch(0, 5)

    # Fetch exactly the second chunk (readahead buffer)
    # The logic inside _fetch has a check: if slice_start==0 and slice_end==len...
    exact_chunk = cache._fetch(5, 15)

    # Verify values
    assert exact_chunk == source_data[5:15]

    # Verify Identity (Zero Copy)
    # Note: string/bytes literals might be interned, but since we slice from source_data,
    # identity checks on the deque contents vs result should pass if logic holds.
    stored_readahead = cache.chunks[1][2]
    assert exact_chunk is stored_readahead


def test_end_of_file_truncation(cache_setup, source_data):
    """Ensure readahead doesn't go past file size."""
    cache, fetcher = cache_setup
    # File size is 100.

    # Fetch 95-100.
    # missing_len = 5.
    # readahead would usually be 10, but file ends at 100.
    result = cache._fetch(95, 100)

    assert result == source_data[95:100]
    assert len(fetcher.call_log) == 1

    # Check lengths requested.
    # Request: 5 bytes. Remaining space: 0. Readahead should be 0.
    args = fetcher.call_log[0]
    assert args["start"] == 95
    # Should only request the 5 bytes needed, no readahead
    assert args["chunk_lengths"] == [5]

    # Ensure no empty readahead chunk was added
    assert len(cache.chunks) == 1


def test_none_arguments(cache_setup, source_data):
    """Test behavior when start/end are None."""
    cache, _ = cache_setup

    # Fetch all
    result = cache._fetch(None, None)
    assert len(result) == 100
    assert result == source_data


def test_out_of_bounds(cache_setup):
    """Test start >= size returns empty."""
    cache, _ = cache_setup
    assert cache._fetch(150, 200) == b""


class TrackedAsyncMockFetcher:
    """Simulates an async backend and tracks calls for assertions."""

    def __init__(self, data: bytes):
        self.data = data
        self.should_fail = False
        self.calls = []

    async def __call__(self, start, size, split_factor=1):
        self.calls.append({"start": start, "size": size, "split_factor": split_factor})
        if self.should_fail:
            raise RuntimeError("Mocked network error")

        await asyncio.sleep(0.001)
        end = min(start + size, len(self.data))
        return self.data[start:end]


@pytest.fixture
def prefetcher_setup(source_data):
    """Provides a fresh Prefetcher and its mocked fetcher for each test."""
    fetcher = TrackedAsyncMockFetcher(source_data)

    cache = Prefetcher(
        blocksize=10,
        fetcher=fetcher,
        size=len(source_data),
        max_prefetch_size=30,
        concurrency=4,
    )
    yield cache, fetcher
    cache.close()


def test_prefetcher_initial_state(prefetcher_setup):
    cache, _ = prefetcher_setup
    assert cache.user_offset == 0
    assert cache.sequential_streak == 0
    assert not cache.is_stopped


def test_prefetcher_sequential_reads(prefetcher_setup, source_data):
    cache, _ = prefetcher_setup

    res1 = cache._fetch(0, 15)
    assert res1 == source_data[0:15]
    assert cache.sequential_streak > 0
    res2 = cache._fetch(15, 25)
    assert res2 == source_data[15:25]


def test_prefetcher_out_of_bounds(prefetcher_setup):
    cache, _ = prefetcher_setup
    res = cache._fetch(250, 260)
    assert res == b""


def test_prefetcher_with_no_offsets(prefetcher_setup, source_data):
    cache, _ = prefetcher_setup
    res = cache._fetch(None, None)
    assert res == source_data


def test_prefetcher_seek_resets_streak(prefetcher_setup, source_data):
    cache, _ = prefetcher_setup

    cache._fetch(0, 10)
    assert cache.sequential_streak > 0

    res = cache._fetch(50, 60)
    assert res == source_data[50:60]
    assert cache.user_offset == 60


def test_prefetcher_exact_block_reads(prefetcher_setup, source_data):
    """Test reading exactly the blocksize increments streak and fetches correctly."""
    cache, fetcher = prefetcher_setup

    res1 = cache._fetch(0, 10)
    assert res1 == source_data[0:10]
    assert cache.sequential_streak == 1

    res2 = cache._fetch(10, 20)
    assert res2 == source_data[10:20]
    assert cache.sequential_streak == 2

    assert len(fetcher.calls) >= 2


def test_prefetcher_adaptive_small_reads(prefetcher_setup, source_data):
    """Test that reading a small amount scales the fetcher down to match."""
    cache, fetcher = prefetcher_setup

    # Fetch 4 bytes. Blocksize is 10 originally, but the read history makes adaptive size=4.
    res1 = cache._fetch(0, 4)
    assert res1 == source_data[0:4]

    # Because adaptive blocksize=4, the producer specifically fetched 4 bytes.
    # Therefore, 0 bytes remain in the zero-copy block.
    assert len(cache._current_block) - cache._current_block_idx == 0
    assert cache.user_offset == 4

    # Verify the fetcher only requested 4 bytes from the backend
    assert fetcher.calls[0]["size"] == 4


def test_prefetcher_partial_read_from_queued_block(prefetcher_setup, source_data):
    """Test zero-copy pointer logic when the queued block is larger than the read request."""
    cache, fetcher = prefetcher_setup

    # Manually queue an explicit 10-byte block simulating background prefetching
    task = asyncio.Future()
    task.set_result(source_data[0:10])
    cache.queue.put_nowait(task)

    # User only asks for 4 bytes out of the 10-byte queued block
    res1 = cache._fetch(0, 4)
    assert res1 == source_data[0:4]

    # 10 bytes were queued, 4 consumed, leaving exactly 6 in the zero-copy buffer
    assert len(cache._current_block) - cache._current_block_idx == 6
    assert cache.user_offset == 4

    # The next read should drain the remaining zero-copy buffer without fetching
    res2 = cache._fetch(4, 8)
    assert res2 == source_data[4:8]
    assert cache.user_offset == 8
    assert len(cache._current_block) - cache._current_block_idx == 2


def test_prefetcher_cross_block_read(prefetcher_setup, source_data):
    """Test requesting a large chunk that spans multiple underlying prefetch blocks."""
    cache, _ = prefetcher_setup
    res = cache._fetch(0, 25)

    assert res == source_data[0:25]
    assert cache.user_offset == 25

    # Read history becomes 25, adaptive size becomes 25.
    # The producer fetches 25, we consume 25. Exactly 0 remain.
    assert len(cache._current_block) - cache._current_block_idx == 0


def test_prefetcher_seek_same_offset(prefetcher_setup):
    """Test that seeking to the current user_offset is a no-op and does not clear buffers."""
    cache, _ = prefetcher_setup
    cache._fetch(0, 5)
    streak_before = cache.sequential_streak
    block_before = cache._current_block
    idx_before = cache._current_block_idx

    fsspec.asyn.sync(cache.loop, cache.seek, cache.user_offset)
    assert cache.sequential_streak == streak_before
    assert cache._current_block == block_before
    assert cache._current_block_idx == idx_before


def test_prefetcher_eof_handling(prefetcher_setup, source_data):
    """Test behavior when fetching up to and past the file size limit."""
    cache, _ = prefetcher_setup
    res = cache._fetch(95, 110)
    assert res == source_data[95:100]
    assert cache._fetch(105, 115) == b""


def test_prefetcher_producer_error_propagation(prefetcher_setup):
    """Test that exceptions in the background fetcher task surface to the caller."""
    cache, fetcher = prefetcher_setup
    fetcher.should_fail = True
    with pytest.raises(RuntimeError, match="Mocked network error"):
        cache._fetch(0, 10)
    assert cache.is_stopped is True


def test_prefetcher_dynamic_split_factor(prefetcher_setup, source_data):
    """Test that split_factor increases for large chunks on sequential reads."""
    cache, fetcher = prefetcher_setup

    with mock.patch.object(Prefetcher, "MIN_CHUNK_SIZE", 5):
        cache._fetch(0, 10)
        cache._fetch(10, 20)

        fsspec.asyn.sync(cache.loop, asyncio.sleep, 0.05)

    recent_calls = [c for c in fetcher.calls if c["start"] >= 20]
    assert len(recent_calls) > 0
    assert recent_calls[0]["split_factor"] > 1


def test_prefetcher_max_prefetch_limit(prefetcher_setup):
    """Test that the producer pauses when the queue hits the max_prefetch_size."""
    cache, _ = prefetcher_setup
    cache._fetch(0, 1)
    fsspec.asyn.sync(cache.loop, asyncio.sleep, 0.05)
    max_expected_offset = cache.user_offset + cache.max_prefetch_size + cache.blocksize
    assert cache.current_offset <= max_expected_offset


def test_prefetcher_close_while_active(prefetcher_setup):
    """Test that closing the prefetcher safely cancels pending background tasks."""
    cache, _ = prefetcher_setup
    cache._fetch(0, 10)
    cache._fetch(10, 20)

    assert len(cache._active_tasks) > 0 or not cache.queue.empty()
    assert cache.is_stopped is False

    cache.close()

    assert cache.is_stopped is True
    assert len(cache._active_tasks) == 0
    assert cache.queue.empty() is True


def test_prefetcher_adaptive_averaging(prefetcher_setup):
    """Verify that the blocksize adapts upwards and downwards based on read history."""
    cache, _ = prefetcher_setup
    assert cache._get_adaptive_blocksize() == 10

    # Test upward adaptation (5 reads of 12 = 60 bytes used)
    for _ in range(5):
        cache._fetch(cache.user_offset, cache.user_offset + 12)

    # Average of five 12s is 12
    assert cache._get_adaptive_blocksize() == 12

    # Test downward adaptation (5 reads of 4 = 20 bytes used, 80 total)
    for _ in range(5):
        cache._fetch(cache.user_offset, cache.user_offset + 4)

    # Average of five 12s and five 4s is (60 + 20) / 10 = 80 / 10 = 8
    assert cache._get_adaptive_blocksize() == 8

    # Push out all the 12s, leaving only 4s (5 reads of 4 = 20 bytes used, exactly 100 total)
    for _ in range(5):
        cache._fetch(cache.user_offset, cache.user_offset + 4)

    # Average of ten 4s is 4
    assert cache._get_adaptive_blocksize() == 4


def test_prefetcher_history_eviction(prefetcher_setup):
    """Verify that only the last 10 reads impact the adaptive blocksize."""
    cache, _ = prefetcher_setup
    for _ in range(10):
        cache._fetch(cache.user_offset, cache.user_offset + 1)

    assert cache.history_sum == 10
    assert len(cache.read_history) == 10

    # Adding a large read should evict the oldest 1
    cache._fetch(cache.user_offset, cache.user_offset + 10)
    assert cache.history_sum == 19
    assert cache.read_history[-1] == 10


def test_prefetcher_seek_resets_history(prefetcher_setup):
    """Verify that a seek clears adaptive history to prevent stale logic."""
    cache, _ = prefetcher_setup
    cache._fetch(0, 100)
    cache._fetch(100, 200)
    cache._fetch(200, 300)
    assert cache.history_sum > 0

    fsspec.asyn.sync(cache.loop, cache.seek, 500)
    assert cache.history_sum == 0
    assert len(cache.read_history) == 0
    assert cache._get_adaptive_blocksize() == cache.blocksize


def test_prefetcher_queue_empty_race_condition(prefetcher_setup):
    """
    Verify that the defensive asyncio.QueueEmpty catch works if the queue
    reports not empty but actually contains no items.
    """
    cache, _ = prefetcher_setup

    while not cache.queue.empty():
        cache.queue.get_nowait()

    with mock.patch.object(cache.queue, "empty", side_effect=[False, True]):
        fsspec.asyn.sync(cache.loop, cache._cancel_all_tasks, False)


def test_producer_loop_uses_adaptive_size(prefetcher_setup, source_data):
    """Verify the producer actually fetches using the adaptive blocksize."""
    cache, fetcher = prefetcher_setup

    with mock.patch.object(cache, "_get_adaptive_blocksize", return_value=15):
        cache._wakeup_producer.set()
        fsspec.asyn.sync(cache.loop, asyncio.sleep, 0.1)

        prefetch_calls = [c for c in fetcher.calls]
        assert len(prefetch_calls) > 0
        assert prefetch_calls[-1]["size"] == 15


def test_prefetcher_producer_exception_handling(prefetcher_setup):
    """
    Verify that an unexpected exception inside the producer loop is caught,
    placed into the queue, and stops the cache.
    """
    cache, _ = prefetcher_setup

    with mock.patch.object(
        cache, "_get_adaptive_blocksize", side_effect=Exception("Mocked Error!")
    ):
        cache._wakeup_producer.set()

        fsspec.asyn.sync(cache.loop, asyncio.sleep, 0.05)
        with pytest.raises(Exception, match="Mocked Error!"):
            cache._fetch(0, 10)


def test_prefetcher_producer_early_stop(prefetcher_setup):
    """
    Verify that if the cache is stopped while the producer is waiting,
    waking it up causes it to immediately break the loop.
    """
    cache, _ = prefetcher_setup
    cache.is_stopped = True
    cache._wakeup_producer.set()
    fsspec.asyn.sync(cache.loop, asyncio.sleep, 0.05)
    assert cache._producer_task.done() is True


def test_prefetcher_read_after_producer_stops(prefetcher_setup):
    """
    Test that reading from a Prefetcher after the producer has fatally
    stopped (and the queue is drained of the original error) raises our
    new RuntimeError safeguard.
    """
    cache, fetcher = prefetcher_setup
    fetcher.should_fail = True
    with pytest.raises(RuntimeError, match="Mocked network error"):
        cache._fetch(0, 10)

    assert cache.is_stopped is True
    assert cache.queue.empty() is True
    assert cache.user_offset < cache.size
    with pytest.raises(
        RuntimeError, match="Could not fetch data, the producer is stopped"
    ):
        cache._fetch(cache.user_offset, cache.user_offset + 10)


def test_prefetcher_zero_copy_full_current_block(prefetcher_setup, source_data):
    """
    Test the zero-copy optimization path where the requested size matches
    the entire available _current_block, and the index is 0.
    """
    cache, _ = prefetcher_setup
    cache._current_block = source_data[0:10]
    cache._current_block_idx = 0
    res = cache._fetch(0, 10)

    assert res == source_data[0:10]
    assert res is cache._current_block
    assert cache.user_offset == 10


def test_prefetcher_break_on_empty_block(prefetcher_setup):
    """
    Test that _async_fetch safely breaks its collection loop if read()
    unexpectedly returns an empty bytes object.
    """
    cache, _ = prefetcher_setup
    task = asyncio.Future()
    task.set_result(b"")
    cache.queue.put_nowait(task)
    res = cache._fetch(0, 10)
    assert res == b""


def test_prefetcher_cancelled_error_propagation(prefetcher_setup):
    """
    Verify that an asyncio.CancelledError is re-raised without altering
    the is_stopped flag, differentiating a deliberate cancellation from a crash.
    """
    cache, _ = prefetcher_setup

    async def simulate_cancellation():
        task = cache.loop.create_future()
        task.cancel()
        cache.queue.put_nowait(task)
        assert cache.is_stopped is False
        with pytest.raises(asyncio.CancelledError):
            await cache.read()

    fsspec.asyn.sync(cache.loop, simulate_cancellation)
    assert cache.is_stopped is False
