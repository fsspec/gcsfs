import pytest

from gcsfs.caching import ReadAheadChunked


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
