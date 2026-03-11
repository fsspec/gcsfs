import asyncio
from collections import deque

import fsspec.asyn
from fsspec.caching import BaseCache, register_cache


class ReadAheadChunked(BaseCache):
    """
    An optimized ReadAhead cache that fetches multiple chunks in a single
    HTTP request but manages them as separate bytes objects to avoid
    expensive memory slicing.

    While this approach primarily optimizes for CPU and memory allocation overhead,
    it strictly maintains the same semantics as the existing readahead cache.
    For example, if a user requests 5MB and the cache fetches 10MB, it serves the
    requested 5MB but retains that data in memory to handle potential backward seeks.
    This mirrors the standard readahead behavior, which does not eagerly discard served
    chunks until a new fetch is required.
    """

    name = "readahead_chunked"

    def __init__(self, blocksize: int, fetcher, size: int) -> None:
        super().__init__(blocksize, fetcher, size)
        self.chunks = deque()  # Entries: (start, end, data_bytes)

    @property
    def cache(self):
        """
        Compatibility property for tests/legacy code that expects 'cache'
        to be a single bytestring.

        WARNING: Accessing this property forces a memory copy of the
        entire current buffer, negating the Zero-Copy optimization
        of ReadAheadChunked. Use for debugging/testing only.
        """
        if not self.chunks:
            return b""
        return b"".join(chunk[2] for chunk in self.chunks)

    def _fetch(self, start: int | None, end: int | None) -> bytes:
        if start is None:
            start = 0
        if end is None or end > self.size:
            end = self.size
        if start >= self.size:
            return b""

        # Handle backward seeks that go beyond the start of our cache window
        if self.chunks and self.chunks[0][0] > start:
            self.chunks.clear()

        parts = []
        current_pos = start

        # Satisfy as much as possible from the existing cache (Zero-Copy)
        for c_start, c_end, c_data in self.chunks:
            if c_end <= start:
                continue  # Skip chunks completely before our window

            if c_start >= end:
                break  # If we've reached chunks completely past our window, stop

            if c_end > current_pos:
                slice_start = max(0, current_pos - c_start)
                slice_end = min(len(c_data), end - c_start)

                if slice_start == 0 and slice_end == len(c_data):
                    # Zero-copy: Direct reference to the full object
                    parts.append(c_data)
                else:
                    # Slicing creates a copy, but it's unavoidable for partials
                    parts.append(c_data[slice_start:slice_end])

                current_pos += slice_end - slice_start

        # Fetch missing data if necessary
        should_fetch_backend = current_pos < end
        if should_fetch_backend:
            # On a cache miss, we replace the entire window (standard readahead behavior)
            self.chunks.clear()

            missing_len = min(self.size - current_pos, end - current_pos)
            readahead_block = min(
                self.size - (current_pos + missing_len), self.blocksize
            )

            self.miss_count += 1
            chunk_lengths = [missing_len]
            if readahead_block > 0:
                chunk_lengths.append(readahead_block)

            # Vector read call
            new_chunks = self.fetcher(start=current_pos, chunk_lengths=chunk_lengths)

            # Process the requested data
            req_data = new_chunks[0]
            self.chunks.append((current_pos, current_pos + len(req_data), req_data))
            self.total_requested_bytes += len(req_data)
            parts.append(req_data)

            # Process the readahead data (if any)
            if len(new_chunks) > 1:
                ra_data = new_chunks[1]
                ra_start = current_pos + len(req_data)
                self.chunks.append((ra_start, ra_start + len(ra_data), ra_data))
                self.total_requested_bytes += len(ra_data)

        if not parts:
            return b""

        if not should_fetch_backend:
            self.hit_count += 1

        # Optimization: return the single object directly if possible
        if len(parts) == 1:
            return parts[0]

        return b"".join(parts)


class Prefetcher(BaseCache):
    """
    Asynchronous prefetching cache that reads ahead.

    This cache spawns a background producer task that fetches sequential
    blocks of data before they are explicitly requested. It is highly optimized
    for sequential reads but can recover from arbitrary seeks by restarting
    the prefetch loop.

    Parameters
    ----------
    blocksize : int
        Base size of the chunks to read ahead, in bytes.
    fetcher : Callable
        A coroutine of the form `f(start, end)` which gets bytes from the remote.
    size : int
        Total size of the file being read.
    max_prefetch_size : int, optional
        Maximum bytes to prefetch ahead of the current user offset.
        Defaults to `max(2 * blocksize, 128MB)`.
    concurrency : int, optional
        Number of concurrent network requests to use for large chunks. Defaults to 4.
    """

    name = "prefetcher"

    MIN_CHUNK_SIZE = 5 * 1024 * 1024
    DEFAULT_PREFETCH_SIZE = 128 * 1024 * 1024

    def __init__(
        self,
        blocksize: int,
        fetcher,
        size: int,
        max_prefetch_size=None,
        concurrency=4,
        **kwargs,
    ):
        super().__init__(blocksize, fetcher, size)
        self.fetcher = kwargs.pop("fetcher_override", self.fetcher)
        self.concurrency = concurrency
        self._user_max_prefetch_size = max_prefetch_size
        self.sequential_streak = 0
        self.user_offset = 0
        self.current_offset = 0
        self.queue = asyncio.Queue()
        self.is_stopped = False
        self._active_tasks = set()
        self._wakeup_producer = asyncio.Event()
        self._current_block = b""
        self._current_block_idx = 0
        self.loop = fsspec.asyn.get_loop()
        self.read_history = deque(maxlen=10)
        self.history_sum = 0

        async def _start_producer():
            self._producer_task = asyncio.create_task(self._producer_loop())

        fsspec.asyn.sync(self.loop, _start_producer)

    def _get_adaptive_blocksize(self) -> int:
        """Returns the adaptive blocksize configuration."""
        count = len(self.read_history)
        if not count:
            avg_size = self.blocksize
        else:
            avg_size = self.history_sum // count

        # Cap the adaptive blocksize only if the user explicitly set a max prefetch size
        if self._user_max_prefetch_size is not None:
            return min(avg_size, self._user_max_prefetch_size)

        return avg_size

    @property
    def max_prefetch_size(self) -> int:
        """Dynamically calculates max prefetch based on user intent or current blocksize."""
        if self._user_max_prefetch_size is not None:
            return self._user_max_prefetch_size

        return max(2 * self._get_adaptive_blocksize(), self.DEFAULT_PREFETCH_SIZE)

    async def _cancel_all_tasks(self, wait=False):
        self.is_stopped = True
        self._wakeup_producer.set()

        tasks_to_wait = []

        if hasattr(self, "_producer_task") and isinstance(
            self._producer_task, asyncio.Task
        ):
            if not self._producer_task.done():
                self._producer_task.cancel()
                tasks_to_wait.append(self._producer_task)

        for task in list(self._active_tasks):
            if not task.done():
                tasks_to_wait.append(task)

        self._active_tasks.clear()
        if hasattr(self, "queue"):
            while not self.queue.empty():
                try:
                    self.queue.get_nowait()
                except asyncio.QueueEmpty:
                    break

        if wait and tasks_to_wait:
            await asyncio.gather(*tasks_to_wait, return_exceptions=True)

    async def _restart_producer(self):
        # Cancel old tasks without waiting
        await self._cancel_all_tasks(wait=False)
        self.is_stopped = False
        self.sequential_streak = 0
        self.read_history.clear()
        self.history_sum = 0
        self._producer_task = asyncio.create_task(self._producer_loop())

    async def _producer_loop(self):
        try:
            while not self.is_stopped:
                await self._wakeup_producer.wait()
                self._wakeup_producer.clear()

                block_size = self._get_adaptive_blocksize()
                prefetch_size = min(
                    (self.sequential_streak + 1) * block_size,
                    self.max_prefetch_size,
                )

                while (
                    not self.is_stopped
                    and (self.current_offset - self.user_offset) < prefetch_size
                    and self.current_offset < self.size
                ):
                    space_remaining = self.size - self.current_offset
                    prefetch_space_available = prefetch_size - (
                        self.current_offset - self.user_offset
                    )
                    if (
                        space_remaining >= block_size
                        and prefetch_space_available < block_size
                    ):
                        break

                    if prefetch_size >= self.MIN_CHUNK_SIZE:
                        if prefetch_space_available >= self.MIN_CHUNK_SIZE:
                            actual_size = min(
                                max(self.MIN_CHUNK_SIZE, block_size),
                                space_remaining,
                            )
                        else:
                            break
                    else:
                        actual_size = min(block_size, space_remaining)

                    if self.sequential_streak < 2:
                        sfactor = (
                            self.concurrency
                            if actual_size >= self.MIN_CHUNK_SIZE
                            else min(self.concurrency, 1)
                        )  # random usecase
                    else:
                        sfactor = (
                            min(
                                self.concurrency,
                                max(1, actual_size * self.concurrency // prefetch_size),
                            )
                            if actual_size >= self.MIN_CHUNK_SIZE
                            else 1
                        )  # sequential usecase

                    download_task = asyncio.create_task(
                        self.fetcher(
                            self.current_offset, actual_size, split_factor=sfactor
                        )
                    )
                    self._active_tasks.add(download_task)
                    download_task.add_done_callback(self._active_tasks.discard)

                    await self.queue.put(download_task)
                    self.current_offset += actual_size

        except asyncio.CancelledError:
            pass
        except Exception as e:
            await self.queue.put(e)
            self.is_stopped = True

    async def read(self):
        """Reads the next chunk from the object."""
        if self.user_offset >= self.size:
            return b""
        if self.is_stopped and self.queue.empty():
            # This may happen if user read despite previous read produced an exception.
            raise RuntimeError("Could not fetch data, the producer is stopped")

        if self.queue.empty():
            self._wakeup_producer.set()

        task = await self.queue.get()

        # Check if the producer pushed an exception
        if isinstance(task, Exception):
            self.is_stopped = True
            raise task

        if task.done():
            self.hit_count += 1
        else:
            self.miss_count += 1

        try:
            block = await task
            self.sequential_streak += 1
            if self.sequential_streak >= 2:
                self._wakeup_producer.set()  # starts prefetching.
            return block
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.is_stopped = True
            raise e

    async def seek(self, new_offset):
        if new_offset == self.user_offset:
            return

        self.user_offset = new_offset
        self.current_offset = new_offset
        await self._restart_producer()

    async def _async_fetch(self, start, end):
        if start != self.user_offset:
            # We seeked elsewhere, reset the current block
            self._current_block = b""
            self._current_block_idx = 0
            await self.seek(start)

        requested_size = end - start
        chunks = []
        collected = 0

        # Update read history for the adaptive blocksize logic
        if requested_size > 0:
            if len(self.read_history) == self.read_history.maxlen:
                self.history_sum -= self.read_history[0]
            self.read_history.append(requested_size)
            self.history_sum += requested_size

        available_in_block = len(self._current_block) - self._current_block_idx
        if available_in_block > 0:
            take = min(requested_size, available_in_block)

            if take == len(self._current_block) and self._current_block_idx == 0:
                chunks.append(self._current_block)
            else:
                chunks.append(
                    self._current_block[
                        self._current_block_idx : self._current_block_idx + take
                    ]
                )

            self._current_block_idx += take
            collected += take
            self.user_offset += take

        while collected < requested_size and self.user_offset < self.size:
            block = await self.read()
            if not block:
                break

            needed = requested_size - collected
            if len(block) > needed:
                chunks.append(block[:needed])
                self._current_block = block
                self._current_block_idx = needed
                collected += needed
                self.user_offset += needed
                break
            else:
                chunks.append(block)
                collected += len(block)
                self.user_offset += len(block)
                self._current_block = b""
                self._current_block_idx = 0

        if len(chunks) == 1:
            out = chunks[0]
        else:
            out = b"".join(chunks)

        self.total_requested_bytes += len(out)
        return out

    def _fetch(self, start: int | None, stop: int | None) -> bytes:
        if start is None:
            start = 0
        if stop is None:
            stop = self.size
        if start >= self.size or start >= stop:
            return b""
        return fsspec.asyn.sync(self.loop, self._async_fetch, start, stop)

    def close(self):
        """Clean shutdown. Cancels tasks and waits for them to abort."""
        fsspec.asyn.sync(self.loop, self._cancel_all_tasks, True)


for gcs_cache in [ReadAheadChunked, Prefetcher]:
    register_cache(gcs_cache, clobber=True)
