import asyncio
import collections
import concurrent.futures
import ctypes
import logging
import os
import sys
import threading
import weakref
from io import BytesIO

from fsspec.asyn import FSTimeoutError
from google.api_core.exceptions import NotFound
from google.cloud.storage.asyncio.async_appendable_object_writer import (
    _DEFAULT_FLUSH_INTERVAL_BYTES,
    AsyncAppendableObjectWriter,
)
from google.cloud.storage.asyncio.async_multi_range_downloader import (
    AsyncMultiRangeDownloader,
    MRDStreamConfig,
)

MRD_MAX_RANGES = 1000  # MRD supports up to 1000 ranges per request
try:
    DEFAULT_CONCURRENCY = int(os.environ.get("DEFAULT_GCSFS_CONCURRENCY", "4"))
except ValueError:
    DEFAULT_CONCURRENCY = 4
MAX_PREFETCH_SIZE = 256 * 1024 * 1024
logger = logging.getLogger("gcsfs")


try:
    PyBytes_FromStringAndSize = ctypes.pythonapi.PyBytes_FromStringAndSize
    PyBytes_FromStringAndSize.argtypes = (ctypes.c_void_p, ctypes.c_ssize_t)
    PyBytes_FromStringAndSize.restype = ctypes.py_object

    PyBytes_AsString = ctypes.pythonapi.PyBytes_AsString
    PyBytes_AsString.argtypes = (ctypes.py_object,)
    PyBytes_AsString.restype = ctypes.c_void_p
    HAS_CPYTHON_API = True
except Exception:
    PyBytes_FromStringAndSize = None
    PyBytes_AsString = None
    HAS_CPYTHON_API = False


async def init_mrd(
    grpc_client,
    bucket_name,
    object_name,
    generation=None,
    cache_type=None,
    cache_source=None,
    stream_config=None,
    concurrency=None,
    read_handle=None,
):
    """
    Creates the AsyncMultiRangeDownloader using an existing client.
    Wraps Google API errors into standard Python exceptions.
    """
    from gcsfs.core import _get_cache_type_header_value

    metadata = None
    cache_val = _get_cache_type_header_value(cache_type, cache_source)
    if cache_val:
        metadata = [("x-goog-api-client", cache_val)]

    kwargs = {}
    if metadata:
        kwargs["metadata"] = metadata
    if read_handle is not None:
        kwargs["read_handle"] = read_handle

    if stream_config is None and concurrency is not None and concurrency > 1:
        stream_config = MRDStreamConfig(min_connections=1, max_connections=concurrency)
    if stream_config is not None:
        kwargs["stream_config"] = stream_config

    try:
        return await AsyncMultiRangeDownloader.create_mrd(
            grpc_client, bucket_name, object_name, generation, **kwargs
        )
    except NotFound:
        # We wrap the error here to match standard Python error handling
        # and avoid leaking Google API exceptions to users.
        raise FileNotFoundError(f"{bucket_name}/{object_name}")


async def download_range(offset, length, mrd):
    """
    Downloads a byte range from the file asynchronously.
    """
    # If length = 0, mrd returns till end of file, so handle that case here
    if length == 0:
        return b""
    buffer = BytesIO()
    await mrd.download_ranges([(offset, length, buffer)])
    data = buffer.getvalue()
    bytes_downloaded = len(data)

    if length != bytes_downloaded:
        logger.warning(
            f"Short read detected for {mrd.bucket_name}/{mrd.object_name}! "
            f"Requested {length} bytes but downloaded {bytes_downloaded} bytes."
        )

    logger.debug(
        f"Requested {length} bytes from offset {offset}, downloaded {bytes_downloaded} "
        f"bytes from mrd path: {mrd.bucket_name}/{mrd.object_name}"
    )
    return data


async def download_ranges(ranges, mrd):
    """
    Downloads multiple byte ranges from the file asynchronously in a single batch.

    Args:
        ranges: List of (offset, length) tuples to download. Max 1000 ranges allowed.
        mrd: AsyncMultiRangeDownloader instance

    Returns:
        List of bytes objects, one for each range
    """
    # Prepare tasks: Filter out empty ranges and create buffers immediately
    # Structure: (original_index, offset, length, buffer)
    # Calling MRD with length=0 returns till end of file. We handle zero-length
    # ranges by returning b"" without calling MRD. So only create tasks for length > 0

    if len(ranges) > MRD_MAX_RANGES:
        raise ValueError("Invalid input - number of ranges cannot be more than 1000")

    tasks = [
        (i, off, length, BytesIO())
        for i, (off, length) in enumerate(ranges)
        if length > 0
    ]

    # Execute Download
    if tasks:
        # The MRD expects list of (offset, length, buffer)
        # We extract these from our task list
        await mrd.download_ranges([(off, length, buf) for _, off, length, buf in tasks])

    # Map results back to their original positions
    results = [b""] * len(ranges)
    for i, _, _, buffer in tasks:
        results[i] = buffer.getvalue()

    # Log stats
    total_requested = sum(r[1] for r in ranges)
    total_downloaded = sum(len(r) for r in results)

    if total_requested != total_downloaded:
        logger.warning(
            f"Short read detected for {mrd.bucket_name}/{mrd.object_name}! "
            f"Requested {total_requested} bytes but downloaded {total_downloaded} bytes."
        )

    if logger.isEnabledFor(logging.DEBUG):
        requested_ranges_to_log = [(r[0], r[1]) for r in ranges]
        logger.debug(
            f"mrd path: {mrd.bucket_name}/{mrd.object_name} | "
            f"Requested {len(ranges)} ranges: {requested_ranges_to_log} | "
            f"total bytes requested: {total_requested} | "
            f"total bytes downloaded: {total_downloaded}"
        )

    return results


async def init_aaow(
    grpc_client, bucket_name, object_name, generation=None, flush_interval_bytes=None
):
    """
    Creates and opens the AsyncAppendableObjectWriter.
    """
    writer_options = {}
    # Only pass flush_interval_bytes if the user explicitly provided a
    # non-default flush interval.
    if flush_interval_bytes and flush_interval_bytes != _DEFAULT_FLUSH_INTERVAL_BYTES:
        writer_options["FLUSH_INTERVAL_BYTES"] = flush_interval_bytes
    writer = AsyncAppendableObjectWriter(
        client=grpc_client,
        bucket_name=bucket_name,
        object_name=object_name,
        generation=generation,
        writer_options=writer_options,
    )
    await writer.open()
    return writer


async def close_mrd(mrd):
    """
    Closes the AsyncMultiRangeDownloader gracefully.
    Logs a warning if closing fails, instead of raising an exception.
    """
    if mrd:
        try:
            await mrd.close()
        except Exception as e:
            logger.warning(
                f"Error closing AsyncMultiRangeDownloader for {mrd.bucket_name}/{mrd.object_name}: {e}"
            )


async def close_aaow(aaow, finalize_on_close=False):
    """
    Closes the AsyncAppendableObjectWriter gracefully.
    Logs a warning if closing fails, instead of raising an exception.
    """
    if aaow:
        try:
            await aaow.close(finalize_on_close=finalize_on_close)
        except Exception as e:
            logger.warning(
                f"Error closing AsyncAppendableObjectWriter for {aaow.bucket_name}/{aaow.object_name}: {e}"
            )


# Default timeout for synchronous teardowns when no explicit timeout is configured.
DEFAULT_TEARDOWN_TIMEOUT_SECONDS = 60.0

# Strong references for background tasks scheduled via loop.create_task().
# Without holding external references, Python's asyncio event loop may allow
# pending tasks to be garbage-collected mid-execution ("Task was destroyed but
# it is pending").
_deferred_close_tasks = set()
_deferred_close_lock = threading.Lock()


def _on_loop_thread(loop):
    """Returns True if the current thread is servicing the given event loop."""
    if loop is None:
        return False
    try:
        return asyncio.get_running_loop() is loop
    except RuntimeError:
        return False


def _defer_task(
    loop,
    coro,
    description="deferred task",
    logger=None,
    log_level=logging.WARNING,
):
    """Schedules a coroutine as a tracked background task on ``loop``.

    Retains a strong reference in ``_deferred_close_tasks`` until completion to
    prevent asyncio garbage collection from discarding pending tasks mid-flight,
    and ensures unhandled task exceptions are retrieved and logged.
    """
    task = loop.create_task(coro)
    with _deferred_close_lock:
        _deferred_close_tasks.add(task)

    def _on_done(t):
        with _deferred_close_lock:
            _deferred_close_tasks.discard(t)
        if not t.cancelled():
            exc = t.exception()
            if exc:
                log = logger or logging.getLogger("gcsfs")
                log.log(
                    log_level,
                    "%s failed during asynchronous execution: %s",
                    description,
                    exc,
                    exc_info=(type(exc), exc, exc.__traceback__),
                )

    task.add_done_callback(_on_done)
    return task


def sync_teardown(
    loop,
    func_or_coro,
    *args,
    timeout=None,
    description="teardown",
    **kwargs,
):
    """Safely runs an async teardown coroutine on ``loop`` from synchronous context.

    Schedules via :func:`asyncio.run_coroutine_threadsafe` or defers on the loop
    thread to prevent deadlocks.
    """
    coro = func_or_coro(*args, **kwargs) if callable(func_or_coro) else func_or_coro
    if not asyncio.iscoroutine(coro):
        return

    if sys.is_finalizing():
        coro.close()
        return

    if loop is None or not loop.is_running() or loop.is_closed():
        coro.close()
        raise RuntimeError(f"Skipping {description}: no usable IO loop available.")

    if _on_loop_thread(loop):
        _defer_task(loop, coro, description=description, log_level=logging.ERROR)
        return

    try:
        future = asyncio.run_coroutine_threadsafe(coro, loop)
    except RuntimeError:
        coro.close()
        raise RuntimeError(f"Skipping {description}: event loop is closed.")

    if timeout is not None and timeout <= 0:
        return

    try:
        return future.result(timeout)
    except concurrent.futures.TimeoutError:
        raise FSTimeoutError(f"{description} did not complete within {timeout}s.")


class PartialView:
    """A bounded memory writer providing robust overfill/underfill constraint validations."""

    def __init__(self, parent, start_offset, expected_size):
        self.parent = parent
        self.start_offset = start_offset
        self.expected_size = expected_size
        self.current_offset = 0
        self._view_lock = threading.Lock()

    def write(self, data):
        """
        Schedules a write operation to memory mapping.
        """
        if not isinstance(data, bytes):
            raise ValueError(f"Expected bytes, but got {type(data)}")

        size = len(data)
        with self._view_lock:
            if self.current_offset + size > self.expected_size:
                error_msg = (
                    f"Attempted to write {size} bytes "
                    f"at offset {self.current_offset}. "
                    f"Max capacity is {self.expected_size} bytes."
                )
                raise BufferError(error_msg)

            abs_offset = self.start_offset + self.current_offset
            self.current_offset += size

        return self.parent._submit_write(abs_offset, data, size)

    def close(self):
        """
        Validates boundaries enforcing complete local payload consistency.
        """
        if self.current_offset < self.expected_size:
            error_msg = (
                f"Expected {self.expected_size} bytes, "
                f"but only received {self.current_offset} bytes. "
                f"Buffer contains uninitialized data."
            )
            raise BufferError(error_msg)


class DirectMemmoveBuffer:
    """
    A buffer-like object that writes data directly to memory asynchronously.

    This class provides an interface that queues `ctypes.memmove` operations
    to a thread pool executor. It provides synchronous backpressure: if `max_pending`
    operations are currently writing, the `write()` call will safely block the
    calling thread (e.g., an asyncio loop) until capacity frees up.

    Memory allocation is natively deferred. If the payload precisely aligns
    with expected bounds sequentially, it gracefully overrides manual memmoves
    using true Zero-Copy payload replacement safely under the hood.

    Note: This class is now strictly Thread-Safe
    """

    THRESHOLD_BYTES_FOR_SCHEDULING = 128 * 1024

    def __init__(self, expected_size, executor, max_pending=5):
        """
        Initializes the DirectMemmoveBuffer.

        Args:
            expected_size (int): The total amount of bytes expected to populate memory.
            executor (concurrent.futures.Executor): The thread pool executor to run the
                memmove operations. The lifecycle of this executor is managed by the caller.
            max_pending (int, optional): The maximum number of pending write operations
                allowed in the queue. Defaults to 5.
        """
        self.expected_size = expected_size
        self.executor = executor

        # Volatile state variables. Must only be amended while holding self._lock.
        self._pending_count = 0
        self._error = None
        self._total_bytes_written = 0
        self._stop_accepting_writes = False
        self._is_closed = False

        # Track allocated (start, end) intervals to prevent overlapping views.
        self._allocated_intervals = []

        # PyBytes Native Pointers & Allocation tracking natively handled
        self._result_bytes = None
        self._start_address = None

        # Primitives:
        # 1. semaphore: Provides backpressure by limiting the number of active tasks.
        # 2. _lock: Protects mutations to the volatile state variables above.
        # 3. _done_event: Signals when the queue of active background tasks reaches zero.
        self.semaphore = threading.Semaphore(max_pending)
        self._lock = threading.Lock()
        self._done_event = threading.Event()
        self._done_event.set()

    def get_view(self, offset, size):
        """Constructs secure mapped offset references correctly handling constraint layouts."""
        if offset < 0 or offset + size > self.expected_size:
            raise ValueError("Invalid view requested: exceeds physical boundaries!")

        start = offset
        end = offset + size

        with self._lock:
            if self._stop_accepting_writes or self._is_closed:
                raise ValueError("Cannot get view on a closed/closing buffer.")

            # Enforce Write-Once memory semantics: prevent overlapping views
            for a_start, a_end in self._allocated_intervals:
                if max(start, a_start) < min(end, a_end):
                    raise ValueError(
                        f"Overlapping view requested: [{start}, {end}) "
                        f"overlaps with already allocated view [{a_start}, {a_end})"
                    )

            self._allocated_intervals.append((start, end))

        return PartialView(self, offset, size)

    def _decrement_pending(self):
        """Helper to cleanly release concurrency primitives after a task finishes."""
        self.semaphore.release()
        with self._lock:
            self._pending_count -= 1
            if self._pending_count == 0:
                self._done_event.set()

    def _submit_write(self, dest_offset, data_bytes, size):
        if size == 0:
            with self._lock:
                if self._stop_accepting_writes or self._is_closed:
                    raise ValueError("I/O operation on closed buffer.")
                if self._error:
                    raise self._error

            fut = concurrent.futures.Future()
            fut.set_result(None)
            return fut

        self.semaphore.acquire()

        try:
            with self._lock:
                if self._stop_accepting_writes or self._is_closed:
                    raise ValueError("I/O operation on closed buffer.")

                if self._error:
                    raise self._error

                if self._result_bytes is None:
                    if dest_offset == 0 and size == self.expected_size:
                        # fastpath: return buffer directly
                        self._result_bytes = data_bytes
                        self.semaphore.release()  # Release because we skip the executor
                        fut = concurrent.futures.Future()
                        fut.set_result(None)
                        self._total_bytes_written += size
                        return fut
                    if HAS_CPYTHON_API:
                        self._result_bytes = PyBytes_FromStringAndSize(
                            None, self.expected_size
                        )
                        self._start_address = PyBytes_AsString(self._result_bytes)
                    else:
                        self._result_bytes = bytearray(self.expected_size)
                        self._start_address = (
                            -1
                        )  # Dummy value to pass the defensive check below

                # Defensive programming: gracefully catch internal overwrite attempts
                if self._start_address is None:
                    raise BufferError(
                        "Attempted to execute standard write over a Zero-Copied payload."
                    )

                if self._pending_count == 0:
                    self._done_event.clear()
                self._pending_count += 1

        except BaseException:
            self.semaphore.release()
            raise

        if size <= self.THRESHOLD_BYTES_FOR_SCHEDULING:
            # Fast path, no need to send it to executor
            try:
                self._do_memmove(dest_offset, data_bytes, size)
            except BaseException:
                # The exception is already captured in self._error by _do_memmove
                pass

            fut = concurrent.futures.Future()
            local_err = self._error
            if local_err:
                fut.set_exception(local_err)
            else:
                fut.set_result(None)
            return fut
        else:
            try:
                # Slow path, schedule it on executor.
                return self.executor.submit(
                    self._do_memmove, dest_offset, data_bytes, size
                )
            except BaseException as e:
                with self._lock:
                    self._error = e
                self._decrement_pending()
                raise e

    def _do_memmove(self, dest_offset, data_bytes, size):
        try:
            with self._lock:
                if self._error:
                    return

            # Isolate pointer math to CPython only.
            # PyPy uses memory-safe native slice assignment.
            if HAS_CPYTHON_API:
                dest = self._start_address + dest_offset
                ctypes.memmove(dest, data_bytes, size)
            else:
                memoryview(self._result_bytes)[
                    dest_offset : dest_offset + size
                ] = data_bytes

            with self._lock:
                self._total_bytes_written += size

        except BaseException as e:
            with self._lock:
                if self._error is None:
                    self._error = e
            raise
        finally:
            self._decrement_pending()

    def get_value(self):
        with self._lock:
            if self._error:
                raise self._error
            if not self._is_closed:
                raise RuntimeError("Buffer is still not closed yet!")
            if self._result_bytes is None and self.expected_size == 0:
                return b""
            if self._total_bytes_written < self.expected_size:
                raise BufferError(
                    f"Buffer incomplete: Expected {self.expected_size} bytes but "
                    f"only populated {self._total_bytes_written}. Returning this "
                    f"payload would leak uninitialized memory."
                )

            if not isinstance(self._result_bytes, bytes):
                return bytes(self._result_bytes)

            return self._result_bytes

    def close(self):
        """
        Locks the buffer preventing further incoming writes, waits for all pending
        write operations to complete, and checks for errors.
        """
        with self._lock:
            self._stop_accepting_writes = True

        self._done_event.wait()
        with self._lock:
            self._is_closed = True
            if self._error:
                raise self._error


async def _close_mrds(mrds, raise_exception=False):
    """Close a list of MRDs asynchronously."""
    if not mrds:
        return

    async def _close_single(mrd):
        if "_raw_close" in getattr(mrd, "__dict__", {}):
            res = mrd._raw_close()
        elif hasattr(mrd, "close"):
            res = mrd.close()
        else:
            return None
        if asyncio.iscoroutine(res):
            return await res
        return res

    results = await asyncio.gather(
        *(_close_single(mrd) for mrd in mrds), return_exceptions=True
    )
    for r in results:
        if isinstance(r, Exception):
            if raise_exception:
                raise r
            logger.warning("Error closing MRD: %s", r)


class MRDCache:
    """Filesystem-level cache of AsyncMultiRangeDownloader instances.

    Architecture:
    - Active Map (`_active`): Maps key -> [mrd, refcount] for files currently being read.
      Concurrent readers on the same object share the same active MRD instance.
    - Inactive LRU (`_inactive`): Bounded OrderedDict mapping key -> mrd for idle files.
      When all readers of a file finish, its MRD moves to `_inactive` for hot-path reuse.
      When idle MRDs exceed `max_idle_mrds`, the least recently used idle MRD is evicted and closed.
    """

    def __init__(
        self,
        gcsfs,
        max_idle_mrds: int = 16,
        **kwargs,
    ):
        """
        Initializes the MRDCache.

        Args:
            gcsfs (ExtendedGcsFileSystem): The filesystem instance.
            max_idle_mrds (int, optional): Maximum number of idle MRDs to retain in LRU. Defaults to 16.
        """
        self._gcsfs = weakref.ref(gcsfs)
        self._max_idle_mrds = max_idle_mrds
        self._pid = os.getpid()
        self._active = {}  # key -> [mrd, refcount]
        self._inactive = collections.OrderedDict()  # key -> mrd (LRU)
        self._pending = {}  # key -> asyncio.Future (in-flight MRD creations)
        self._closed = False

    async def get(
        self,
        bucket_name,
        object_name,
        generation,
        concurrency=1,
        cache_type=None,
        cache_source=None,
    ):
        """
        Gets an AsyncMultiRangeDownloader for the specified object.
        If an active MRD already exists for the object, it is shared and its refcount incremented.
        If an MRD creation for the same key is already in progress, concurrent callers wait for
        that first MRD to be created and then share it.
        Otherwise, an idle MRD from LRU is reused, or a new MRD is initialized.

        Args:
            bucket_name (str): Name of the bucket.
            object_name (str): Name of the object.
            generation (int): Object generation.
            concurrency (int, optional): Requested stream concurrency. Defaults to 1.
            cache_type (str, optional): The cache type string.
            cache_source (str, optional): The cache source string.

        Returns:
            AsyncMultiRangeDownloader: An active downloader ready for requests.
        """
        if self._closed:
            raise RuntimeError("MRDCache is closed.")
        fs = self._gcsfs()
        if fs is None:
            raise RuntimeError("ExtendedGcsFileSystem has been garbage collected.")

        info = await fs._info(f"{bucket_name}/{object_name}", generation=generation)
        if info:
            generation = generation or info.get("generation")
            finalized = (
                info.get("timeFinalized") is not None
                if "timeFinalized" in info
                else True
            )
        else:
            finalized = True
        key = (bucket_name, object_name, generation)

        pid = os.getpid()
        current_loop = asyncio.get_running_loop()
        if (
            getattr(self, "_pid", None) != pid
            or getattr(self, "_loop", None) is not current_loop
        ):
            self._pid = pid
            self._loop = current_loop
            self._active.clear()
            self._inactive.clear()
            self._pending.clear()

        while True:
            if self._closed:
                raise RuntimeError("MRDCache is closed.")

            # 1. If already active, share the instance and bump refcount
            if key in self._active:
                entry = self._active[key]
                entry[1] += 1
                return entry[0]

            # 2. Check if idle in inactive LRU (only finalized objects are cached idle)
            if finalized and key in self._inactive:
                mrd = self._inactive.pop(key)
                self._active[key] = [mrd, 1]
                return mrd

            # 3. If another coroutine is already creating this MRD, wait for it
            if key in self._pending:
                await self._pending[key]
                continue

            # 4. First caller creates a pending future for this key
            loop = asyncio.get_running_loop()
            fut = loop.create_future()
            self._pending[key] = fut
            break

        try:
            await fs._get_grpc_client()
            mrd = await init_mrd(
                fs.grpc_client,
                bucket_name,
                object_name,
                generation,
                cache_type=cache_type,
                cache_source=cache_source,
                concurrency=concurrency,
            )

            raw_close = mrd.close
            mrd._raw_close = raw_close

            async def _mrd_close():
                await self.release(mrd)

            mrd.close = _mrd_close
            mrd._cache = self
            mrd._cache_key = key
            mrd.finalized = finalized
            mrd.cache_type = cache_type
            mrd.cache_source = cache_source
            mrd.concurrency = concurrency

            self._active[key] = [mrd, 1]
            fut.set_result(mrd)
            return mrd
        except BaseException as e:
            fut.set_exception(e)
            raise
        finally:
            self._pending.pop(key, None)

    async def release(self, mrd):
        """
        Releases an active MRD reference. When the reference count drops to 0,
        the MRD is moved to the inactive LRU (or closed if unfinalized or caching is disabled).
        """
        current_loop = None
        try:
            current_loop = asyncio.get_running_loop()
        except RuntimeError:
            pass
        if (
            self._closed
            or getattr(self, "_pid", None) != os.getpid()
            or (current_loop is not None and getattr(self, "_loop", None) is not current_loop)
        ):
            return

        key = getattr(mrd, "_cache_key", mrd)
        entry = self._active.get(key)
        if not entry:
            return

        entry[1] -= 1
        if entry[1] > 0:
            return

        del self._active[key]
        mrd = entry[0]

        if getattr(mrd, "finalized", True) and self._max_idle_mrds > 0:
            self._inactive[key] = mrd
            self._inactive.move_to_end(key)
            if len(self._inactive) > self._max_idle_mrds:
                _, evicted = self._inactive.popitem(last=False)
                await _close_mrds([evicted], raise_exception=False)
        else:
            await _close_mrds([mrd], raise_exception=False)

    async def close(self):
        """
        Closes the cache and all active and inactive MRDs.
        """
        if self._closed:
            return
        self._closed = True

        for fut in self._pending.values():
            if not fut.done():
                fut.cancel()
        self._pending.clear()

        mrds_to_close = list(self._inactive.values()) + [
            mrd for mrd, _ in self._active.values()
        ]
        self._inactive.clear()
        self._active.clear()

        await _close_mrds(mrds_to_close, raise_exception=True)
