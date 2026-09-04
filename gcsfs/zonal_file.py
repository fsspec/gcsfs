import logging
import sys
import threading

from fsspec import asyn
from google.cloud.storage.asyncio.async_appendable_object_writer import (
    _DEFAULT_FLUSH_INTERVAL_BYTES,
)

from gcsfs import zb_hns_utils
from gcsfs.core import (
    DEFAULT_BLOCK_SIZE,
    GCSFile,
    _coalesce_generation,
    _get_prefetcher_and_cache_config,
    _on_loop_thread,
)

from .caching import (  # noqa: F401 Unused import to register GCS-Specific caches, Please do not remove it.
    ReadAheadChunked,
)

logger = logging.getLogger("gcsfs.zonal_file")

# Strong references for background tasks scheduled via loop.create_task().
# Without holding external references, Python's asyncio event loop may allow
# pending tasks to be garbage-collected mid-execution ("Task was destroyed but
# it is pending").
_deferred_close_tasks = set()
_deferred_close_lock = threading.Lock()


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
                    exc_info=exc,
                )

    task.add_done_callback(_on_done)
    return task


# Default timeout for synchronous teardowns when the file has no timeout set.
_DEFAULT_TEARDOWN_TIMEOUT_SECONDS = 60.0


class ZonalFile(GCSFile):
    """
    ZonalFile is subclass of GCSFile and handles data operations from
    Zonal buckets only using a high-performance gRPC path.
    """

    def __init__(
        self,
        gcsfs,
        path,
        mode="rb",
        block_size=DEFAULT_BLOCK_SIZE,
        autocommit=True,
        cache_type=None,
        cache_options=None,
        acl=None,
        consistency="md5",
        metadata=None,
        content_type=None,
        timeout=None,
        fixed_key_metadata=None,
        generation=None,
        kms_key_name=None,
        pool_size=zb_hns_utils.DEFAULT_CONCURRENCY,
        finalize_on_close=False,
        flush_interval_bytes=_DEFAULT_FLUSH_INTERVAL_BYTES,
        **kwargs,
    ):
        """
        Initializes the ZonalFile object.

        For Zonal buckets, `finalize_on_close` is set to `False` by default to optimize
        for write throughput and keep the file appendable. This means that when exiting
        a `with` block or closing, the file will not be automatically finalized. To
        ensure the write is finalized, `.commit()` must be called explicitly or
        `finalize_on_close` must be set to `True` when opening the file.

        For Zonal buckets, `flush_interval_bytes` controls the write buffer size before
        persisting data to GCS (default: 16 MiB). This value must be a multiple
        of `_MAX_CHUNK_SIZE_BYTES` (2 MiB). Note that this higher default value may
        increase memory usage.
        """
        bucket, key, path_generation = gcsfs.split_path(path)
        generation = _coalesce_generation(generation, path_generation)
        if not key:
            raise OSError("Attempt to open a bucket")
        self.aaow = None
        self.finalize_on_close = finalize_on_close
        self.finalized = False
        self.mode = mode
        self.flush_interval_bytes = flush_interval_bytes
        self.gcsfs = gcsfs
        self.pool_size = pool_size
        object_size = None
        if "r" in self.mode:
            resolved_cache_type, _, resolved_cache_source = (
                _get_prefetcher_and_cache_config(cache_type, kwargs)
            )
            self.mrd_pool = asyn.sync(
                self.gcsfs.loop,
                self.gcsfs._mrd_pool_cache.get,
                bucket,
                key,
                generation,
                pool_size=self.pool_size,
                cache_type=resolved_cache_type,
                cache_source=resolved_cache_source,
            )
            if getattr(self.mrd_pool, "details", None) is not None:
                self._details = self.mrd_pool.details
            object_size = self.mrd_pool.persisted_size

            if object_size is None:
                logger.warning(
                    "AsyncMultiRangeDownloader (MRD) exists but has no 'persisted_size'. "
                    "This may result in incorrect behavior for unfinalized objects."
                )
        elif "w" in self.mode or "a" in self.mode:
            pass
        else:
            raise NotImplementedError(
                "Only read, write and append operations are currently supported for Zonal buckets."
            )

        super().__init__(
            gcsfs,
            path,
            mode,
            block_size,
            autocommit,
            cache_type,
            cache_options,
            acl,
            consistency,
            metadata,
            content_type,
            timeout,
            fixed_key_metadata,
            generation,
            kms_key_name,
            # Zonal buckets support append; this prevents GCSFile from forcing 'w' mode
            _supports_append="a" in mode,
            # pass persisted_size here so that Cache is initialized with correct object size
            size=object_size,
            **kwargs,
        )

    async def _init_mrd(self, bucket_name, object_name, generation=None):
        """
        Initializes the AsyncMultiRangeDownloader.
        """
        await self.gcsfs._get_grpc_client()
        return await zb_hns_utils.init_mrd(
            self.gcsfs.grpc_client, bucket_name, object_name, generation
        )

    async def _init_aaow(
        self, bucket_name, object_name, generation=None, flush_interval_bytes=None
    ):
        """
        Initializes the AsyncAppendableObjectWriter.
        """
        # generation is needed while creating aaow to append to existing objects
        if "a" in self.mode and generation is None:
            try:
                # self.path might not be set yet, so reconstruct full path
                info = await self.gcsfs._info(f"{bucket_name}/{object_name}")
                generation = info.get("generation")
            except FileNotFoundError:
                # if file doesn't exist, we don't need generation
                pass
        await self.gcsfs._get_grpc_client()
        return await zb_hns_utils.init_aaow(
            self.gcsfs.grpc_client,
            bucket_name,
            object_name,
            generation,
            flush_interval_bytes,
        )

    def _ensure_aaow(self):
        if self.aaow is None:
            self.aaow = asyn.sync(
                self.gcsfs.loop,
                self._init_aaow,
                self.bucket,
                self.key,
                self.generation,
                self.flush_interval_bytes,
            )

    def _fetch_range(
        self,
        start: int | None = None,
        end: int | None = None,
        chunk_lengths: list[int] | None = None,
    ):
        """
        Overrides the default _fetch_range to implement the gRPC read path.

        Args:
            start: The start offset for requested bytes (included).
            end: The end offset for requested bytes (excluded).
            chunk_lengths: A list of integers specifying the sizes of sequential chunks to read
                starting from the start offset. This cannot be used at the same time as the end parameter.

        Returns:
            A single bytes object if chunk_lengths is None, or a list of bytes objects corresponding
            to the requested chunk sizes. If the range cannot be satisfied, it returns empty bytes
            or a list with empty bytes.

        Raises:
            ValueError: If both end and chunk_lengths are provided.
            RuntimeError: If an underlying fetch operation fails for an unexpected reason.
        """
        if end is not None and chunk_lengths is not None:
            raise ValueError(
                "The end and chunk_lengths arguments are mutually exclusive and cannot be used together."
            )

        if self._prefetch_engine:
            # This block is basically where caches and prefetch engines may overlap.
            # We plan to remove this behaviour in future.

            try:
                if chunk_lengths is None:
                    return self._prefetch_engine.fetch(start, end)

                # Fetch chunks sequentially through the prefetch engine
                # Spawning concurrent task is worst here, because that would act as seek for prefetcher.
                results = []
                current_offset = start if start is not None else 0
                for length in chunk_lengths:
                    data = self._prefetch_engine.fetch(
                        current_offset, current_offset + length
                    )
                    results.append(data)
                    current_offset += length
                    if length != len(data):
                        raise RuntimeError("not satisfiable")
                return results
            except RuntimeError as e:
                if "not satisfiable" in str(e):
                    return b"" if chunk_lengths is None else [b""]
                raise

        # non-prefetch route
        async def _do_fetch():
            if chunk_lengths is not None:
                return await self.gcsfs._fetch_range_split(
                    self.path,
                    concurrency=self.concurrency,
                    start=start,
                    chunk_lengths=chunk_lengths,
                    size=self.size,
                    mrd=self.mrd_pool,
                    cache_type=self.cache_type,
                    cache_source=self.cache_source,
                )

            return await self.gcsfs._cat_file(
                self.path,
                start=start,
                end=end,
                concurrency=self.concurrency,
                mrd=self.mrd_pool,
                cache_type=self.cache_type,
                cache_source=self.cache_source,
            )

        try:
            return asyn.sync(self.fs.loop, _do_fetch)
        except RuntimeError as e:
            if "not satisfiable" in str(e):
                return b"" if chunk_lengths is None else [b""]
            raise

    async def _async_fetch_range(self, start_offset, total_size, split_factor=1):
        """The native coroutine called by the BackgroundPrefetcher."""
        return await self.gcsfs._concurrent_mrd_fetch(
            start_offset, total_size, split_factor, self.mrd_pool
        )

    def write(self, data):
        """
        Writes data using AsyncAppendableObjectWriter.

        Unlike standard GCSFile which buffers writes in an in-memory buffer before
        uploading chunks, ZonalFile does not require an internal write buffer here.
        The underlying AsyncAppendableObjectWriter manages its own internal buffering,
        streaming, and chunk flushes. Data is passed directly to `self.aaow.append()`,
        avoiding redundant memory copies.

        For more details, see the documentation for AsyncAppendableObjectWriter:
        https://github.com/googleapis/python-storage/blob/9e6fefdc24a12a9189f7119bc9119e84a061842f/google/cloud/storage/_experimental/asyncio/async_appendable_object_writer.py#L38
        """
        if self.closed:
            raise ValueError("I/O operation on closed file.")
        if not self.writable():
            raise ValueError("File not in write mode.")
        if self.forced:
            raise ValueError("This file has been force-flushed, can only close")

        # Lazily initialize the AsyncAppendableObjectWriter on the first write to avoid
        # unnecessary object creation for files that are opened but never written to.
        self._ensure_aaow()
        asyn.sync(self.gcsfs.loop, self.aaow.append, data)
        bytes_written = len(data)
        self.loc += bytes_written
        return bytes_written

    def flush(self, force=False):
        """
        Flushes the AsyncAppendableObjectWriter, sending all buffered data
        to the server.
        """
        if self.closed:
            raise ValueError("Flush on closed file.")
        if force and self.forced:
            raise ValueError("Force flush cannot be called more than once.")
        if self.finalized:
            logger.warning("File is already finalized. Ignoring flush call.")
            return
        if force:
            self.forced = True

        if self.readable():
            # no-op to flush on read-mode
            return

        # Case 1: Intermediate flush (force=False)
        # If no data has been written (aaow is None), there is nothing to flush.
        if self.aaow is None and not force:
            return

        # Case 2: Closing flush (force=True) or some data has been written (AAOW exists)
        # We must ensure aaow exists so that the file is created even for empty writes,
        # and to flush any buffered data if it exists.
        self._ensure_aaow()

        asyn.sync(self.gcsfs.loop, self.aaow.flush)

    def commit(self):
        """
        Commits the write by finalizing the AsyncAppendableObjectWriter.
        """
        if not self.writable():  # No-op
            logger.warning("File not in write mode. Ignoring commit call.")
            return
        if self.finalized:  # No-op
            logger.warning(
                "This file has already been finalized. Ignoring commit call."
            )
            return

        self._ensure_aaow()
        asyn.sync(self.gcsfs.loop, self.aaow.finalize)
        self.finalized = True
        # File is already finalized, avoid finalizing again on close
        self.finalize_on_close = False

    def discard(self):
        """Discard is not applicable for Zonal Buckets. Log a warning instead."""
        logger.warning(
            "Discard is not applicable for Zonal Buckets. \
            Data is uploaded via streaming and cannot be cancelled."
        )

    def _initiate_upload(self):
        """Initiates the upload for Zonal buckets using gRPC."""
        from gcsfs.extended_gcsfs import initiate_upload

        self.location = asyn.sync(
            self.gcsfs.loop,
            initiate_upload,
            self.gcsfs,
            self.bucket,
            self.key,
            self.content_type,
            self.metadata,
            self.fixed_key_metadata,
            mode="create" if "x" in self.mode else "overwrite",
            kms_key_name=self.kms_key_name,
            timeout=self.timeout,
        )

    def _simple_upload(self):
        """Performs a simple upload for Zonal buckets using gRPC."""
        from gcsfs.extended_gcsfs import simple_upload

        self.buffer.seek(0)
        data = self.buffer.read()
        asyn.sync(
            self.gcsfs.loop,
            simple_upload,
            self.gcsfs,
            self.bucket,
            self.key,
            data,
            self.metadata,
            self.consistency,
            self.content_type,
            self.fixed_key_metadata,
            mode="create" if "x" in self.mode else "overwrite",
            kms_key_name=self.kms_key_name,
            timeout=self.timeout,
            finalize_on_close=self.finalize_on_close,
        )

    def _upload_chunk(self, final=False):
        raise NotImplementedError(
            "_upload_chunk is not implemented yet for ZonalFile. Please use write() instead."
        )

    def _close_impl(self):
        super()._close_impl()

        loop = getattr(getattr(self, "gcsfs", None), "loop", None)
        errors = []

        # Teardown the read-side MRD pool if initialized.
        if hasattr(self, "mrd_pool") and self.mrd_pool:
            try:
                self._sync_teardown(
                    loop, self.mrd_pool.close, description="closing mrd_pool"
                )
            except Exception as e:
                errors.append(e)

        # Finalize and close the write-side AAOW stream.
        # Wrapped independently so a read pool failure does not abandon write finalization.
        if getattr(self, "aaow", None) and self.aaow._is_stream_open:
            try:
                self._sync_teardown(
                    loop,
                    zb_hns_utils.close_aaow,
                    self.aaow,
                    finalize_on_close=self.finalize_on_close,
                    description="finalizing AsyncAppendableObjectWriter",
                )
            except Exception as e:
                errors.append(e)

        if errors:
            raise errors[0]

    def _sync_teardown(self, loop, func, *args, description, **kwargs):
        """Runs an async teardown coroutine on ``loop`` from synchronous context.

        Unlike purely read-side caching, write-side teardown finalizes appendable
        objects server-side. Branches that cannot safely run the teardown coroutine
        log warnings or errors to provide visibility into potential unfinalized resources.
        """
        path = getattr(self, "path", "<unknown>")

        if sys.is_finalizing():
            return

        if loop is None or not loop.is_running():
            # If the event loop is absent, closed, or not actively running,
            # synchronous coordination via asyn.sync() is impossible.
            logger.error(
                "Skipping %s for %s: no usable IO loop available. This may "
                "leave server-side resources unfinalized.",
                description,
                path,
            )
            return

        if _on_loop_thread(loop):
            # Avoid deadlock when closing reentrantly from the event loop thread.
            _defer_task(
                loop,
                func(*args, **kwargs),
                description=f"{description} for {path}",
                logger=logger,
                log_level=logging.ERROR,
            )
            return

        # Bound synchronous teardown so hung network calls do not stall the deferred-close thread forever.
        teardown_timeout = (
            getattr(self, "timeout", None) or _DEFAULT_TEARDOWN_TIMEOUT_SECONDS
        )
        try:
            asyn.sync(loop, func, *args, timeout=teardown_timeout, **kwargs)
        except asyn.FSTimeoutError:
            logger.error(
                "%s for %s did not complete within %ss; the upload may be "
                "left unfinalized.",
                description,
                path,
                teardown_timeout,
            )
            raise
