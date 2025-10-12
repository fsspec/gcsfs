import asyncio
import logging
import threading
from io import BytesIO

from fsspec import asyn
from google.cloud.storage._experimental.asyncio.async_multi_range_downloader import \
    AsyncMultiRangeDownloader

from .core import GCSFile

logger = logging.getLogger("gcsfs")


class GCSZonalFile(GCSFile):
    """
    A GCSFile subclass for Zonal Storage buckets.

    This may have custom logic for operations like fetching data ranges.
    """

    def __init__(
        self,
        gcsfs,
        path,
        mode="rb",
        block_size=None,
        autocommit=True,
        cache_type="readahead",
        cache_options=None,
        acl=None,
        consistency="md5",
        metadata=None,
        content_type=None,
        timeout=None,
        fixed_key_metadata=None,
        generation=None,
        kms_key_name=None,
        client=None,
        **kwargs,
    ):
        self.gcsfs = gcsfs
        bucket_name, name, _ = self.gcsfs.split_path(path)
        self.mrd = AsyncMultiRangeDownloader(client, bucket_name, name)
        self._run_async_in_new_loop(self.mrd.open())
        GCSFile.__init__(
            self,
            gcsfs,
            path,
            mode=mode,
            block_size=block_size,
            autocommit=autocommit,
            cache_type=cache_type,
            cache_options=cache_options,
            acl=acl,
            consistency=consistency,
            metadata=metadata,
            content_type=content_type,
            timeout=timeout,
            fixed_key_metadata=fixed_key_metadata,
            generation=generation,
            kms_key_name=kms_key_name,
            **kwargs,
        )

    def _fetch_range(self, start=None, end=None):
        """
        Get data from a Zonal Storage bucket.

        This method would contain custom logic optimized for zonal reads.
        """
        # Placeholder for custom zonal fetch logic.
        logger.info("--- GCSZonalFile._fetch_range is called ---")
        buffer = BytesIO()
        offset = start
        length = (end - start) + 1
        self._run_async_in_new_loop(self.mrd.download_ranges([(offset, length, buffer)]))
        downloaded_data = buffer.getvalue()
        print(downloaded_data)
        return downloaded_data

    def _run_async_in_new_loop(self, coro):
        """
        Runs a coroutine in a new event loop in a separate thread.
        This is done to avoid any deadlocks when calling async code from a
        sync method that is itself running inside an outer event loop.
        """
        result = None
        exception = None

        def runner():
            nonlocal result, exception
            try:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                result = asyncio.run_coroutine_threadsafe(coro, loop)
                loop.close()
            except Exception as e:
                exception = e

        thread = threading.Thread(target=runner)
        thread.start()
        thread.join()

        if exception:
            raise exception
        return result
