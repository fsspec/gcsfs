import logging
from io import BytesIO

from fsspec import asyn

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
        mrd=None,
        **kwargs,
    ):
        self.mrd = mrd
        super().__init__(
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
        sync_download_ranges = asyn.sync_wrapper(self.mrd.download_ranges)
        offset = start
        length = (end - start) + 1
        results = sync_download_ranges([(offset, length, buffer)])
        downloaded_data = buffer.getvalue()
        print(downloaded_data)
        return downloaded_data
