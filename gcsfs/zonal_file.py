import logging

from fsspec import asyn
from google.cloud.storage._experimental.asyncio.async_multi_range_downloader import (
    AsyncMultiRangeDownloader,
)

from gcsfs import zb_hns_utils
from gcsfs.core import GCSFile

logger = logging.getLogger("gcsfs.zonal_file")

DEFAULT_BLOCK_SIZE = 5 * 2**20


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
        autocommit=False,
        *args,
        **kwargs,
    ):
        """
        Initializes the ZonalFile object.
        """
        super().__init__(gcsfs, path, mode, block_size, autocommit, *args, **kwargs)
        self.mrd = None
        if "r" in self.mode:
            self.mrd = asyn.sync(
                self.gcsfs.loop, self._init_mrd, self.bucket, self.key, self.generation
            )
        elif "w" in self.mode:
            self.aaow = asyn.sync(
                self.gcsfs.loop,
                zb_hns_utils.init_aaow,
                self.gcsfs.grpc_client,
                self.bucket,
                self.key,
            )
        else:
            raise NotImplementedError(
                "Only 'r' and 'w' modes are currently supported for Zonal buckets."
            )

    async def _init_mrd(self, bucket_name, object_name, generation=None):
        """
        Initializes the AsyncMultiRangeDownloader.
        """
        return await AsyncMultiRangeDownloader.create_mrd(
            self.gcsfs.grpc_client, bucket_name, object_name, generation
        )

    def _fetch_range(self, start, end):
        """
        Overrides the default _fetch_range to implement the gRPC read path.

        """
        try:
            return self.gcsfs.cat_file(self.path, start=start, end=end, mrd=self.mrd)
        except RuntimeError as e:
            if "not satisfiable" in str(e):
                return b""
            raise

    def write(self, data):
        """
        Writes data using AsyncAppendableObjectWriter.
        """
        if not self.writable():
            raise ValueError("File not in write mode")
        if self.closed:
            raise ValueError("I/O operation on closed file.")
        if self.forced:
            raise ValueError("This file has been force-flushed, can only close")

        asyn.sync(self.gcsfs.loop, self.aaow.append, data)

    def flush(self, force=False):
        """
        Flushes the AsyncAppendableObjectWriter, sending all buffered data
        to the server.
        """
        if self.closed:
            raise ValueError("Flush on closed file")
        if force and self.forced:
            raise ValueError("Force flush cannot be called more than once")
        if force:
            self.forced = True

        if self.readable():
            # no-op to flush on read-mode
            return

        asyn.sync(self.gcsfs.loop, self.aaow.flush)

    def commit(self):
        """
        Commits the write by finalizing the AsyncAppendableObjectWriter.
        """
        if not self.writable():
            raise ValueError("File not in write mode")
        self.autocommit = True
        asyn.sync(self.gcsfs.loop, self.aaow.finalize)

    def discard(self):
        """Discard is not applicable for Zonal Buckets. Log a warning instead."""
        logger.warning(
            "Discard is unavailable for Zonal Buckets. \
            Data is uploaded via streaming and cannot be cancelled."
        )

    async def initiate_upload(
        fs,
        bucket,
        key,
        content_type="application/octet-stream",
        metadata=None,
        fixed_key_metadata=None,
        mode="overwrite",
        kms_key_name=None,
    ):
        raise NotImplementedError(
            "Initiate_upload operation is not implemented yet for Zonal buckets. Please use write() instead."
        )

    async def simple_upload(
        fs,
        bucket,
        key,
        datain,
        metadatain=None,
        consistency=None,
        content_type="application/octet-stream",
        fixed_key_metadata=None,
        mode="overwrite",
        kms_key_name=None,
    ):
        raise NotImplementedError(
            "Simple_upload operation is not implemented yet for Zonal buckets. Please use write() instead."
        )

    async def upload_chunk(fs, location, data, offset, size, content_type):
        raise NotImplementedError(
            "Upload_chunk operation is not implemented yet for Zonal buckets. Please use write() instead."
        )

    def close(self):
        """
        Closes the ZonalFile and the underlying AsyncMultiRangeDownloader and AsyncAppendableObjectWriter.
        If in write mode, finalizes the write if autocommit is True.
        """
        if hasattr(self, "mrd") and self.mrd:
            asyn.sync(self.gcsfs.loop, self.mrd.close)
        if hasattr(self, "aaow") and self.aaow:
            asyn.sync(
                self.gcsfs.loop, self.aaow.close, finalize_on_close=self.autocommit
            )
        super().close()
