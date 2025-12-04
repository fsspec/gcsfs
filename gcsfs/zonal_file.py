import logging

from fsspec import asyn
from google.cloud.storage._experimental.asyncio.async_multi_range_downloader import (
    AsyncMultiRangeDownloader,
)

from gcsfs import zb_hns_utils
from gcsfs.core import GCSFile

logger = logging.getLogger("gcsfs.zonal_file")


class ZonalFile(GCSFile):
    """
    ZonalFile is subclass of GCSFile and handles data operations from
    Zonal buckets only using a high-performance gRPC path.
    """

    def __init__(self, *args, **kwargs):
        """
        Initializes the ZonalFile object.
        """
        super().__init__(*args, **kwargs)
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
                "Only read operations are currently supported for Zonal buckets."
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

        For more details, see the documentation for AsyncAppendableObjectWriter:
        https://github.com/googleapis/python-storage/blob/9e6fefdc24a12a9189f7119bc9119e84a061842f/google/cloud/storage/_experimental/asyncio/async_appendable_object_writer.py#L38
        """
        if self.closed:
            raise ValueError("I/O operation on closed file.")
        if not self.writable():
            raise ValueError("File not in write mode")
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
        )

    def _upload_chunk(self, final=False):
        raise NotImplementedError(
            "_upload_chunk is not implemented yet for ZonalFile. Please use write() instead."
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
