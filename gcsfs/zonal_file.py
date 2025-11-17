from fsspec import asyn
from google.cloud.storage._experimental.asyncio.async_multi_range_downloader import (
    AsyncMultiRangeDownloader,
)

from gcsfs.core import GCSFile


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
                self.gcsfs.loop, self._init_aaow, self.bucket, self.key, self.generation
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

    async def _init_aaow(
        self, bucket_name, object_name, generation=None, overwrite=True
    ):
        """
        Initializes the AsyncAppendableObjectWriter.
        """
        if generation is None and await self.gcsfs._exists(self.path):
            info = self.gcsfs.info(self.path)
            generation = info.get("generation")

        return await zb_hns_utils.init_aaow(
            self.gcsfs.grpc_client, bucket_name, object_name, generation, overwrite
        )

    def flush(self, force=False):
        raise NotImplementedError(
            "Write operations are not yet implemented for Zonal buckets."
        )

    def commit(self):
        raise NotImplementedError(
            "Write operations are not yet implemented for Zonal buckets."
        )

    def discard(self):
        raise NotImplementedError(
            "Write operations are not yet implemented for Zonal buckets."
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
            "Write operations are not yet implemented for Zonal buckets."
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
            "Write operations are not yet implemented for Zonal buckets."
        )

    async def upload_chunk(fs, location, data, offset, size, content_type):
        raise NotImplementedError(
            "Write operations are not yet implemented for Zonal buckets."
        )

    def close(self):
        """
        Closes the ZonalFile and the underlying AsyncMultiRangeDownloader.
        """
        if self.mrd:
            asyn.sync(self.gcsfs.loop, self.mrd.close)
        if self.aaow:
            asyn.sync(self.gcsfs.loop, self.aaow.close)
        super().close()
