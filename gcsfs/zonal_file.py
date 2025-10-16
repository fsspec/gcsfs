from io import BytesIO

from fsspec import asyn
from google.cloud.storage._experimental.asyncio.async_grpc_client import AsyncGrpcClient
from google.cloud.storage._experimental.asyncio.async_multi_range_downloader import AsyncMultiRangeDownloader

from .core import GCSFile


class ZonalFile(GCSFile):
    """
    GCSFile subclass designed to handle reads from
    Zonal buckets using a high-performance gRPC path.
    """

    def __init__(self, *args, **kwargs):
        """
        Initializes the ZonalFile object.
        """
        super().__init__(*args, **kwargs)
        self.mrd = asyn.sync(self.gcsfs.loop, self._init_mrd, self.bucket, self.key, self.generation)

    @classmethod
    async def _create_mrd(cls, grpc_client, bucket_name, object_name, generation=None):
        """
        Creates the AsyncMultiRangeDownloader.
        """
        mrd = await AsyncMultiRangeDownloader.create_mrd(
            grpc_client, bucket_name, object_name, generation
        )
        return mrd

    async def _init_mrd(self, bucket_name, object_name, generation=None):
        """
        Initializes the AsyncMultiRangeDownloader.
        """
        return await self._create_mrd(self.gcsfs.grpc_client, bucket_name, object_name, generation)

    @classmethod
    async def download_range(cls, offset, length, mrd):
        """
        Downloads a byte range from the file asynchronously.
        """
        buffer = BytesIO()
        await mrd.download_ranges([(offset, length, buffer)])
        return buffer.getvalue()

    def _fetch_range(self, start, end):
        """
        Overrides the default _fetch_range to implement the gRPC read path.

        """
        return self.gcsfs.cat_file(self.path, start=start, end=end, mrd=self.mrd)
