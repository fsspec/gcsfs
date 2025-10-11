from .core import GCSFile
from fsspec import asyn
from google.cloud.storage._experimental.asyncio.async_multi_range_downloader import AsyncMultiRangeDownloader
from google.cloud.storage._experimental.asyncio.async_grpc_client import AsyncGrpcClient
from io import BytesIO

class ZonalHandler():
    """
    Handler for Zonal bucket operations.
    """

    def __init__(self, gcsfilesystemadapter, **kwargs):
        """
        Initializes the ZonalFile object.
        """
        self.fs = gcsfilesystemadapter
        self.mrd_cache = {}

    async def _get_downloader(self, bucket, object, generation=None):
        """
        Initializes the AsyncMultiRangeDownloader.
        """
        if self.fs.grpc_client is None:
            self.fs.grpc_client = AsyncGrpcClient().grpc_client

        if self.mrd_cache.get((bucket, object, generation)):
            return self.mrd_cache[(bucket, object, generation)]

        downloader = await AsyncMultiRangeDownloader.create_mrd(
            self.fs.grpc_client, bucket, object, generation
        )
        self.mrd_cache[(bucket, object, generation)] = downloader
        return downloader

    async def download_range(self,path,  start, end):
        """
        Downloads a byte range from the file asynchronously.
        """
        bucket, object, generation = self.fs.split_path(path)
        mrd = await self._get_downloader(bucket, object, generation)
        offset = start
        length = end - start + 1
        buffer = BytesIO()
        results = await mrd.download_ranges([(offset, length, buffer)])
        return buffer.getvalue()
    