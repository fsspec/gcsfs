from .core import GCSFile
from fsspec import asyn
from google.cloud.storage._experimental.asyncio.async_multi_range_downloader import AsyncMultiRangeDownloader
from google.cloud.storage._experimental.asyncio.async_grpc_client import AsyncGrpcClient
from io import BytesIO

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
        self.mrd = asyn.sync(self.gcsfs.loop, self._get_downloader, self.bucket, self.key, self.generation)

    async def _get_downloader(self, bucket_name, object_name, generation=None):
        """
        Initializes the AsyncMultiRangeDownloader.
        """
        if self.gcsfs.grpc_client is None:
            self.gcsfs.grpc_client = AsyncGrpcClient().grpc_client

        downloader = await AsyncMultiRangeDownloader.create_mrd(
            self.gcsfs.grpc_client, bucket_name, object_name, generation
        )
        return downloader

    async def download_range(self,path,  start, end):
        """
        Downloads a byte range from the file asynchronously.
        """
        offset = start
        length = end - start + 1
        buffer = BytesIO()
        results = await self.mrd.download_ranges([(offset, length, buffer)])
        return buffer.getvalue()

    def _fetch_range(self, start, end):
        """
        Overrides the default _fetch_range to implement the gRPC read path.

        """        
        return asyn.sync(self.gcsfs.loop, self.download_range, self.path, start, end)
