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

    def close(self):
        """
        Closes the ZonalFile and the underlying AsyncMultiRangeDownloader.
        """
        if self.mrd:
            asyn.sync(self.gcsfs.loop, self.mrd.close)
        super().close()
