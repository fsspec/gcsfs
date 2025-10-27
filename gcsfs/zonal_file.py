from fsspec import asyn

from . import zb_hns_utils
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

    async def _init_mrd(self, bucket_name, object_name, generation=None):
        """
        Initializes the AsyncMultiRangeDownloader.
        """
        return await zb_hns_utils.create_mrd(self.gcsfs.grpc_client, bucket_name, object_name, generation)

    def _fetch_range(self, start, end):
        """
        Overrides the default _fetch_range to implement the gRPC read path.

        """
        return self.gcsfs.cat_file(self.path, start=start, end=end, mrd=self.mrd)
