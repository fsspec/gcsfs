import logging
from enum import Enum

from fsspec import asyn
from google.cloud.storage._experimental.asyncio.async_grpc_client import AsyncGrpcClient

from .core import GCSFileSystem, GCSFile
from .zonal_file import ZonalFile

logger = logging.getLogger("gcsfs")


class BucketType(Enum):
    ZONAL_HIERARCHICAL = "ZONAL_HIERARCHICAL"
    HIERARCHICAL = "HIERARCHICAL"
    NON_HIERARCHICAL = "NON_HIERARCHICAL"
    UNKNOWN = "UNKNOWN"


gcs_file_types = {
    BucketType.ZONAL_HIERARCHICAL: ZonalFile,
    BucketType.UNKNOWN: GCSFile,
    None: GCSFile,
}


class GCSHNSFileSystem(GCSFileSystem):
    """
    An subclass of GCSFileSystem that will contain specialized
    logic for Zonal and HNS buckets.
    """

    def __init__(self, *args, **kwargs):
        kwargs.pop('experimental_zb_hns_support', None)
        super().__init__(*args, **kwargs)
        self.grpc_client = None
        self._storage_layout_cache = {}

    async def _get_storage_layout(self, bucket):
        if bucket in self._storage_layout_cache:
            return self._storage_layout_cache[bucket]
        try:
            response = await self._call("GET", f"b/{bucket}/storageLayout", json_out=True)
            if response.get("locationType") == "zone":
                bucket_type = BucketType.ZONAL_HIERARCHICAL
            else:
                # This should be updated to include HNS in the future
                bucket_type = BucketType.NON_HIERARCHICAL
            self._storage_layout_cache[bucket] = bucket_type
            return bucket_type
        except Exception as e:
            logger.error(f"Could not determine storage layout for bucket {bucket}: {e}")
            # Default to UNKNOWN
            self._storage_layout_cache[bucket] = BucketType.UNKNOWN
            return BucketType.UNKNOWN

    _sync_get_storage_layout = asyn.sync_wrapper(_get_storage_layout)

    def _open(
            self,
            path,
            mode="rb",
            **kwargs,
    ):
        """
        Open a file.
        """
        bucket, _, _ = self.split_path(path)
        bucket_type = self._sync_get_storage_layout(bucket)
        return gcs_file_types[bucket_type](gcsfs=self, path=path, mode=mode, **kwargs)

    def _process_limits(self, start, end):
        # Dummy method to process start and end
        if start is None:
            start = 0
        if end is None:
            end = 100
        return start, end - start + 1

    async def _cat_file(self, path, start=None, end=None, **kwargs):
        """
        Fetch a file's contents as bytes.
        """
        mrd = kwargs.pop("mrd", None)
        if mrd is None:
            if self.grpc_client is None:
                self.grpc_client = AsyncGrpcClient().grpc_client
            bucket, object_name, generation = self.split_path(path)
            mrd = await ZonalFile._create_mrd(self.grpc_client, bucket, object_name, generation)

        offset, length = self._process_limits(start, end)
        return await ZonalFile.download_range(offset=offset, length=length, mrd=mrd)
