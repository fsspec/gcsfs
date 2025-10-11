import logging
from enum import Enum

from fsspec import asyn
from google.cloud.storage._experimental.asyncio.async_multi_range_downloader import AsyncMultiRangeDownloader

from .core import GCSFileSystem, GCSFile
from google.cloud.storage._experimental.asyncio.async_grpc_client import (AsyncGrpcClient)

from .gcs_adapter import GCSAdapter
from .zonalfile import ZonalFile

logger = logging.getLogger("gcsfs")

class BucketType(Enum):
    ZONAL_HIERARCHICAL = "ZONAL_HIERARCHICAL"
    HIERARCHICAL = "HIERARCHICAL"
    NON_HIERARCHICAL = "NON_HIERARCHICAL"
    UNKNOWN = "UNKNOWN"

gcs_file_types = {
    BucketType.ZONAL_HIERARCHICAL: ZonalFile,
    BucketType.UNKNOWN : GCSFile,
}

class ZonalFileSystem(GCSFileSystem):
    """
    An experimental subclass of GCSFileSystem that will contain specialized
    logic for Zonal and HNS buckets.
    """
    def __init__(self, *args, **kwargs):
        # Ensure the experimental flag is not passed down again
        kwargs.pop('experimental_zb_hns_support', None)
        super().__init__(*args, **kwargs)
        self.grpc_client = AsyncGrpcClient().grpc_client
        self._storage_layout_cache = {}
        print("ZonalFileSystem initialized")

    async def _get_storage_layout(self, bucket):
        print("Getting storage layout for bucket {}".format(bucket))
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
            print("Getting storage layout for bucket {}".format(bucket))
            return bucket_type
        except Exception as e:
            logger.error(f"Could not determine storage layout for bucket {bucket}: {e}")
            # Default to UNKNOWN
            self._storage_layout_cache[bucket] = BucketType.UNKNOWN
            return BucketType.UNKNOWN

    _sync_get_storage_layout = asyn.sync_wrapper(_get_storage_layout)

    async def _init_async_multi_range_reader(self, client):
        """Initializes the reader if the bucket is Zonal [4]."""

        print("Initializing AsyncMultiRangeReader")
        async_multi_range_reader = await AsyncMultiRangeDownloader.create_mrd(
            client, bucket_name="chandrasiri-rs", object_name="sunidhi"
        )
        print("Initialized AsyncMultiRangeReader")
        # return async_multi_range_reader
        return client

    init_mrd = asyn.sync_wrapper(_init_async_multi_range_reader)
    def _open(
            self,
            path,
            mode="rb",
            **kwargs,
    ):
        """
        Open a file.
        """
        print(f"ZonalFileSystem._open() called for path: {path}")
        bucket, _, _ = self.split_path(path)
        print(f"Bucket: {bucket}")
        try:
            bucket_type = self._sync_get_storage_layout(bucket)
            if bucket_type == BucketType.ZONAL_HIERARCHICAL:
                print("Creating mrd {}".format(bucket))
                # mrd = self.init_mrd(self.grpc_client)
                print("Opening ZonalFile")
                # return ZonalFile(path=path, bucket=bucket, mrd=mrd, **kwargs)
        except Exception as e:
            logger.warning(
                    f"Failed to get storage layout for bucket {bucket}: {e}"
            )
        return super()._open(path, mode, **kwargs)

    def _fetch_range(self, start=None, end=None):
        """Get data from GCS
        start, end : None or integers
            if not both None, fetch only given range
        """
        print("ZonalFileSystem._fetch_range() called")

    async def _cat_file(self, path, start=None, end=None, **kwargs):
        """Simple one-shot get of file data"""
        print("ZonalFileSystem._cat_file() called")
        client = AsyncGrpcClient().grpc_client
        mrd = await AsyncMultiRangeDownloader.create_mrd(
            client, bucket_name="chandrasiri-rs", object_name="sunidhi"
        )
        return b'This is an output'

    # def _upl