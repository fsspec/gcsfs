import logging
from enum import Enum

from fsspec import asyn
from google.cloud.storage._experimental.asyncio.async_grpc_client import AsyncGrpcClient

from . import zb_hns_utils
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
    BucketType.NON_HIERARCHICAL: GCSFile,
    BucketType.HIERARCHICAL: GCSFile,
    BucketType.UNKNOWN: GCSFile,
}


class GCSFileSystemAdapter(GCSFileSystem):
    """
    An subclass of GCSFileSystem that will contain specialized
    logic for ZB and HNS buckets.
    """

    def __init__(self, *args, **kwargs):
        kwargs.pop('experimental_zb_hns_support', None)
        super().__init__(*args, **kwargs)
        self.grpc_client = None
        self.grpc_client = asyn.sync(self.loop, self._create_grpc_client)
        self._storage_layout_cache = {}

    async def _create_grpc_client(self):
        if self.grpc_client is None:
            return AsyncGrpcClient().grpc_client
        else:
            return self.grpc_client

    async def _get_storage_layout(self, bucket):
        if bucket in self._storage_layout_cache:
            return self._storage_layout_cache[bucket]
        try:
            response = await self._call("GET", f"b/{bucket}/storageLayout", json_out=True)
            if response.get("locationType") == "zone":
                self._storage_layout_cache[bucket] = BucketType.ZONAL_HIERARCHICAL
            else:
                # This should be updated to include HNS in the future
                self._storage_layout_cache[bucket] = BucketType.NON_HIERARCHICAL
        except Exception as e:
            logger.error(f"Could not determine storage layout for bucket {bucket}: {e}")
            # Default to UNKNOWN
            self._storage_layout_cache[bucket] = BucketType.UNKNOWN
        return self._storage_layout_cache[bucket]

    _sync_get_storage_layout = asyn.sync_wrapper(_get_storage_layout)

    def _open(
            self,
            path,
            mode="rb",
            block_size=None,
            cache_options=None,
            acl=None,
            consistency=None,
            metadata=None,
            autocommit=True,
            fixed_key_metadata=None,
            generation=None,
            **kwargs,
    ):
        """
        Open a file.
        """
        bucket, _, _ = self.split_path(path)
        bucket_type = self._sync_get_storage_layout(bucket)
        return gcs_file_types[bucket_type](self, path,
            mode,
            block_size,
            cache_options=cache_options,
            consistency=consistency,
            metadata=metadata,
            acl=acl,
            autocommit=autocommit,
            fixed_key_metadata=fixed_key_metadata,
            **kwargs,)

    async def process_limits_to_offset_and_length(self, path, start, end):
        """
        Calculates the read offset and length from start and end parameters.

        Args:
            path (str): The path to the file.
            start (int | None): The starting byte position.
            end (int | None): The ending byte position.

        Returns:
            tuple: A tuple containing (offset, length).

        Raises:
            ValueError: If the calculated range is invalid.
        """
        size = None

        if start is None:
            offset = 0
        elif start < 0:
            size = size or (await self._info(path))["size"]
            offset = size + start
        else:
            offset = start

        if end is None:
            size = size or (await self._info(path))["size"]
            effective_end = size
        elif end < 0:
            size = size or (await self._info(path))["size"]
            effective_end = size + end
        else:
            effective_end = end

        size = size or (await self._info(path))["size"]
        if offset < 0:
            raise ValueError(f"Calculated start offset ({offset}) cannot be negative.")
        if effective_end < offset:
            raise ValueError(f"Calculated end position ({effective_end}) cannot be before start offset ({offset}).")
        elif effective_end == offset:
            length = 0  # Handle zero-length slice
        elif effective_end > size:
            length = size - offset  # Clamp length to file size
        else:
            length = effective_end - offset  # Normal case

        if offset + length > size:
            # This might happen with large positive end values
            length = max(0, size - offset)

        return offset, length

    async def is_zonal_bucket(self, bucket):
        layout = await self._get_storage_layout(bucket)
        return layout == BucketType.ZONAL_HIERARCHICAL

    async def _cat_file(self, path, start=None, end=None, **kwargs):
        """
        Fetch a file's contents as bytes.
        """
        mrd = kwargs.pop("mrd", None)
        if mrd is None:
            bucket, object_name, generation = self.split_path(path)
            # Fall back to default implementation if not a zonal bucket
            if not await self.is_zonal_bucket(bucket):
                return await super()._cat_file(path, start=start, end=end, **kwargs)

            mrd = await zb_hns_utils.create_mrd(self.grpc_client, bucket, object_name, generation)

        offset, length = await self.process_limits_to_offset_and_length(path, start, end)
        return await zb_hns_utils.download_range(offset=offset, length=length, mrd=mrd)
