import logging
from enum import Enum

from fsspec import asyn
from google.api_core import exceptions as api_exceptions
from google.api_core import gapic_v1
from google.api_core.client_info import ClientInfo
from google.cloud import storage_control_v2
from google.cloud.storage._experimental.asyncio.async_grpc_client import AsyncGrpcClient
from google.cloud.storage._experimental.asyncio.async_multi_range_downloader import (
    AsyncMultiRangeDownloader,
)

from gcsfs import __version__ as version
from gcsfs import zb_hns_utils
from gcsfs.core import GCSFile, GCSFileSystem
from gcsfs.zonal_file import ZonalFile

logger = logging.getLogger("gcsfs")

USER_AGENT = "python-gcsfs"


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


class ExtendedGcsFileSystem(GCSFileSystem):
    """
    This class will be used when GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT env variable is set to true.
    ExtendedGcsFileSystem is a subclass of GCSFileSystem that adds new logic for bucket types
    including zonal and hierarchical. For buckets without special properties, it forwards requests
    to the parent class GCSFileSystem for default processing.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.grpc_client = None
        self.storage_control_client = None
        # initializing grpc and storage control client for Hierarchical and
        # zonal bucket operations
        self.grpc_client = asyn.sync(self.loop, self._create_grpc_client)
        self._storage_control_client = asyn.sync(
            self.loop, self._create_control_plane_client
        )
        self._storage_layout_cache = {}

    async def _create_grpc_client(self):
        if self.grpc_client is None:
            return AsyncGrpcClient(
                client_info=ClientInfo(user_agent=f"{USER_AGENT}/{version}"),
            ).grpc_client
        else:
            return self.grpc_client

    async def _create_control_plane_client(self):
        # Initialize the storage control plane client for bucket
        # metadata operations
        client_info = gapic_v1.client_info.ClientInfo(
            user_agent=f"{USER_AGENT}/{version}"
        )
        return storage_control_v2.StorageControlAsyncClient(
            credentials=self.credentials.credentials, client_info=client_info
        )

    async def _lookup_bucket_type(self, bucket):
        if bucket in self._storage_layout_cache:
            return self._storage_layout_cache[bucket]
        bucket_type = await self._get_bucket_type(bucket)
        # Dont cache UNKNOWN type
        if bucket_type == BucketType.UNKNOWN:
            return BucketType.UNKNOWN
        self._storage_layout_cache[bucket] = bucket_type
        return self._storage_layout_cache[bucket]

    _sync_lookup_bucket_type = asyn.sync_wrapper(_lookup_bucket_type)

    async def _get_bucket_type(self, bucket):
        try:
            bucket_name_value = f"projects/_/buckets/{bucket}/storageLayout"
            response = await self._storage_control_client.get_storage_layout(
                name=bucket_name_value
            )

            if response.location_type == "zone":
                return BucketType.ZONAL_HIERARCHICAL
            else:
                # This should be updated to include HNS in the future
                return BucketType.NON_HIERARCHICAL
        except api_exceptions.NotFound:
            logger.warning(f"Error: Bucket {bucket} not found or you lack permissions.")
            return BucketType.UNKNOWN
        except Exception as e:
            logger.error(
                f"Could not determine bucket type for bucket name {bucket}: {e}"
            )
            # Default to UNKNOWN in case bucket type is not obtained
            return BucketType.UNKNOWN

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
        bucket_type = self._sync_lookup_bucket_type(bucket)
        return gcs_file_types[bucket_type](
            self,
            path,
            mode,
            block_size,
            cache_options=cache_options,
            consistency=consistency,
            metadata=metadata,
            acl=acl,
            autocommit=autocommit,
            fixed_key_metadata=fixed_key_metadata,
            generation=generation,
            **kwargs,
        )

    # Replacement method for _process_limits to support new params (offset and length) for MRD.
    async def _process_limits_to_offset_and_length(self, path, start, end):
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
            size = (await self._info(path))["size"] if size is None else size
            offset = size + start
        else:
            offset = start

        if end is None:
            size = (await self._info(path))["size"] if size is None else size
            effective_end = size
        elif end < 0:
            size = (await self._info(path))["size"] if size is None else size
            effective_end = size + end
        else:
            effective_end = end

        if offset < 0:
            raise ValueError(f"Calculated start offset ({offset}) cannot be negative.")
        if effective_end < offset:
            raise ValueError(
                f"Calculated end position ({effective_end}) cannot be before start offset ({offset})."
            )
        elif effective_end == offset:
            length = 0  # Handle zero-length slice
        else:
            length = effective_end - offset  # Normal case
            size = (await self._info(path))["size"] if size is None else size
            if effective_end > size:
                length = max(0, size - offset)  # Clamp and ensure non-negative

        return offset, length

    sync_process_limits_to_offset_and_length = asyn.sync_wrapper(
        _process_limits_to_offset_and_length
    )

    async def _is_zonal_bucket(self, bucket):
        bucket_type = await self._lookup_bucket_type(bucket)
        return bucket_type == BucketType.ZONAL_HIERARCHICAL

    async def _cat_file(self, path, start=None, end=None, mrd=None, **kwargs):
        """Fetch a file's contents as bytes, with an optimized path for Zonal buckets.

        This method overrides the parent `_cat_file` to read objects in Zonal buckets using gRPC.

        Args:
            path (str): The full GCS path to the file (e.g., "bucket/object").
            start (int, optional): The starting byte position to read from.
            end (int, optional): The ending byte position to read to.
            mrd (AsyncMultiRangeDownloader, optional): An existing multi-range
                downloader instance. If not provided, a new one will be created for Zonal buckets.

        Returns:
            bytes: The content of the file or file range.
        """
        mrd = kwargs.pop("mrd", None)
        mrd_created = False

        # A new MRD is required when read is done directly by the
        # GCSFilesystem class without creating a GCSFile object first.
        if mrd is None:
            bucket, object_name, generation = self.split_path(path)
            # Fall back to default implementation if not a zonal bucket
            if not await self._is_zonal_bucket(bucket):
                return await super()._cat_file(path, start=start, end=end, **kwargs)

            mrd = await AsyncMultiRangeDownloader.create_mrd(
                self.grpc_client, bucket, object_name, generation
            )
            mrd_created = True

        offset, length = await self._process_limits_to_offset_and_length(
            path, start, end
        )
        try:
            return await zb_hns_utils.download_range(
                offset=offset, length=length, mrd=mrd
            )
        finally:
            # Explicit cleanup if we created the MRD
            if mrd_created:
                await mrd.close()
