import logging
from enum import Enum

from fsspec import asyn
from google.api_core import exceptions as api_exceptions
from google.api_core import gapic_v1
from google.api_core.client_info import ClientInfo
from google.cloud import storage_control_v2
from google.cloud.storage._experimental.asyncio.async_grpc_client import AsyncGrpcClient

from . import __version__ as version
from . import zb_hns_utils
from .core import GCSFile, GCSFileSystem
from .zonal_file import ZonalFile

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


class GCSFileSystemAdapter(GCSFileSystem):
    """
    This class will be used when experimental_zb_hns_support is set to true for all bucket types.
    GCSFileSystemAdapter is a subclass of GCSFileSystem that adds specialized
    logic to support Zonal and Hierarchical buckets.
    """

    def __init__(self, *args, **kwargs):
        kwargs.pop("experimental_zb_hns_support", None)
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

    async def _get_storage_layout(self, bucket):
        if bucket in self._storage_layout_cache:
            return self._storage_layout_cache[bucket]
        try:

            # Bucket name details
            bucket_name_value = f"projects/_/buckets/{bucket}/storageLayout"
            # Make the request to get bucket type
            response = await self._storage_control_client.get_storage_layout(
                name=bucket_name_value
            )

            if response.location_type == "zone":
                self._storage_layout_cache[bucket] = BucketType.ZONAL_HIERARCHICAL
            else:
                # This should be updated to include HNS in the future
                self._storage_layout_cache[bucket] = BucketType.NON_HIERARCHICAL
        except api_exceptions.NotFound:
            print(f"Error: Bucket {bucket} not found or you lack permissions.")
            return None
        except Exception as e:
            logger.error(
                f"Could not determine bucket type for bucket name {bucket}: {e}"
            )
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
            raise ValueError(
                f"Calculated end position ({effective_end}) cannot be before start offset ({offset})."
            )
        elif effective_end == offset:
            length = 0  # Handle zero-length slice
        elif effective_end > size:
            length = max(0, size - offset)  # Clamp and ensure non-negative
        else:
            length = effective_end - offset  # Normal case

        return offset, length

    sync_process_limits_to_offset_and_length = asyn.sync_wrapper(
        _process_limits_to_offset_and_length
    )

    async def _is_zonal_bucket(self, bucket):
        layout = await self._get_storage_layout(bucket)
        return layout == BucketType.ZONAL_HIERARCHICAL

    async def _cat_file(self, path, start=None, end=None, **kwargs):
        """
        Fetch a file's contents as bytes.
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

            mrd = await zb_hns_utils.create_mrd(
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
            # Explicit cleanup if we created the MRD and it has a close method
            if mrd_created:
                await mrd.close()
