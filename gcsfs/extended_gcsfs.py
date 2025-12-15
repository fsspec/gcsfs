import logging
from enum import Enum
from functools import partial

from fsspec import asyn
from google.api_core import exceptions as api_exceptions
from google.api_core import gapic_v1
from google.api_core.client_info import ClientInfo
from google.auth.credentials import AnonymousCredentials
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
        # Adds user-passed credentials to ExtendedGcsFileSystem to pass to gRPC/Storage Control clients.
        # We unwrap the nested credentials here because self.credentials is a GCSFS wrapper,
        # but the clients expect the underlying google.auth credentials object.
        self.credential = self.credentials.credentials
        # When token="anon", self.credentials.credentials is None. This is
        # often used for testing with emulators. However, the gRPC and storage
        # control clients require a credentials object for initialization.
        # We explicitly use AnonymousCredentials() to allow unauthenticated access.
        if self.credentials.token == "anon":
            self.credential = AnonymousCredentials()
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
                credentials=self.credential,
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
        # Remove with_quota_project parameter once b/442805436 is fixed.
        creds = self.credential
        if hasattr(creds, "with_quota_project"):
            creds = creds.with_quota_project(None)

        return storage_control_v2.StorageControlAsyncClient(
            credentials=creds, client_info=client_info
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
            elif (
                response.hierarchical_namespace
                and response.hierarchical_namespace.enabled
            ):
                return BucketType.HIERARCHICAL
            else:
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
            block_size=block_size or self.default_block_size,
            cache_options=cache_options,
            consistency=consistency or self.consistency,
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

    async def _is_bucket_hns_enabled(self, bucket):
        """Checks if a bucket has Hierarchical Namespace enabled."""
        bucket_type = await self._lookup_bucket_type(bucket)
        return bucket_type in [BucketType.ZONAL_HIERARCHICAL, BucketType.HIERARCHICAL]

    # The rename method in fsspec's AbstractFileSystem calls self.mv, which in turn calls self._mv.
    # By overriding _mv, we ensure that all rename/move operations go through this HNS-aware logic.
    async def _mv(self, path1, path2, **kwargs):
        if path1 == path2:
            logger.debug(
                "%s mv: The paths are the same, so no files/directories were moved.",
                self,
            )
            return

        bucket1, key1, _ = self.split_path(path1)
        bucket2, key2, _ = self.split_path(path2)

        print("in hns aware mv implementation", path1, path2)

        try:
            is_hns = await self._is_bucket_hns_enabled(bucket1)
            info1 = await self._info(path1)
            is_folder = info1.get("type") == "directory"

            # We only use HNS rename if it's an HNS bucket, the source is a folder,
            # and the move is within the same bucket.
            if is_hns and is_folder and bucket1 == bucket2 and key1 and key2:
                logger.info(
                    f"Using HNS-aware folder rename for '{path1}' to '{path2}'."
                )
                source_folder_name = f"projects/_/buckets/{bucket1}/folders/{key1}"
                destination_folder_id = key2

                request = storage_control_v2.RenameFolderRequest(
                    name=source_folder_name,
                    destination_folder_id=destination_folder_id,
                )

                await self._storage_control_client.rename_folder(request=request)
                self.invalidate_cache(path1)
                self.invalidate_cache(path2)
                self.invalidate_cache(self._parent(path1))
                self.invalidate_cache(self._parent(path2))
                print("successfully renamed folder")
                return

        except api_exceptions.NotFound as e:
            raise FileNotFoundError(
                f"Source folder '{path1}' not found for HNS rename."
            ) from e
        except api_exceptions.Conflict as e:
            # This occurs if the destination folder already exists.
            logger.error(
                f"HNS rename failed because destination '{path2}' already exists: {e}"
            )
            # Raise FileExistsError for fsspec compatibility.
            raise FileExistsError(
                f"HNS rename failed: Destination already exists: {path2}"
            ) from e
        except api_exceptions.FailedPrecondition as e:
            logger.error(
                f"HNS rename failed due to precondition for '{path1}' to '{path2}': {e}"
            )
            raise OSError(f"HNS rename failed: {e}") from e
        except Exception as e:
            logger.warning(f"Could not perform HNS-aware mv: {e}")

        logger.debug(f"Falling back to object-level mv for '{path1}' to '{path2}'.")
        # Use functools.partial to pass kwargs to super().mv inside the executor
        loop = self.loop
        func = partial(super().mv, path1, path2, **kwargs)
        return await loop.run_in_executor(None, func)

    mv = asyn.sync_wrapper(_mv)
