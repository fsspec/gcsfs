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
        # The HNS RenameFolder operation began failing with an "input/output error"
        # after an authentication library change caused it to send a
        # `quota_project_id` from application default credentials. The
        # RenameFolder API rejects requests with this parameter.
        #
        # This workaround explicitly removes the `quota_project_id` to prevent
        # the API from rejecting the request. A long-term fix is in progress
        # in the GCS backend to relax this restriction.
        #
        # TODO: Remove this workaround once the GCS backend fix is deployed.
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
            logger.debug(f"get_storage_layout request for name: {bucket_name_value}")
            response = await self._storage_control_client.get_storage_layout(
                name=bucket_name_value
            )

            if response.location_type == "zone":
                return BucketType.ZONAL_HIERARCHICAL
            if (
                response.hierarchical_namespace
                and response.hierarchical_namespace.enabled
            ):
                return BucketType.HIERARCHICAL
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
        try:
            bucket_type = await self._lookup_bucket_type(bucket)
        except Exception:
            logger.warning(
                f"Could not determine if bucket '{bucket}' is HNS-enabled, falling back to default non-HNS"
            )
            return False

        return bucket_type in [BucketType.ZONAL_HIERARCHICAL, BucketType.HIERARCHICAL]

    def _update_dircache_after_rename(self, path1, path2):
        """
        Performs a targeted update of the directory cache after a successful
        folder rename operation.

        This involves three main steps:
        1. Removing the source folder and all its descendants from the cache.
        2. Removing the source folder's entry from its parent's listing.
        3. Adding the new destination folder's entry to its parent's listing.

        Args:
            path1 (str): The source path that was renamed.
            path2 (str): The destination path.
        """
        # 1. Find and remove all descendant paths of the source from the cache.
        source_prefix = f"{path1.rstrip('/')}/"
        for key in list(self.dircache):
            if key.startswith(source_prefix):
                self.dircache.pop(key, None)

        # 2. Remove the old source entry from its parent's listing.
        self.dircache.pop(path1, None)
        parent1 = self._parent(path1)
        if parent1 in self.dircache:
            for i, entry in enumerate(self.dircache[parent1]):
                if entry.get("name") == path1:
                    self.dircache[parent1].pop(i)
                    break

        # 3. Invalidate the destination path and update its parent's cache.
        self.dircache.pop(path2, None)
        parent2 = self._parent(path2)
        if parent2 in self.dircache:
            _, key2, _ = self.split_path(path2)
            new_entry = {
                "Key": key2,
                "Size": 0,
                "name": path2,
                "size": 0,
                "type": "directory",
                "storageClass": "DIRECTORY",
            }
            self.dircache[parent2].append(new_entry)

    async def _mv(self, path1, path2, **kwargs):
        """
        Move a file or directory. Overrides the parent `_mv` to provide an
        optimized, atomic implementation for renaming folders in HNS-enabled
        buckets. Falls back to the parent's object-level copy-and-delete
        implementation for files or for non-HNS buckets.
        """
        if path1 == path2:
            logger.debug(
                "%s mv: The paths are the same, so no files/directories were moved.",
                self,
            )
            return

        bucket1, key1, _ = self.split_path(path1)
        bucket2, key2, _ = self.split_path(path2)

        is_hns = False
        try:
            is_hns = await self._is_bucket_hns_enabled(bucket1)
        except Exception as e:
            logger.warning(
                f"Could not determine if bucket '{bucket1}' is HNS-enabled, falling back to default mv: {e}"
            )

        if not is_hns:
            logger.debug(
                f"Not an HNS bucket. Falling back to object-level mv for '{path1}' to '{path2}'."
            )
            return await self.loop.run_in_executor(
                None, partial(super().mv, path1, path2, **kwargs)
            )

        try:
            info1 = await self._info(path1)
            is_folder = info1.get("type") == "directory"

            # We only use HNS rename if the source is a folder and the move is
            # within the same bucket.
            if is_folder and bucket1 == bucket2 and key1 and key2:
                logger.info(
                    f"Using HNS-aware folder rename for '{path1}' to '{path2}'."
                )
                source_folder_name = f"projects/_/buckets/{bucket1}/folders/{key1}"
                destination_folder_id = key2

                request = storage_control_v2.RenameFolderRequest(
                    name=source_folder_name,
                    destination_folder_id=destination_folder_id,
                )

                logger.debug(f"rename_folder request: {request}")
                await self._storage_control_client.rename_folder(request=request)
                self._update_dircache_after_rename(path1, path2)

                logger.info(
                    "Successfully renamed folder from '%s' to '%s'", path1, path2
                )
                return
        except Exception as e:
            if isinstance(e, FileNotFoundError):
                # If the source doesn't exist, fail fast.
                raise
            if isinstance(e, api_exceptions.NotFound):
                raise FileNotFoundError(
                    f"Source '{path1}' not found for move operation."
                ) from e
            if isinstance(e, api_exceptions.Conflict):
                # This occurs if the destination folder already exists.
                # Raise FileExistsError for fsspec compatibility.
                raise FileExistsError(
                    f"HNS rename failed due to conflict for '{path1}' to '{path2}'"
                ) from e
            if isinstance(e, api_exceptions.FailedPrecondition):
                raise OSError(f"HNS rename failed: {e}") from e

            logger.warning(f"Could not perform HNS-aware mv: {e}")

        logger.debug(f"Falling back to object-level mv for '{path1}' to '{path2}'.")
        # TODO: Check feasibility to call async copy and rm methods instead of sync mv method
        return await self.loop.run_in_executor(
            None, partial(super().mv, path1, path2, **kwargs)
        )

    mv = asyn.sync_wrapper(_mv)

    async def _mkdir(
        self, path, create_parents=False, create_hns_bucket=False, **kwargs
    ):
        """
        If the path does not contain an object key, a new bucket is created.
        If `create_hns_bucket` is True, the bucket will have Hierarchical Namespace enabled.

        For HNS-enabled buckets, this method creates a folder object. If
        `create_parents` is True, any missing parent folders are also created.
        If `create_parents` is False and a parent does not exist, a
        FileNotFoundError is raised.

        For non-HNS buckets, it falls back to the parent implementation which
        may involve creating a bucket or doing nothing (as GCS has no true empty directories).
        """
        path = self._strip_protocol(path)
        if create_hns_bucket:
            kwargs["hierarchicalNamespace"] = {"enabled": True}
            # HNS buckets require uniform bucket-level access.
            kwargs["iamConfiguration"] = {"uniformBucketLevelAccess": {"enabled": True}}
            # When uniformBucketLevelAccess is enabled, ACLs cannot be used.
            # We must explicitly set them to None to prevent the parent
            # method from using default ACLs.
            kwargs["acl"] = None
            kwargs["default_acl"] = None

        bucket, key, _ = self.split_path(path)
        # If key is empty, it's a bucket operation. Defer to parent.
        if not key:
            return await super()._mkdir(path, create_parents=create_parents, **kwargs)

        is_hns = False
        try:
            is_hns = await self._is_bucket_hns_enabled(bucket)
        except Exception as e:
            logger.warning(
                f"Could not determine if bucket '{bucket}' is HNS-enabled, falling back to default mkdir: {e}"
            )

        if not is_hns:
            return await super()._mkdir(path, create_parents=create_parents, **kwargs)

        logger.info(f"Using HNS-aware mkdir for '{path}'.")
        parent = f"projects/_/buckets/{bucket}"
        folder_id = key.rstrip("/")
        request = storage_control_v2.CreateFolderRequest(
            parent=parent,
            folder_id=folder_id,
            recursive=create_parents,
        )
        try:
            logger.debug(f"create_folder request: {request}")
            await self._storage_control_client.create_folder(request=request)
            # Instead of invalidating the parent cache, update it to add the new entry.
            parent_path = self._parent(path)
            if parent_path in self.dircache:
                new_entry = {
                    "Key": key.rstrip("/"),
                    "Size": 0,
                    "name": path,
                    "size": 0,
                    "type": "directory",
                    "storageClass": "DIRECTORY",
                }
                self.dircache[parent_path].append(new_entry)
        except api_exceptions.Conflict as e:
            logger.debug(f"Directory already exists: {path}: {e}")
        except api_exceptions.FailedPrecondition as e:
            # This error can occur if create_parents=False and the parent dir doesn't exist.
            # Translate it to FileNotFoundError for fsspec compatibility.
            raise FileNotFoundError(
                f"mkdir for '{path}' failed due to a precondition error: {e}"
            ) from e

    mkdir = asyn.sync_wrapper(_mkdir)

    async def _get_directory_info(self, path, bucket, key, generation):
        """
        Override to use Storage Control API's get_folder for HNS buckets.
        For HNS, we avoid calling _ls (_list_objects) entirely.
        """
        is_hns = await self._is_bucket_hns_enabled(bucket)

        # If bucket is HNS, use get folder metadata api to determine a directory
        if is_hns:
            try:
                # folder_id is the path relative to the bucket
                folder_id = key.rstrip("/")
                folder_resource_name = (
                    f"projects/_/buckets/{bucket}/folders/{folder_id}"
                )

                request = storage_control_v2.GetFolderRequest(name=folder_resource_name)

                # Verify existence using get_folder API
                response = await self._storage_control_client.get_folder(
                    request=request
                )

                # If successful, return directory metadata
                return {
                    "bucket": bucket,
                    "name": path,
                    "size": 0,
                    "storageClass": "DIRECTORY",
                    "type": "directory",
                    "ctime": response.create_time,
                    "mtime": response.update_time,
                    "metageneration": response.metageneration,
                }
            except api_exceptions.NotFound:
                # If get_folder fails, the folder does not exist.
                raise FileNotFoundError(path)
            except Exception as e:
                # Log unexpected errors
                logger.error(f"Error fetching folder metadata for {path}: {e}")
                raise e

        # Fallback to standard GCS behavior for non-HNS buckets
        return await super()._get_directory_info(path, bucket, key, generation)


async def upload_chunk(fs, location, data, offset, size, content_type):
    raise NotImplementedError(
        "upload_chunk is not implemented yet for Zonal experimental feature. Please use write() instead."
    )


async def initiate_upload(
    fs,
    bucket,
    key,
    content_type="application/octet-stream",
    metadata=None,
    fixed_key_metadata=None,
    mode="overwrite",
    kms_key_name=None,
):
    raise NotImplementedError(
        "initiate_upload is not implemented yet for Zonal experimental feature. Please use write() instead."
    )


async def simple_upload(
    fs,
    bucket,
    key,
    datain,
    metadatain=None,
    consistency=None,
    content_type="application/octet-stream",
    fixed_key_metadata=None,
    mode="overwrite",
    kms_key_name=None,
):
    raise NotImplementedError(
        "simple_upload is not implemented yet for Zonal experimental feature. Please use write() instead."
    )
