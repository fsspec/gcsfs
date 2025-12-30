import logging
import os
from enum import Enum
from functools import partial

from fsspec import asyn
from google.api_core import exceptions as api_exceptions
from google.api_core import gapic_v1
from google.api_core.client_info import ClientInfo
from google.auth.credentials import AnonymousCredentials
from google.cloud import storage_control_v2
from google.cloud.storage._experimental.asyncio.async_appendable_object_writer import (
    AsyncAppendableObjectWriter,
)
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
        self._grpc_client = None
        self._storage_control_client = None
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
        self._storage_layout_cache = {}

    @property
    def grpc_client(self):
        if self.asynchronous and self._grpc_client is None:
            raise RuntimeError(
                "Please await _get_grpc_client() before accessing grpc_client"
            )
        if self._grpc_client is None:
            self._grpc_client = asyn.sync(self.loop, self._get_grpc_client)
        return self._grpc_client

    async def _get_grpc_client(self):
        if self._grpc_client is None:
            self._grpc_client = AsyncGrpcClient(
                credentials=self.credential,
                client_info=ClientInfo(user_agent=f"{USER_AGENT}/{version}"),
            ).grpc_client
        return self._grpc_client

    async def _get_control_plane_client(self):
        if self._storage_control_client is None:

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

            self._storage_control_client = storage_control_v2.StorageControlAsyncClient(
                credentials=creds, client_info=client_info
            )
        return self._storage_control_client

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
            await self._get_control_plane_client()
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
        cache_type="readahead",
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
            cache_type=cache_type,
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
        try:
            mrd_created = False

            # A new MRD is required when read is done directly by the
            # GCSFilesystem class without creating a GCSFile object first.
            if mrd is None:
                bucket, object_name, generation = self.split_path(path)
                # Fall back to default implementation if not a zonal bucket
                if not await self._is_zonal_bucket(bucket):
                    return await super()._cat_file(path, start=start, end=end, **kwargs)

                await self._get_grpc_client()
                mrd = await AsyncMultiRangeDownloader.create_mrd(
                    self.grpc_client, bucket, object_name, generation
                )
                mrd_created = True

            offset, length = await self._process_limits_to_offset_and_length(
                path, start, end
            )

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

    async def _put_file(
        self,
        lpath,
        rpath,
        metadata=None,
        consistency=None,
        content_type=None,
        chunksize=50 * 2**20,
        callback=None,
        fixed_key_metadata=None,
        mode="overwrite",
        **kwargs,
    ):
        bucket, key, generation = self.split_path(rpath)
        if not await self._is_zonal_bucket(bucket):
            return await super()._put_file(
                lpath,
                rpath,
                metadata=metadata,
                consistency=consistency,
                content_type=content_type,
                chunksize=chunksize,
                callback=callback,
                fixed_key_metadata=fixed_key_metadata,
                mode=mode,
                **kwargs,
            )

        if os.path.isdir(lpath):
            return

        if generation:
            raise ValueError("Cannot write to specific object generation")

        if (
            metadata
            or fixed_key_metadata
            or consistency
            or (content_type and content_type != "application/octet-stream")
        ):
            logger.warning(
                "Zonal buckets do not support content_type, metadata, "
                "fixed_key_metadata or consistency during upload. "
                "These parameters will be ignored."
            )
        await self._get_grpc_client()
        writer = await zb_hns_utils.init_aaow(self.grpc_client, bucket, key)

        try:
            with open(lpath, "rb") as f:
                await writer.append_from_file(f, block_size=chunksize)
        finally:
            finalize_on_close = kwargs.get("finalize_on_close", False)
            await writer.close(finalize_on_close=finalize_on_close)

        self.invalidate_cache(self._parent(rpath))

    async def _pipe_file(
        self,
        path,
        data,
        metadata=None,
        consistency=None,
        content_type="application/octet-stream",
        fixed_key_metadata=None,
        chunksize=50 * 2**20,
        mode="overwrite",
        **kwargs,
    ):
        bucket, key, generation = self.split_path(path)
        if not await self._is_zonal_bucket(bucket):
            return await super()._pipe_file(
                path,
                data,
                metadata=metadata,
                consistency=consistency,
                content_type=content_type,
                fixed_key_metadata=fixed_key_metadata,
                chunksize=chunksize,
                mode=mode,
            )

        if (
            metadata
            or fixed_key_metadata
            or chunksize != 50 * 2**20
            or (content_type and content_type != "application/octet-stream")
        ):
            logger.warning(
                "Zonal buckets do not support content_type, metadata, "
                "fixed_key_metadata or chunksize during upload. These "
                "parameters will be ignored."
            )
        await self._get_grpc_client()
        writer = await zb_hns_utils.init_aaow(self.grpc_client, bucket, key)
        try:
            await writer.append(data)
        finally:
            finalize_on_close = kwargs.get("finalize_on_close", False)
            await writer.close(finalize_on_close=finalize_on_close)

        self.invalidate_cache(self._parent(path))


async def upload_chunk(fs, location, data, offset, size, content_type):
    """
    Uploads a chunk of data using AsyncAppendableObjectWriter.
    """
    if offset or size or content_type:
        logger.warning(
            "Zonal buckets do not support offset, or content_type during upload. These parameters will be ignored."
        )

    if not isinstance(location, AsyncAppendableObjectWriter):
        raise TypeError(
            "upload_chunk for Zonal buckets expects an AsyncAppendableObjectWriter instance."
        )
    if not location._is_stream_open:
        raise ValueError("Writer is closed. Please initiate a new upload.")

    try:
        await location.append(data)
    except Exception as e:
        logger.error(
            f"Error uploading chunk at offset {location.offset}: {e}. Closing stream."
        )
        # Don't finalize the upload on error
        await location.close(finalize_on_close=False)
        raise
    finally:
        if (location.offset or 0) >= size:
            logger.debug(
                "Uploaded data is equal or greater than size. Finalizing upload."
            )
            await location.close(finalize_on_close=True)


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
    """
    Initiates an upload for Zonal buckets by creating an AsyncAppendableObjectWriter.
    """
    if (
        metadata
        or fixed_key_metadata
        or kms_key_name
        or (content_type and content_type != "application/octet-stream")
    ):
        logger.warning(
            "Zonal buckets do not support content_type, metadata, fixed_key_metadata, "
            "or kms_key_name during upload. These parameters will be ignored."
        )

    await fs._get_grpc_client()
    # If generation is not passed to init_aaow, it creates a new object and overwrites if object already exists.
    # Hence it works for both 'overwrite' and 'create' modes.
    return await zb_hns_utils.init_aaow(fs.grpc_client, bucket, key)


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
    **kwargs,
):
    """
    Performs a simple, single-request upload to Zonal bucket using gRPC.
    """
    if (
        metadatain
        or fixed_key_metadata
        or kms_key_name
        or consistency
        or (content_type and content_type != "application/octet-stream")
    ):
        logger.warning(
            "Zonal buckets do not support content_type, metadatain, fixed_key_metadata, "
            "consistency or kms_key_name during upload. These parameters will be ignored."
        )
    await fs._get_grpc_client()
    # If generation is not passed to init_aaow, it creates a new object and overwrites if object already exists.
    # Hence it works for both 'overwrite' and 'create' modes.
    writer = await zb_hns_utils.init_aaow(fs.grpc_client, bucket, key)
    try:
        await writer.append(datain)
    finally:
        finalize_on_close = kwargs.get("finalize_on_close", False)
        await writer.close(finalize_on_close=finalize_on_close)
