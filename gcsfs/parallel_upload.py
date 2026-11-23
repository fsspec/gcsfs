"""Parallel uploads through the google-cloud-storage SDK.

``parallel_upload_bytes`` and ``parallel_upload_file`` build the ``Blob`` that
describes the object and call the SDK's parallel upload functions.
"""

import datetime

from google.cloud.storage import Blob, Bucket

from gcsfs.core import SUPPORTED_FIXED_KEY_METADATA

# Prefer the regular path, and fall back to the experimental one.
try:
    from google.cloud.storage.asyncio import async_transfer_manager
except ImportError:
    try:
        from google.cloud.storage._experimental.asyncio import async_transfer_manager
    except ImportError:  # releases before googleapis/google-cloud-python#19105
        async_transfer_manager = None

_UNSUPPORTED_MESSAGE = (
    "parallel_upload requires a google-cloud-storage release that supports "
    "parallel uploads."
)


def _build_blob(
    bucket,
    key,
    content_type,
    metadata,
    fixed_key_metadata,
    storage_class,
    kms_key_name,
):
    """Returns the ``Blob`` describing the object to create.

    A value of ``None`` (or an empty ``kms_key_name``) leaves its field unset.
    ``fixed_key_metadata`` keys other than the ones in
    ``SUPPORTED_FIXED_KEY_METADATA`` are ignored.
    """
    blob = Blob(key, Bucket(None, bucket))
    if content_type is not None:
        blob.content_type = content_type
    if metadata is not None:
        blob.metadata = metadata
    if storage_class is not None:
        blob.storage_class = storage_class
    if kms_key_name:
        blob.kms_key_name = kms_key_name
    fixed_key_metadata = fixed_key_metadata or {}
    for name in SUPPORTED_FIXED_KEY_METADATA:
        value = fixed_key_metadata.get(name)
        if value is None:
            continue
        if name == "custom_time":
            if isinstance(value, str):
                value = datetime.datetime.fromisoformat(value)
            # Blob.custom_time expects UTC.
            if value.tzinfo is not None:
                value = value.astimezone(datetime.timezone.utc)
        setattr(blob, name, value)
    return blob


def _upload_kwargs(
    bucket,
    key,
    content_type,
    metadata,
    fixed_key_metadata,
    storage_class,
    kms_key_name,
    segment_size,
    max_workers,
):
    """Returns the keyword arguments shared by the SDK's upload functions.

    ``max_workers`` is included only when given.
    """
    kwargs = {
        "blob": _build_blob(
            bucket,
            key,
            content_type,
            metadata,
            fixed_key_metadata,
            storage_class,
            kms_key_name,
        ),
        "preferred_segment_size_bytes": segment_size,
    }
    if max_workers is not None:
        kwargs["max_workers"] = max_workers
    return kwargs


async def parallel_upload_bytes(
    client,
    bucket,
    key,
    data,
    *,
    content_type=None,
    metadata=None,
    fixed_key_metadata=None,
    storage_class=None,
    kms_key_name=None,
    segment_size=None,
    max_workers=None,
) -> None:
    """Uploads ``data`` to ``bucket/key`` as one object, in parallel.

    ``content_type``, ``metadata``, ``fixed_key_metadata``, ``storage_class``
    and ``kms_key_name`` describe the object. ``segment_size``, in bytes, and
    ``max_workers`` are left to the SDK when ``None``.

    Raises ``NotImplementedError`` when the installed google-cloud-storage
    release does not support parallel uploads.
    """
    if async_transfer_manager is None:
        raise NotImplementedError(_UNSUPPORTED_MESSAGE)
    kwargs = _upload_kwargs(
        bucket,
        key,
        content_type,
        metadata,
        fixed_key_metadata,
        storage_class,
        kms_key_name,
        segment_size,
        max_workers,
    )
    try:
        await async_transfer_manager.upload_chunks_concurrently_from_bytes(
            client, bucket, key, data, **kwargs
        )
    except NotImplementedError as e:
        raise NotImplementedError(_UNSUPPORTED_MESSAGE) from e


async def parallel_upload_file(
    client,
    bucket,
    key,
    lpath,
    *,
    content_type=None,
    metadata=None,
    fixed_key_metadata=None,
    storage_class=None,
    kms_key_name=None,
    segment_size=None,
    max_workers=None,
) -> None:
    """Uploads the local file ``lpath`` to ``bucket/key`` as one object, in parallel.

    The keyword arguments and the error raised are those of
    ``parallel_upload_bytes``.
    """
    if async_transfer_manager is None:
        raise NotImplementedError(_UNSUPPORTED_MESSAGE)
    kwargs = _upload_kwargs(
        bucket,
        key,
        content_type,
        metadata,
        fixed_key_metadata,
        storage_class,
        kms_key_name,
        segment_size,
        max_workers,
    )
    try:
        await async_transfer_manager.upload_chunks_concurrently_from_filename(
            client, bucket, key, lpath, **kwargs
        )
    except NotImplementedError as e:
        raise NotImplementedError(_UNSUPPORTED_MESSAGE) from e
