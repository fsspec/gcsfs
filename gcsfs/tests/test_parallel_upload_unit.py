"""Unit tests for parallel uploads. The SDK call and the bucket layout lookup are mocked."""

import datetime
import importlib
import sys
import types
from unittest import mock

import fsspec
import pytest

from gcsfs import parallel_upload
from gcsfs.extended_gcsfs import ExtendedGcsFileSystem
from gcsfs.tests.utils import is_real_gcs

pytestmark = pytest.mark.skipif(
    is_real_gcs(), reason="Unit tests with mocks; no real GCS needed."
)


def _fs(**kwargs):
    return ExtendedGcsFileSystem(token="anon", skip_instance_cache=True, **kwargs)


def _flags(fs):
    return (
        fs.parallel_upload,
        fs.parallel_upload_threshold,
        fs.parallel_upload_segment_size,
    )


def test_flags_default_off():
    assert _flags(_fs()) == (False, 128 * 2**20, None)


@pytest.mark.parametrize(
    "kwargs, expected",
    [
        (
            dict(
                parallel_upload=True,
                parallel_upload_threshold=1024,
                parallel_upload_segment_size=4096,
            ),
            (True, 1024, 4096),
        ),
        # Config files give strings.
        (dict(parallel_upload="true"), (True, 128 * 2**20, None)),
        (dict(parallel_upload=""), (False, 128 * 2**20, None)),
    ],
)
def test_flags_from_constructor(kwargs, expected):
    assert _flags(_fs(**kwargs)) == expected


def test_flags_from_env(monkeypatch):
    monkeypatch.setenv("FSSPEC_GCS_PARALLEL_UPLOAD", "true")
    monkeypatch.setenv("FSSPEC_GCS_PARALLEL_UPLOAD_THRESHOLD", "1024")
    monkeypatch.setenv("FSSPEC_GCS_PARALLEL_UPLOAD_SEGMENT_SIZE", "4096")
    # fsspec reads FSSPEC_* variables when it is imported, so read them again.
    conf = {}
    fsspec.config.set_conf_env(conf)
    monkeypatch.setattr(fsspec.config, "conf", conf)
    assert _flags(_fs()) == (True, 1024, 4096)


@pytest.mark.parametrize(
    "flag, value",
    [
        ("parallel_upload", "maybe"),
    ],
)
def test_unparsable_flag_names_the_flag(flag, value):
    with pytest.raises(ValueError, match=flag):
        _fs(**{flag: value})


@pytest.fixture
def sdk():
    """Stands in for the SDK module, so these tests pass on any SDK release."""
    module = types.SimpleNamespace(
        upload_chunks_concurrently_from_bytes=mock.AsyncMock(),
        upload_chunks_concurrently_from_filename=mock.AsyncMock(),
    )
    with mock.patch("gcsfs.parallel_upload.async_transfer_manager", module):
        yield module


REGULAR_PATH = "google.cloud.storage.asyncio.async_transfer_manager"
EXPERIMENTAL_PATH = "google.cloud.storage._experimental.asyncio.async_transfer_manager"


@pytest.mark.parametrize(
    "regular",
    [types.ModuleType(REGULAR_PATH), None],
    ids=["both paths", "experimental path only"],
)
def test_sdk_module_is_imported_from_the_regular_path_first(regular):
    # None in sys.modules makes the import fail, as on an SDK without that path.
    experimental = types.ModuleType(EXPERIMENTAL_PATH)
    modules = {REGULAR_PATH: regular, EXPERIMENTAL_PATH: experimental}
    try:
        with mock.patch.dict(sys.modules, modules):
            reloaded = importlib.reload(parallel_upload)
            assert reloaded.async_transfer_manager is (regular or experimental)
    finally:
        importlib.reload(parallel_upload)


@pytest.mark.asyncio
async def test_upload_bytes_builds_the_blob(sdk):
    client = object()
    kms_key = "projects/p/locations/l/keyRings/r/cryptoKeys/k"
    await parallel_upload.parallel_upload_bytes(
        client,
        "bkt",
        "dir/key",
        b"data",
        content_type="text/plain",
        metadata={"k": "v"},
        fixed_key_metadata={
            "cache_control": "no-cache",
            "custom_time": "2026-01-02T03:04:05Z",
        },
        storage_class="RAPID",
        kms_key_name=kms_key,
        segment_size=4096,
    )
    call = sdk.upload_chunks_concurrently_from_bytes.await_args
    assert call.args == (client, "bkt", "dir/key", b"data")
    assert set(call.kwargs) == {"blob", "preferred_segment_size_bytes"}
    assert call.kwargs["preferred_segment_size_bytes"] == 4096
    blob = call.kwargs["blob"]
    assert (blob.bucket.name, blob.name) == ("bkt", "dir/key")
    assert (blob.content_type, blob.metadata, blob.cache_control) == (
        "text/plain",
        {"k": "v"},
        "no-cache",
    )
    assert blob.custom_time == datetime.datetime(
        2026, 1, 2, 3, 4, 5, tzinfo=datetime.timezone.utc
    )
    assert blob.storage_class == "RAPID"
    assert blob.kms_key_name == kms_key


@pytest.mark.asyncio
@pytest.mark.parametrize("kms_key_name", [None, ""])
async def test_upload_bytes_sets_only_what_was_given(sdk, kms_key_name):
    await parallel_upload.parallel_upload_bytes(
        object(), "bkt", "key", b"data", kms_key_name=kms_key_name, max_workers=3
    )
    call = sdk.upload_chunks_concurrently_from_bytes.await_args
    assert call.kwargs["max_workers"] == 3
    blob = call.kwargs["blob"]
    assert blob._properties == {}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "upload, source",
    [
        (parallel_upload.parallel_upload_bytes, b"data"),
        (parallel_upload.parallel_upload_file, "/tmp/data"),
    ],
)
async def test_upload_without_sdk_support_raises_not_implemented(upload, source):
    # As on an SDK release without the module.
    with mock.patch("gcsfs.parallel_upload.async_transfer_manager", None):
        with pytest.raises(NotImplementedError, match="parallel_upload"):
            await upload(object(), "bkt", "key", source)


@pytest.mark.asyncio
async def test_upload_bytes_with_stub_sdk_raises_not_implemented(sdk):
    sdk.upload_chunks_concurrently_from_bytes.side_effect = NotImplementedError
    with pytest.raises(NotImplementedError, match="parallel_upload") as excinfo:
        await parallel_upload.parallel_upload_bytes(object(), "bkt", "key", b"data")
    assert isinstance(excinfo.value.__cause__, NotImplementedError)


@pytest.mark.asyncio
async def test_upload_file_calls_the_sdk(sdk):
    client = object()
    kms_key = "projects/p/locations/l/keyRings/r/cryptoKeys/k"
    await parallel_upload.parallel_upload_file(
        client,
        "bkt",
        "key",
        "/tmp/data",
        storage_class="RAPID",
        kms_key_name=kms_key,
        max_workers=3,
    )
    call = sdk.upload_chunks_concurrently_from_filename.await_args
    assert call.args == (client, "bkt", "key", "/tmp/data")
    assert set(call.kwargs) == {"blob", "preferred_segment_size_bytes", "max_workers"}
    blob = call.kwargs["blob"]
    assert (blob.storage_class, blob.kms_key_name, call.kwargs["max_workers"]) == (
        "RAPID",
        kms_key,
        3,
    )


@pytest.mark.asyncio
async def test_upload_file_with_stub_sdk_raises_not_implemented(sdk):
    sdk.upload_chunks_concurrently_from_filename.side_effect = NotImplementedError
    with pytest.raises(NotImplementedError, match="parallel_upload") as excinfo:
        await parallel_upload.parallel_upload_file(object(), "bkt", "key", "/tmp/data")
    assert isinstance(excinfo.value.__cause__, NotImplementedError)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "custom_time",
    [
        "2026-01-02T03:04:05+08:00",
        datetime.datetime(
            2026, 1, 2, 3, 4, 5, tzinfo=datetime.timezone(datetime.timedelta(hours=8))
        ),
    ],
)
async def test_upload_bytes_custom_time_is_sent_as_utc(sdk, custom_time):
    await parallel_upload.parallel_upload_bytes(
        object(), "bkt", "key", b"data", fixed_key_metadata={"custom_time": custom_time}
    )
    blob = sdk.upload_chunks_concurrently_from_bytes.await_args.kwargs["blob"]
    assert blob.custom_time == datetime.datetime(
        2026, 1, 1, 19, 4, 5, tzinfo=datetime.timezone.utc
    )
