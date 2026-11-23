"""Unit tests for parallel uploads. The SDK call and the bucket layout lookup are mocked."""

import base64
import datetime
import importlib.util
import pathlib
import sys
import types
from unittest import mock

import fsspec
import pytest
from google.cloud import _storage_v2

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
        (dict(parallel_upload_threshold=0), (False, 0, None)),
        # Config files and empty env vars give strings.
        (
            dict(
                parallel_upload="true",
                parallel_upload_threshold="1024",
                parallel_upload_segment_size=" 4096 ",
            ),
            (True, 1024, 4096),
        ),
        (
            dict(
                parallel_upload="",
                parallel_upload_threshold="",
                parallel_upload_segment_size="",
            ),
            (False, 128 * 2**20, None),
        ),
        (
            dict(
                parallel_upload_threshold=None,
                parallel_upload_segment_size="   ",
            ),
            (False, 128 * 2**20, None),
        ),
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


def test_empty_env_vars_fall_back_to_defaults(monkeypatch):
    monkeypatch.setenv("FSSPEC_GCS_PARALLEL_UPLOAD", "")
    monkeypatch.setenv("FSSPEC_GCS_PARALLEL_UPLOAD_THRESHOLD", "")
    monkeypatch.setenv("FSSPEC_GCS_PARALLEL_UPLOAD_SEGMENT_SIZE", "")
    conf = {}
    fsspec.config.set_conf_env(conf)
    monkeypatch.setattr(fsspec.config, "conf", conf)
    assert _flags(_fs()) == (False, 128 * 2**20, None)


@pytest.mark.parametrize(
    "flag, value",
    [
        ("parallel_upload", "maybe"),
        ("parallel_upload_threshold", "big"),
        ("parallel_upload_threshold", True),
        ("parallel_upload_threshold", 1.5),
        ("parallel_upload_threshold", -1),
        ("parallel_upload_segment_size", "big"),
        ("parallel_upload_segment_size", False),
        ("parallel_upload_segment_size", 0),
        ("parallel_upload_segment_size", "-1"),
    ],
)
def test_unparsable_flag_names_the_flag(flag, value):
    with pytest.raises(ValueError, match=flag):
        _fs(**{flag: value})


DEFAULT_OBJECT = _storage_v2.Object(
    size=4,
    generation=123,
    checksums=_storage_v2.ObjectChecksums(crc32c=0x12345678),
)


@pytest.fixture
def sdk():
    """Stands in for the SDK module, so these tests pass on any SDK release."""
    module = types.SimpleNamespace(
        upload_chunks_concurrently_from_bytes=mock.AsyncMock(
            return_value=DEFAULT_OBJECT
        ),
        upload_chunks_concurrently_from_filename=mock.AsyncMock(
            return_value=DEFAULT_OBJECT
        ),
    )
    with mock.patch("gcsfs.parallel_upload.async_transfer_manager", module):
        yield module


REGULAR_PKG = "google.cloud.storage.asyncio"
EXPERIMENTAL_PKG = "google.cloud.storage._experimental.asyncio"
REGULAR_PATH = f"{REGULAR_PKG}.async_transfer_manager"
EXPERIMENTAL_PATH = f"{EXPERIMENTAL_PKG}.async_transfer_manager"


@pytest.mark.parametrize(
    "regular, experimental, expected",
    [
        (
            types.ModuleType(REGULAR_PATH),
            types.ModuleType(EXPERIMENTAL_PATH),
            "regular",
        ),
        (None, types.ModuleType(EXPERIMENTAL_PATH), "experimental"),
        (None, None, None),
    ],
    ids=["both paths", "experimental path only", "neither path"],
)
def test_sdk_module_is_imported_from_the_regular_path_first(
    monkeypatch, regular, experimental, expected
):
    for pkg_name in (
        "google.cloud.storage._experimental",
        REGULAR_PKG,
        EXPERIMENTAL_PKG,
    ):
        if pkg_name not in sys.modules:
            pkg = types.ModuleType(pkg_name)
            pkg.__path__ = []
            monkeypatch.setitem(sys.modules, pkg_name, pkg)
        monkeypatch.delattr(
            sys.modules[pkg_name], "async_transfer_manager", raising=False
        )
    monkeypatch.setitem(sys.modules, REGULAR_PATH, regular)
    monkeypatch.setitem(sys.modules, EXPERIMENTAL_PATH, experimental)
    spec = importlib.util.spec_from_file_location(
        "_parallel_upload", parallel_upload.__file__
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert (
        module.async_transfer_manager
        is {"regular": regular, "experimental": experimental, None: None}[expected]
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "upload, sdk_attr, source, expected_source",
    [
        (
            parallel_upload.parallel_upload_bytes,
            "upload_chunks_concurrently_from_bytes",
            b"data",
            b"data",
        ),
        (
            parallel_upload.parallel_upload_file,
            "upload_chunks_concurrently_from_filename",
            "/tmp/data",
            "/tmp/data",
        ),
        (
            parallel_upload.parallel_upload_file,
            "upload_chunks_concurrently_from_filename",
            pathlib.Path("/tmp/data"),
            str(pathlib.Path("/tmp/data")),
        ),
    ],
    ids=["bytes", "str path", "Path"],
)
async def test_upload_builds_the_blob(sdk, upload, sdk_attr, source, expected_source):
    client = object()
    kms_key = "projects/p/locations/l/keyRings/r/cryptoKeys/k"
    info = await upload(
        client,
        "bkt",
        "dir/key",
        source,
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
    call = getattr(sdk, sdk_attr).await_args
    assert call.args == (client, "bkt", "dir/key", expected_source)
    assert type(call.args[3]) is type(expected_source)
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
    assert info == {
        "size": "4",
        "generation": "123",
        "crc32c": base64.b64encode((0x12345678).to_bytes(4, "big")).decode(),
    }


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
    "obj, expected",
    [
        (
            _storage_v2.Object(
                size=0,
                generation=7,
                checksums=_storage_v2.ObjectChecksums(crc32c=0),
            ),
            {
                "size": "0",
                "generation": "7",
                "crc32c": base64.b64encode((0).to_bytes(4, "big")).decode(),
            },
        ),
        (
            _storage_v2.Object(size=10, generation=42),
            {"size": "10", "generation": "42"},
        ),
    ],
    ids=["zero crc32c", "without crc32c"],
)
async def test_upload_returns_object_info(sdk, obj, expected):
    sdk.upload_chunks_concurrently_from_bytes.return_value = obj
    info = await parallel_upload.parallel_upload_bytes(object(), "bkt", "key", b"data")
    assert info == expected


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
@pytest.mark.parametrize(
    "upload, sdk_attr, source",
    [
        (
            parallel_upload.parallel_upload_bytes,
            "upload_chunks_concurrently_from_bytes",
            b"data",
        ),
        (
            parallel_upload.parallel_upload_file,
            "upload_chunks_concurrently_from_filename",
            "/tmp/data",
        ),
    ],
)
async def test_upload_with_stub_sdk_raises_not_implemented(
    sdk, upload, sdk_attr, source
):
    getattr(sdk, sdk_attr).side_effect = NotImplementedError
    with pytest.raises(NotImplementedError, match="parallel_upload") as excinfo:
        await upload(object(), "bkt", "key", source)
    assert isinstance(excinfo.value.__cause__, NotImplementedError)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "custom_time, expected",
    [
        (
            "2026-01-02T03:04:05+08:00",
            datetime.datetime(2026, 1, 1, 19, 4, 5, tzinfo=datetime.timezone.utc),
        ),
        (
            datetime.datetime(
                2026,
                1,
                2,
                3,
                4,
                5,
                tzinfo=datetime.timezone(datetime.timedelta(hours=8)),
            ),
            datetime.datetime(2026, 1, 1, 19, 4, 5, tzinfo=datetime.timezone.utc),
        ),
        (
            datetime.datetime(2026, 1, 2, 3, 4, 5),
            datetime.datetime(2026, 1, 2, 3, 4, 5, tzinfo=datetime.timezone.utc),
        ),
    ],
    ids=["offset string", "aware datetime", "naive datetime"],
)
async def test_upload_bytes_custom_time_is_sent_as_utc(sdk, custom_time, expected):
    await parallel_upload.parallel_upload_bytes(
        object(), "bkt", "key", b"data", fixed_key_metadata={"custom_time": custom_time}
    )
    blob = sdk.upload_chunks_concurrently_from_bytes.await_args.kwargs["blob"]
    assert blob.custom_time == expected
