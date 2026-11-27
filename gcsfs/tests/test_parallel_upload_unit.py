"""Unit tests for parallel uploads. The SDK call and the bucket layout lookup are mocked."""

import array
import base64
import contextlib
import datetime
import importlib.util
import logging
import os
import pathlib
import sys
import types
from unittest import mock

import fsspec
import pytest
from google.api_core import exceptions as api_exceptions
from google.cloud import _storage_v2
from google.cloud.storage.asyncio.async_appendable_object_writer import (
    AsyncAppendableObjectWriter,
)

from gcsfs import parallel_upload
from gcsfs.core import GCSFileSystem
from gcsfs.extended_gcsfs import BucketLayout, ExtendedGcsFileSystem
from gcsfs.tests import layouts
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
        ("parallel_upload", 4),
        ("parallel_upload", 1.0),
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


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "metadata, expected",
    [
        ({1: "v", "k": None, "n": 2}, {"1": "v", "k": "", "n": "2"}),
        ({}, None),
    ],
    ids=["keys and values", "empty"],
)
async def test_upload_bytes_metadata_is_sent_as_strings(sdk, metadata, expected):
    await parallel_upload.parallel_upload_bytes(
        object(), "bkt", "key", b"data", metadata=metadata
    )
    blob = sdk.upload_chunks_concurrently_from_bytes.await_args.kwargs["blob"]
    assert blob.metadata == expected


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
async def test_upload_rejects_kms_key_name_with_crypto_key_version(
    sdk, upload, sdk_attr, source
):
    versioned_key = "projects/p/locations/l/keyRings/r/cryptoKeys/k/cryptoKeyVersions/1"
    with pytest.raises(
        ValueError, match="kms_key_name must not include /cryptoKeyVersions/"
    ):
        await upload(object(), "bkt", "key", source, kms_key_name=versioned_key)
    getattr(sdk, sdk_attr).assert_not_awaited()


BUCKET = "bkt"
PATH = f"{BUCKET}/key"
THRESHOLD = 8
BIG = b"x" * THRESHOLD
SMALL = b"x" * (THRESHOLD - 1)
PARALLEL, HTTP, APPENDABLE = "parallel", "http", "appendable"


@contextlib.contextmanager
def _mocked(layout, **fs_kwargs):
    """A filesystem with the flag on, and a mock for each place a write can go."""
    fs_kwargs.setdefault("parallel_upload", True)
    fs_kwargs.setdefault("parallel_upload_threshold", THRESHOLD)
    fs = _fs(**fs_kwargs)
    fs._grpc_client = mock.MagicMock()
    fs._storage_layout_cache[BUCKET] = layout
    writer = mock.AsyncMock(spec=AsyncAppendableObjectWriter)
    writer._is_stream_open = True
    with (
        mock.patch.object(
            fs, "_get_grpc_client", autospec=True, return_value=fs._grpc_client
        ),
        # Already a mock for the whole session (conftest), so it has no spec.
        mock.patch.object(
            fs, "_get_bucket_layout", new_callable=mock.AsyncMock
        ) as lookup,
        mock.patch.object(
            fs,
            "_write_file_cache_update",
            autospec=True,
            wraps=fs._write_file_cache_update,
        ) as cache_update,
        mock.patch(
            "gcsfs.parallel_upload.parallel_upload_bytes", autospec=True
        ) as upload_bytes,
        mock.patch(
            "gcsfs.parallel_upload.parallel_upload_file", autospec=True
        ) as upload_file,
        mock.patch(
            "gcsfs.zb_hns_utils.init_aaow", autospec=True, return_value=writer
        ) as appendable,
        mock.patch("gcsfs.core.GCSFileSystem._pipe_file", autospec=True) as http_pipe,
        mock.patch("gcsfs.core.GCSFileSystem._put_file", autospec=True) as http_put,
    ):
        yield types.SimpleNamespace(
            fs=fs,
            lookup=lookup,
            cache_update=cache_update,
            upload_bytes=upload_bytes,
            upload_file=upload_file,
            appendable=appendable,
            http_pipe=http_pipe,
            http_put=http_put,
        )


def _route(m):
    """The one path the write took."""
    counts = {
        PARALLEL: m.upload_bytes.await_count + m.upload_file.await_count,
        HTTP: m.http_pipe.await_count + m.http_put.await_count,
        APPENDABLE: m.appendable.await_count,
    }
    assert sum(counts.values()) == 1, counts
    return next(path for path, count in counts.items() if count)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "layout, fs_kwargs, call_kwargs, data, expected",
    [
        # Buckets written over HTTP: parallel at or above the threshold.
        (layouts.STANDARD, {}, {}, BIG, PARALLEL),
        (layouts.STANDARD_HIERARCHICAL, {}, {}, BIG, PARALLEL),
        (layouts.STANDARD_RCU, {}, {}, BIG, PARALLEL),
        (layouts.STANDARD, {}, {}, SMALL, HTTP),
        (layouts.STANDARD, {"parallel_upload": False}, {}, BIG, HTTP),
        # Preconditions keep the current path.
        (layouts.STANDARD, {}, {"mode": "create"}, BIG, HTTP),
        (layouts.STANDARD, {}, {"if_generation_match": 5}, BIG, HTTP),
        # A failed layout lookup keeps the current path.
        (BucketLayout.UNKNOWN, {}, {}, BIG, HTTP),
        # Zonal buckets: only writes that end finalized.
        (layouts.ZONAL_HIERARCHICAL, {}, {}, BIG, APPENDABLE),
        (layouts.ZONAL_HIERARCHICAL, {}, {"finalize_on_close": True}, BIG, PARALLEL),
        (layouts.ZONAL_HIERARCHICAL, {"finalize_on_close": True}, {}, BIG, PARALLEL),
        (
            layouts.ZONAL_HIERARCHICAL,
            {},
            {"finalize_on_close": True},
            SMALL,
            APPENDABLE,
        ),
        # RCU buckets with rcu_rapid_write: only writes that end finalized.
        (layouts.STANDARD_RCU, {"rcu_rapid_write": True}, {}, BIG, PARALLEL),
        (layouts.STANDARD_RCU, {"rcu_rapid_write": True}, {}, SMALL, APPENDABLE),
        (
            layouts.STANDARD_RCU,
            {"rcu_rapid_write": True},
            {"finalize_on_close": False},
            BIG,
            APPENDABLE,
        ),
        (layouts.STANDARD, {}, {"metadata": {"k": ""}}, BIG, HTTP),
        (layouts.STANDARD, {}, {"metadata": {"k": None}}, BIG, HTTP),
        (layouts.STANDARD, {}, {"metadata": {"": "v"}}, BIG, HTTP),
        (
            layouts.ZONAL_HIERARCHICAL,
            {},
            {"finalize_on_close": True, "metadata": {"k": ""}},
            BIG,
            PARALLEL,
        ),
    ],
)
async def test_pipe_file_routing(layout, fs_kwargs, call_kwargs, data, expected):
    with _mocked(layout, **fs_kwargs) as m:
        await m.fs._pipe_file(PATH, data, **call_kwargs)
    assert _route(m) == expected


@pytest.mark.asyncio
@pytest.mark.parametrize("path", [BUCKET, f"{BUCKET}/"])
async def test_pipe_file_bucket_only_path_keeps_the_existing_path(path):
    with _mocked(layouts.STANDARD) as m:
        await m.fs._pipe_file(path, BIG)
    assert _route(m) == HTTP


@pytest.mark.asyncio
async def test_pipe_file_upload_arguments():
    kms_key = "projects/p/locations/l/keyRings/r/cryptoKeys/k"
    with _mocked(layouts.STANDARD, parallel_upload_segment_size=4096) as m:
        await m.fs._pipe_file(
            PATH,
            BIG,
            metadata={"k": "v"},
            content_type="text/plain",
            fixed_key_metadata={"cache_control": "no-cache"},
            kms_key_name=kms_key,
        )
    call = m.upload_bytes.await_args
    assert call.args == (m.fs._grpc_client, BUCKET, "key", BIG)
    assert call.kwargs == dict(
        content_type="text/plain",
        metadata={"k": "v"},
        fixed_key_metadata={"cache_control": "no-cache"},
        storage_class=None,
        kms_key_name=kms_key,
        segment_size=4096,
        max_workers=None,
    )
    m.cache_update.assert_awaited_once_with(PATH)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "layout, fs_kwargs, storage_class",
    [
        (layouts.STANDARD, {}, None),
        (layouts.STANDARD_RCU, {}, None),
        (layouts.STANDARD_RCU, {"rcu_rapid_write": True}, "RAPID"),
        (layouts.ZONAL_HIERARCHICAL, {"finalize_on_close": True}, None),
    ],
)
async def test_pipe_file_storage_class(layout, fs_kwargs, storage_class):
    with _mocked(layout, **fs_kwargs) as m:
        await m.fs._pipe_file(PATH, BIG)
    assert m.upload_bytes.await_args.kwargs.get("storage_class") == storage_class


@pytest.mark.asyncio
async def test_pipe_file_per_call_segment_size_and_concurrency():
    with _mocked(layouts.STANDARD, parallel_upload_segment_size=4096) as m:
        await m.fs._pipe_file(
            PATH, BIG, parallel_upload_segment_size=8192, concurrency=3
        )
    kwargs = m.upload_bytes.await_args.kwargs
    assert (kwargs["segment_size"], kwargs["max_workers"]) == (8192, 3)


@pytest.mark.asyncio
async def test_pipe_file_per_call_none_keeps_the_filesystem_segment_size():
    with _mocked(layouts.STANDARD, parallel_upload_segment_size=4096) as m:
        await m.fs._pipe_file(
            PATH, BIG, parallel_upload_segment_size=None, concurrency=None
        )
    kwargs = m.upload_bytes.await_args.kwargs
    assert (kwargs["segment_size"], kwargs["max_workers"]) == (4096, None)


@pytest.mark.asyncio
@pytest.mark.parametrize("data", [BIG, SMALL], ids=["big", "small"])
@pytest.mark.parametrize(
    "name, value",
    [
        ("concurrency", 0),
        ("concurrency", False),
        ("parallel_upload_segment_size", 0),
        ("parallel_upload_segment_size", "bad"),
    ],
)
async def test_pipe_file_rejects_invalid_per_call_options(name, value, data):
    with _mocked(layouts.STANDARD) as m:
        with pytest.raises(ValueError, match=name):
            await m.fs._pipe_file(PATH, data, **{name: value})
    m.upload_bytes.assert_not_awaited()
    m.http_pipe.assert_not_awaited()


@pytest.mark.asyncio
async def test_pipe_file_ignores_per_call_options_when_the_option_is_off():
    with _mocked(layouts.STANDARD, parallel_upload=False) as m:
        await m.fs._pipe_file(
            PATH, BIG, parallel_upload_segment_size="bad", concurrency=0
        )
    assert _route(m) == HTTP


@pytest.mark.asyncio
async def test_pipe_file_content_type_none_uses_the_default():
    with _mocked(layouts.STANDARD) as m:
        await m.fs._pipe_file(PATH, BIG, content_type=None)
    content_type = m.upload_bytes.await_args.kwargs["content_type"]
    assert content_type == "application/octet-stream"


@pytest.mark.asyncio
async def test_pipe_file_threshold_counts_bytes():
    data = array.array("q", [1])  # one item, eight bytes
    with _mocked(layouts.STANDARD) as m:
        await m.fs._pipe_file(PATH, data)
    assert _route(m) == PARALLEL
    assert len(m.upload_bytes.await_args.args[3]) == THRESHOLD


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fs_kwargs, call_kwargs, warns",
    [
        ({}, {"consistency": "md5"}, True),
        ({"consistency": "md5"}, {}, True),
        ({}, {"consistency": "size"}, True),
        ({}, {"consistency": "crc32c"}, True),
        ({}, {}, False),
        ({}, {"consistency": "none"}, False),
        ({"consistency": "md5"}, {"consistency": "none"}, False),
    ],
)
async def test_pipe_file_warns_when_consistency_is_ignored(
    caplog, fs_kwargs, call_kwargs, warns
):
    with (
        _mocked(layouts.STANDARD, **fs_kwargs) as m,
        caplog.at_level(logging.WARNING, logger="gcsfs"),
    ):
        await m.fs._pipe_file(PATH, BIG, **call_kwargs)
    assert _route(m) == PARALLEL
    assert any("consistency" in r.getMessage() for r in caplog.records) is warns


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "rejection", [api_exceptions.InvalidArgument, api_exceptions.FailedPrecondition]
)
async def test_pipe_file_retries_without_rapid_after_rcu_disabled(caplog, rejection):
    with (
        _mocked(layouts.STANDARD_RCU, rcu_rapid_write=True) as m,
        caplog.at_level(logging.WARNING, logger="gcsfs"),
    ):
        m.upload_bytes.side_effect = [rejection("rejected"), None]
        m.lookup.return_value = layouts.STANDARD
        await m.fs._pipe_file(PATH, BIG)
    assert [c.kwargs["storage_class"] for c in m.upload_bytes.await_args_list] == [
        "RAPID",
        None,
    ]
    assert m.fs._storage_layout_cache[BUCKET] == layouts.STANDARD
    assert any("Retrying the upload" in r.getMessage() for r in caplog.records)
    m.cache_update.assert_awaited_once_with(PATH)


@pytest.mark.asyncio
async def test_pipe_file_failed_retry_is_not_repeated():
    second = api_exceptions.InvalidArgument("rejected again")
    with _mocked(layouts.STANDARD_RCU, rcu_rapid_write=True) as m:
        m.upload_bytes.side_effect = [
            api_exceptions.InvalidArgument("rejected"),
            second,
        ]
        m.lookup.return_value = layouts.STANDARD
        with pytest.raises(api_exceptions.InvalidArgument) as excinfo:
            await m.fs._pipe_file(PATH, BIG)
    assert excinfo.value is second
    assert m.upload_bytes.await_count == 2
    m.cache_update.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "layout, fs_kwargs, error, expected_lookups",
    [
        (layouts.STANDARD, {}, api_exceptions.InvalidArgument("rejected"), 0),
        (
            layouts.ZONAL_HIERARCHICAL,
            {"finalize_on_close": True},
            api_exceptions.InvalidArgument("rejected"),
            0,
        ),
        (
            layouts.STANDARD_RCU,
            {"rcu_rapid_write": True},
            api_exceptions.InvalidArgument("rejected"),
            1,
        ),
        (
            layouts.STANDARD_RCU,
            {"rcu_rapid_write": True},
            api_exceptions.FailedPrecondition("rejected"),
            1,
        ),
        (
            layouts.STANDARD_RCU,
            {"rcu_rapid_write": True},
            api_exceptions.ServiceUnavailable("unavailable"),
            1,
        ),
        (
            layouts.STANDARD_RCU,
            {"rcu_rapid_write": True},
            NotImplementedError("no SDK support"),
            0,
        ),
    ],
)
async def test_pipe_file_other_errors_surface_unchanged(
    layout, fs_kwargs, error, expected_lookups
):
    with _mocked(layout, **fs_kwargs) as m:
        m.upload_bytes.side_effect = error
        m.lookup.return_value = layout
        with pytest.raises(type(error)) as excinfo:
            await m.fs._pipe_file(PATH, BIG)
    assert excinfo.value is error
    assert (m.upload_bytes.await_count, m.lookup.await_count) == (1, expected_lookups)
    m.cache_update.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "layout, fs_kwargs",
    [
        (layouts.STANDARD, {}),
        (layouts.STANDARD_RCU, {}),
        (layouts.ZONAL_HIERARCHICAL, {"finalize_on_close": True}),
    ],
)
async def test_pipe_file_forwards_kms_key_name(layout, fs_kwargs):
    kms_key = "projects/p/locations/l/keyRings/r/cryptoKeys/k"
    with (
        mock.patch("gcsfs.zb_hns_utils.Preconditions", None),
        _mocked(layout, **fs_kwargs) as m,
    ):
        await m.fs._pipe_file(PATH, BIG, kms_key_name=kms_key)
    assert _route(m) == PARALLEL
    assert m.upload_bytes.await_args.kwargs["kms_key_name"] == kms_key


@pytest.mark.asyncio
async def test_pipe_file_rejects_kms_key_name_on_rcu_rapid_write():
    kms_key = "projects/p/locations/l/keyRings/r/cryptoKeys/k"
    with _mocked(layouts.STANDARD_RCU, rcu_rapid_write=True) as m:
        with pytest.raises(
            NotImplementedError, match="not supported for gRPC RAPID uploads"
        ):
            await m.fs._pipe_file(PATH, BIG, kms_key_name=kms_key)
    m.upload_bytes.assert_not_awaited()
    m.cache_update.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "layout, fs_kwargs",
    [
        (layouts.STANDARD, {}),
        (layouts.STANDARD_RCU, {}),
        (layouts.ZONAL_HIERARCHICAL, {"finalize_on_close": True}),
    ],
)
async def test_pipe_file_rejects_kms_key_name_with_crypto_key_version(
    layout, fs_kwargs
):
    versioned_key = "projects/p/locations/l/keyRings/r/cryptoKeys/k/cryptoKeyVersions/1"
    with _mocked(layout, **fs_kwargs) as m:
        with pytest.raises(
            ValueError,
            match="kms_key_name must not include /cryptoKeyVersions/",
        ):
            await m.fs._pipe_file(PATH, BIG, kms_key_name=versioned_key)
    m.upload_bytes.assert_not_awaited()
    m.cache_update.assert_not_awaited()


def test_pipe_file_sync_end_to_end(sdk):
    """Only the SDK call is faked: sync pipe_file, the Blob, and the cache."""
    fs = _fs(parallel_upload=True, parallel_upload_threshold=THRESHOLD)
    fs._grpc_client = mock.MagicMock()
    fs._storage_layout_cache[BUCKET] = layouts.STANDARD
    fs.dircache[f"{BUCKET}/dir"] = [{"name": f"{BUCKET}/dir/old", "type": "file"}]
    fs.dircache[BUCKET] = [{"name": f"{BUCKET}/dir", "type": "directory"}]

    fs.pipe_file(
        f"gs://{BUCKET}/dir/key",
        BIG,
        metadata={"k": "v"},
        content_type="text/plain",
        fixed_key_metadata={"cache_control": "no-cache"},
        parallel_upload_segment_size=8192,
        concurrency=3,
    )

    call = sdk.upload_chunks_concurrently_from_bytes.await_args
    assert call.args == (fs._grpc_client, BUCKET, "dir/key", BIG)
    assert call.kwargs["preferred_segment_size_bytes"] == 8192
    assert call.kwargs["max_workers"] == 3
    blob = call.kwargs["blob"]
    assert (blob.bucket.name, blob.name) == (BUCKET, "dir/key")
    assert (blob.content_type, blob.metadata, blob.cache_control) == (
        "text/plain",
        {"k": "v"},
        "no-cache",
    )
    # Only the listing of the parent directory, which was cached, is dropped.
    assert f"{BUCKET}/dir" not in fs.dircache
    assert BUCKET in fs.dircache


@pytest.mark.asyncio
async def test_core_pipe_file_ignores_extra_keyword_arguments():
    fs = GCSFileSystem(token="anon", skip_instance_cache=True)
    with mock.patch("gcsfs.core.simple_upload", autospec=True) as upload:
        await fs._pipe_file(
            PATH, SMALL, parallel_upload_segment_size=8192, concurrency=3
        )
    upload.assert_awaited_once()
    assert not {"parallel_upload_segment_size", "concurrency"} & set(
        upload.await_args.kwargs
    )


@pytest.fixture
def lfile(tmp_path):
    def make(data, name="data.bin"):
        path = tmp_path / name
        path.write_bytes(data)
        return str(path)

    return make


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "layout, fs_kwargs, call_kwargs, data, expected",
    [
        (layouts.STANDARD, {}, {}, BIG, PARALLEL),
        (layouts.STANDARD, {}, {}, SMALL, HTTP),
        (layouts.STANDARD, {"parallel_upload": False}, {}, BIG, HTTP),
        (layouts.STANDARD, {}, {"mode": "create"}, BIG, HTTP),
        (layouts.STANDARD, {}, {"if_generation_match": 5}, BIG, HTTP),
        (BucketLayout.UNKNOWN, {}, {}, BIG, HTTP),
        (layouts.ZONAL_HIERARCHICAL, {}, {}, BIG, APPENDABLE),
        (layouts.ZONAL_HIERARCHICAL, {}, {"finalize_on_close": True}, BIG, PARALLEL),
        (layouts.STANDARD_RCU, {"rcu_rapid_write": True}, {}, BIG, PARALLEL),
        (
            layouts.STANDARD_RCU,
            {"rcu_rapid_write": True},
            {"finalize_on_close": False},
            BIG,
            APPENDABLE,
        ),
        (layouts.STANDARD, {}, {"metadata": {"k": ""}}, BIG, HTTP),
    ],
)
async def test_put_file_routing(lfile, layout, fs_kwargs, call_kwargs, data, expected):
    with _mocked(layout, **fs_kwargs) as m:
        await m.fs._put_file(lfile(data), PATH, **call_kwargs)
    assert _route(m) == expected


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "name, call_kwargs, content_type",
    [
        ("data.json", {}, "application/json"),
        ("data", {}, "application/octet-stream"),
        ("data.json", {"content_type": "text/plain"}, "text/plain"),
    ],
)
async def test_put_file_upload_arguments_and_progress(
    lfile, name, call_kwargs, content_type
):
    lpath = lfile(BIG, name=name)
    callback = mock.Mock()
    with _mocked(layouts.STANDARD) as m:
        await m.fs._put_file(
            lpath,
            PATH,
            metadata={"k": "v"},
            callback=callback,
            parallel_upload_segment_size=8192,
            concurrency=3,
            **call_kwargs,
        )
    call = m.upload_file.await_args
    assert call.args == (m.fs._grpc_client, BUCKET, "key", lpath)
    assert call.kwargs == dict(
        content_type=content_type,
        metadata={"k": "v"},
        fixed_key_metadata=None,
        storage_class=None,
        kms_key_name=None,
        segment_size=8192,
        max_workers=3,
    )
    callback.set_size.assert_called_once_with(THRESHOLD)
    callback.absolute_update.assert_called_once_with(THRESHOLD)
    m.cache_update.assert_awaited_once_with(PATH)


@pytest.mark.asyncio
async def test_put_file_rcu_rapid_write_uses_the_rapid_storage_class(lfile):
    with _mocked(layouts.STANDARD_RCU, rcu_rapid_write=True) as m:
        await m.fs._put_file(lfile(BIG), PATH)
    assert m.upload_file.await_args.kwargs["storage_class"] == "RAPID"


@pytest.mark.asyncio
async def test_put_file_retries_without_rapid_after_rcu_disabled(lfile):
    callback = mock.Mock()
    with _mocked(layouts.STANDARD_RCU, rcu_rapid_write=True) as m:
        m.upload_file.side_effect = [api_exceptions.InvalidArgument("rejected"), None]
        m.lookup.return_value = layouts.STANDARD
        await m.fs._put_file(lfile(BIG), PATH, callback=callback)
    assert [c.kwargs["storage_class"] for c in m.upload_file.await_args_list] == [
        "RAPID",
        None,
    ]
    assert m.fs._storage_layout_cache[BUCKET] == layouts.STANDARD
    callback.set_size.assert_called_once_with(THRESHOLD)
    callback.absolute_update.assert_called_once_with(THRESHOLD)


@pytest.mark.asyncio
async def test_put_file_directory_and_generation_keep_the_existing_path(
    lfile, tmp_path
):
    # Only a version-aware filesystem reads "#123" as a generation.
    with _mocked(
        layouts.STANDARD, version_aware=True, parallel_upload_threshold=0
    ) as m:
        await m.fs._put_file(str(tmp_path), PATH)  # a directory
        await m.fs._put_file(
            lfile(BIG), f"{PATH}#123"
        )  # a path that names a generation
        await m.fs._put_file(lfile(BIG), f"{BUCKET}/")  # no object name
        await m.fs._put_file(os.devnull, PATH)  # not a regular file
    m.upload_file.assert_not_awaited()
    assert m.http_put.await_count == 4


def test_put_file_sync_uploads_a_path_object(sdk, tmp_path):
    lpath = tmp_path / "data.json"
    lpath.write_bytes(BIG)
    fs = _fs(parallel_upload=True, parallel_upload_threshold=THRESHOLD)
    fs._grpc_client = mock.MagicMock()
    fs._storage_layout_cache[BUCKET] = layouts.STANDARD
    fs.dircache[BUCKET] = []
    fs.put_file(lpath, f"gs://{PATH}")
    call = sdk.upload_chunks_concurrently_from_filename.await_args
    assert call.args == (fs._grpc_client, BUCKET, "key", str(lpath))
    assert call.kwargs["blob"].content_type == "application/json"
    assert BUCKET not in fs.dircache


@pytest.mark.asyncio
async def test_put_file_failure_reports_no_progress(lfile):
    callback = mock.Mock()
    error = api_exceptions.InvalidArgument("rejected")
    with _mocked(layouts.STANDARD) as m:
        m.upload_file.side_effect = error
        with pytest.raises(api_exceptions.InvalidArgument) as excinfo:
            await m.fs._put_file(lfile(BIG), PATH, callback=callback)
    assert excinfo.value is error
    callback.absolute_update.assert_not_called()
    m.cache_update.assert_not_awaited()


@pytest.mark.asyncio
async def test_put_file_forwards_kms_key_name(lfile):
    kms_key = "projects/p/locations/l/keyRings/r/cryptoKeys/k"
    with _mocked(layouts.STANDARD) as m:
        await m.fs._put_file(lfile(BIG), PATH, kms_key_name=kms_key)
    assert _route(m) == PARALLEL
    assert m.upload_file.await_args.kwargs["kms_key_name"] == kms_key


@pytest.mark.asyncio
async def test_put_file_rejects_kms_key_name_on_rcu_rapid_write(lfile):
    kms_key = "projects/p/locations/l/keyRings/r/cryptoKeys/k"
    callback = mock.Mock()
    with _mocked(layouts.STANDARD_RCU, rcu_rapid_write=True) as m:
        with pytest.raises(
            NotImplementedError, match="not supported for gRPC RAPID uploads"
        ):
            await m.fs._put_file(
                lfile(BIG), PATH, callback=callback, kms_key_name=kms_key
            )
    m.upload_file.assert_not_awaited()
    callback.set_size.assert_not_called()
    callback.absolute_update.assert_not_called()
    m.cache_update.assert_not_awaited()


@pytest.mark.asyncio
async def test_put_file_rejects_kms_key_name_with_crypto_key_version(lfile):
    versioned_key = "projects/p/locations/l/keyRings/r/cryptoKeys/k/cryptoKeyVersions/1"
    callback = mock.Mock()
    with _mocked(layouts.STANDARD) as m:
        with pytest.raises(
            ValueError, match="kms_key_name must not include /cryptoKeyVersions/"
        ):
            await m.fs._put_file(
                lfile(BIG), PATH, callback=callback, kms_key_name=versioned_key
            )
    m.upload_file.assert_not_awaited()
    callback.set_size.assert_not_called()
    callback.absolute_update.assert_not_called()
    m.cache_update.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("data", [BIG, SMALL], ids=["big", "small"])
@pytest.mark.parametrize(
    "name, value", [("concurrency", 0), ("parallel_upload_segment_size", "bad")]
)
async def test_put_file_rejects_invalid_per_call_options(lfile, name, value, data):
    callback = mock.Mock()
    with _mocked(layouts.STANDARD) as m:
        with pytest.raises(ValueError, match=name):
            await m.fs._put_file(lfile(data), PATH, callback=callback, **{name: value})
    m.upload_file.assert_not_awaited()
    m.http_put.assert_not_awaited()
    callback.set_size.assert_not_called()


@pytest.mark.asyncio
async def test_put_file_ignores_per_call_options_when_the_option_is_off(lfile):
    with _mocked(layouts.STANDARD, parallel_upload=False) as m:
        await m.fs._put_file(
            lfile(BIG), PATH, parallel_upload_segment_size="bad", concurrency=0
        )
    assert _route(m) == HTTP


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fs_kwargs, call_kwargs, warns",
    [
        ({}, {"consistency": "md5"}, True),
        ({"consistency": "crc32c"}, {}, True),
        ({}, {"consistency": "none"}, False),
    ],
)
async def test_put_file_warns_when_consistency_is_ignored(
    lfile, caplog, fs_kwargs, call_kwargs, warns
):
    with (
        _mocked(layouts.STANDARD, **fs_kwargs) as m,
        caplog.at_level(logging.WARNING, logger="gcsfs"),
    ):
        await m.fs._put_file(lfile(BIG), PATH, **call_kwargs)
    assert _route(m) == PARALLEL
    assert any("consistency" in r.getMessage() for r in caplog.records) is warns
