"""Unit tests for writes to Rapid Cache Ultra (RCU) buckets.

By default, writes to RCU buckets use the HTTP upload path and create
``STANDARD`` objects. With ``rcu_rapid_write=True`` they use the gRPC
appendable writer and create ``RAPID`` objects.

When an appendable write to a bucket cached as RCU fails to open,
ExtendedGcsFileSystem looks the bucket layout up once more. If RCU is no
longer enabled, it caches the fresh layout and raises
``RapidCacheUltraDisabledError``, which ``pipe_file``, ``put_file``,
``simple_upload`` and ``initiate_upload`` answer with one retry over HTTP;
otherwise the original error surfaces. Writers that open after the cached
layout was refreshed fail fast with the same error, and uploads that already
opened keep the ``finalize_on_close`` they resolved before opening. The gRPC
writer, the layout lookup, and the HTTP upload are mocked.
"""

import contextlib
import logging
from unittest import mock

import fsspec
import pytest
from google.api_core import exceptions as api_exceptions
from google.cloud.storage.asyncio.async_appendable_object_writer import (
    AsyncAppendableObjectWriter,
)

from gcsfs import core
from gcsfs.extended_gcsfs import (
    BucketLayout,
    ExtendedGcsFileSystem,
    RapidCacheUltraDisabledError,
    initiate_upload,
    simple_upload,
)
from gcsfs.tests import layouts
from gcsfs.tests.utils import is_real_gcs, tmpfile

pytestmark = pytest.mark.skipif(
    is_real_gcs(), reason="Unit tests with mocks; no real GCS needed."
)

BUCKET = "rcu-bucket"
PATH = f"{BUCKET}/key"
UPLOAD_URL = (
    f"https://storage.googleapis.com/upload/storage/v1/b/{BUCKET}/o?upload_id=1"
)


def _writer():
    """A mock writer whose open succeeded."""
    writer = mock.AsyncMock(spec=AsyncAppendableObjectWriter)
    writer._is_stream_open = True
    return writer


def _no_skip(fn, *args):
    """Call fn(*args) and fail (not skip) the test if it calls pytest.skip."""
    from _pytest.outcomes import Skipped

    try:
        return fn(*args)
    except Skipped as exc:
        pytest.fail(f"unexpected skip: {exc}")


@pytest.fixture(
    params=[
        (
            api_exceptions.InvalidArgument,
            "Project 123 may not create storageClass RAPID buckets with "
            "locationConstraint US-CENTRAL1.",
        ),
        (
            api_exceptions.FailedPrecondition,
            "Cannot write rapid object to zone: us-central1-a",
        ),
    ],
    ids=["new_object", "append"],
)
def open_error(request):
    """The error GCS returns when a writer opens after RCU was disabled.

    New objects fail with InvalidArgument, appends with FailedPrecondition.
    """
    error_cls, message = request.param
    return error_cls(message)


@contextlib.contextmanager
def _mocked_fs(**kwargs):
    """Yield ``(fs, init_aaow)`` with the gRPC client and layout lookup mocked.

    Tests seed ``fs._storage_layout_cache[BUCKET]``, set
    ``fs._get_bucket_layout.return_value`` to the layout a fresh lookup
    returns, and configure the ``gcsfs.zb_hns_utils.init_aaow`` mock.
    """
    fs = ExtendedGcsFileSystem(token="anon", skip_instance_cache=True, **kwargs)
    fs._grpc_client = mock.MagicMock()
    with (
        mock.patch.object(fs, "_get_grpc_client", new_callable=mock.AsyncMock),
        mock.patch.object(fs, "_get_bucket_layout", new_callable=mock.AsyncMock),
        mock.patch(
            "gcsfs.zb_hns_utils.init_aaow", new_callable=mock.AsyncMock
        ) as init_aaow,
    ):
        yield fs, init_aaow


@pytest.fixture
def rcu_fs():
    """``_mocked_fs`` with ``rcu_rapid_write=True``, so RCU writes use gRPC."""
    with _mocked_fs(rcu_rapid_write=True) as fs_and_init_aaow:
        yield fs_and_init_aaow


@pytest.fixture
def default_fs():
    """``_mocked_fs`` with default options, so RCU writes use HTTP."""
    with _mocked_fs() as fs_and_init_aaow:
        yield fs_and_init_aaow


def test_rcu_rapid_write_is_off_by_default():
    fs = ExtendedGcsFileSystem(token="anon", skip_instance_cache=True)

    assert fs.rcu_rapid_write is False


def test_rcu_rapid_write_env_var_turns_it_on(monkeypatch):
    """FSSPEC_GCS_RCU_RAPID_WRITE=true reaches the constructor through fsspec's
    config, which parses the value as JSON."""
    monkeypatch.setenv("FSSPEC_GCS_RCU_RAPID_WRITE", "true")
    # fsspec reads FSSPEC_* variables when it is imported, so read them again.
    conf = {}
    fsspec.config.set_conf_env(conf)
    monkeypatch.setattr(fsspec.config, "conf", conf)

    fs = ExtendedGcsFileSystem(token="anon", skip_instance_cache=True)

    assert fs.rcu_rapid_write is True


@pytest.mark.parametrize(
    "raw_value, expected",
    [
        ("False", False),
        ("FALSE", False),
        ("0", False),
        ("off", False),
        ("", False),
        ("True", True),
        ("1", True),
        ("yes", True),
    ],
)
def test_rcu_rapid_write_normalizes_env_var_values(monkeypatch, raw_value, expected):
    """Non-JSON strings like 'False' and JSON ints like 1/0 normalize to bool."""
    monkeypatch.setenv("FSSPEC_GCS_RCU_RAPID_WRITE", raw_value)
    conf = {}
    fsspec.config.set_conf_env(conf)
    monkeypatch.setattr(fsspec.config, "conf", conf)

    fs = ExtendedGcsFileSystem(token="anon", skip_instance_cache=True)

    assert fs.rcu_rapid_write is expected
    assert fs._uses_grpc_write_path(layouts.STANDARD_RCU) is expected


def test_rcu_rapid_write_rejects_unrecognized_string():
    with pytest.raises(ValueError, match="rcu_rapid_write"):
        ExtendedGcsFileSystem(
            token="anon", skip_instance_cache=True, rcu_rapid_write="invalid"
        )


@pytest.mark.parametrize(
    "layout, expected_default, expected_rcu_rapid_write",
    [
        (layouts.ZONAL_HIERARCHICAL, True, True),
        (layouts.STANDARD_RCU, False, True),
        (layouts.HIERARCHICAL_RCU, False, True),
        (layouts.STANDARD, False, False),
        (layouts.STANDARD_HIERARCHICAL, False, False),
        (layouts.STANDARD_RAPID_CACHE, False, False),
        (layouts.HIERARCHICAL_RAPID_CACHE, False, False),
        (BucketLayout.UNKNOWN, False, False),
    ],
)
def test_uses_grpc_write_path(layout, expected_default, expected_rcu_rapid_write):
    """Zonal buckets always write over gRPC, RCU buckets only with rcu_rapid_write."""
    fs = ExtendedGcsFileSystem(token="anon", skip_instance_cache=True)
    rapid_fs = ExtendedGcsFileSystem(
        token="anon", skip_instance_cache=True, rcu_rapid_write=True
    )

    assert fs._uses_grpc_write_path(layout) is expected_default
    assert rapid_fs._uses_grpc_write_path(layout) is expected_rcu_rapid_write


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "layout, rcu_rapid_write, expected",
    [
        (layouts.ZONAL_HIERARCHICAL, False, True),
        (layouts.STANDARD_RCU, False, False),
        (layouts.STANDARD_RCU, True, True),
        (layouts.STANDARD, True, False),
    ],
)
async def test_should_use_grpc_write_path(layout, rcu_rapid_write, expected):
    """_should_use_grpc_write_path resolves the bucket layout once."""
    with _mocked_fs(rcu_rapid_write=rcu_rapid_write) as (fs, _):
        fs._get_bucket_layout.return_value = layout
        assert await fs._should_use_grpc_write_path(BUCKET) is expected
        fs._get_bucket_layout.assert_awaited_once_with(BUCKET)


@pytest.mark.parametrize("mode", ["wb", "xb", "ab"])
@pytest.mark.parametrize(
    "layout, rcu_rapid_write, expected_cls",
    [
        (layouts.STANDARD_RCU, False, "GCSFile"),
        (layouts.HIERARCHICAL_RCU, False, "GCSFile"),
        (layouts.STANDARD_RCU, True, "ZonalFile"),
        (layouts.HIERARCHICAL_RCU, True, "ZonalFile"),
        (layouts.ZONAL_HIERARCHICAL, False, "ZonalFile"),
        (layouts.ZONAL_HIERARCHICAL, True, "ZonalFile"),
    ],
)
def test_open_write_modes_follow_rcu_rapid_write(
    layout, rcu_rapid_write, expected_cls, mode
):
    """Write modes open a ZonalFile on Zonal buckets, and on RCU buckets only
    with rcu_rapid_write."""
    fs = ExtendedGcsFileSystem(
        token="anon", skip_instance_cache=True, rcu_rapid_write=rcu_rapid_write
    )
    fs._storage_layout_cache[BUCKET] = layout

    with (
        mock.patch("gcsfs.extended_gcsfs.GCSFile") as gcs_file,
        mock.patch("gcsfs.extended_gcsfs.ZonalFile") as zonal_file,
    ):
        f = fs.open(PATH, mode)

    assert (
        f is {"GCSFile": gcs_file, "ZonalFile": zonal_file}[expected_cls].return_value
    )


@pytest.mark.parametrize("rcu_rapid_write", [False, True])
@pytest.mark.parametrize("layout", [layouts.STANDARD_RCU, layouts.HIERARCHICAL_RCU])
def test_open_read_mode_on_rcu_uses_zonal_file(layout, rcu_rapid_write):
    """Reads from RCU buckets use ZonalFile with or without rcu_rapid_write."""
    fs = ExtendedGcsFileSystem(
        token="anon", skip_instance_cache=True, rcu_rapid_write=rcu_rapid_write
    )
    fs._storage_layout_cache[BUCKET] = layout

    with (
        mock.patch("gcsfs.extended_gcsfs.GCSFile") as gcs_file,
        mock.patch("gcsfs.extended_gcsfs.ZonalFile") as zonal_file,
    ):
        f = fs.open(PATH, "rb")

    assert f is zonal_file.return_value
    gcs_file.assert_not_called()


def test_open_append_on_rcu_by_default_overwrites_over_http(default_fs):
    """Without rcu_rapid_write, "ab" on an RCU bucket warns and overwrites over
    HTTP, as on other buckets, and finalize_on_close has no effect."""
    fs, init_aaow = default_fs
    fs._storage_layout_cache[BUCKET] = layouts.STANDARD_RCU

    with (
        mock.patch.object(
            fs,
            "_call",
            new_callable=mock.AsyncMock,
            return_value=({"Location": UPLOAD_URL}, b""),
        ) as http_call,
        # fsspec's sync call() wraps the original _call, so patch it too.
        mock.patch.object(
            fs, "call", return_value=({}, '{"generation": "1"}')
        ) as http_upload,
        pytest.warns(UserWarning, match="Append mode 'a' is not supported"),
    ):
        with fs.open(PATH, "ab", finalize_on_close=True) as f:
            f.write(b"data")

    assert type(f) is core.GCSFile
    assert f.mode == "wb"
    init_aaow.assert_not_awaited()
    assert http_call.await_args.kwargs["uploadType"] == "resumable"
    assert http_upload.call_args.args[:2] == ("POST", UPLOAD_URL)
    # call_args holds a memoryview of the upload buffer.
    http_upload.reset_mock()


def test_open_http_gcs_file_does_not_leak_finalize_on_close_into_kwargs(default_fs):
    """HTTP-backed GCSFile handles from _open must not carry finalize_on_close in kwargs."""
    fs, _ = default_fs
    fs._storage_layout_cache[BUCKET] = layouts.STANDARD_RCU

    with (
        mock.patch.object(
            fs,
            "_call",
            new_callable=mock.AsyncMock,
            return_value=({"Location": UPLOAD_URL}, b""),
        ),
        mock.patch.object(fs, "call", return_value=({}, '{"generation": "1"}')),
    ):
        with fs.open(PATH, "wb") as default_handle:
            pass
        with fs.open(PATH, "wb", finalize_on_close=False) as explicit_handle:
            pass

    assert type(default_handle) is core.GCSFile
    assert type(explicit_handle) is core.GCSFile
    assert "finalize_on_close" not in default_handle.kwargs
    assert "finalize_on_close" not in explicit_handle.kwargs


async def _fake_http_call(*args, **kwargs):
    """Start a resumable upload, or complete a multipart upload."""
    if kwargs.get("uploadType") == "resumable":
        return {"Location": UPLOAD_URL}, b""
    return {}


@pytest.mark.asyncio
@pytest.mark.parametrize("layout", [layouts.STANDARD_RCU, layouts.HIERARCHICAL_RCU])
@pytest.mark.parametrize(
    "write, upload_type",
    [
        ("pipe_file", "multipart"),
        ("put_file", "multipart"),
        ("simple_upload", "multipart"),
        ("initiate_upload", "resumable"),
        ("core.simple_upload", "multipart"),
        ("core.initiate_upload", "resumable"),
    ],
)
async def test_rcu_writes_use_http_by_default(
    default_fs, layout, write, upload_type, tmp_path
):
    """Without rcu_rapid_write, every write path uploads to RCU buckets over HTTP,
    even with finalize_on_close=True."""
    fs, init_aaow = default_fs
    fs._storage_layout_cache[BUCKET] = layout
    lpath = tmp_path / "data"
    lpath.write_bytes(b"data")

    with mock.patch.object(
        fs, "_call", new_callable=mock.AsyncMock, side_effect=_fake_http_call
    ) as http_call:
        if write == "pipe_file":
            await fs._pipe_file(PATH, b"data", finalize_on_close=True)
        elif write == "put_file":
            await fs._put_file(str(lpath), PATH, finalize_on_close=True)
        elif write == "simple_upload":
            await simple_upload(fs, BUCKET, "key", b"data", finalize_on_close=True)
        elif write == "initiate_upload":
            await initiate_upload(fs, BUCKET, "key")
        elif write == "core.simple_upload":
            await core.simple_upload(fs, BUCKET, "key", b"data")
        else:
            await core.initiate_upload(fs, BUCKET, "key")

    init_aaow.assert_not_awaited()
    http_call.assert_awaited_once()
    assert http_call.await_args.kwargs["uploadType"] == upload_type


@pytest.mark.asyncio
async def test_rcu_http_writes_keep_upload_params_by_default(default_fs, caplog):
    """Without rcu_rapid_write, RCU uploads keep the metadata, content type and
    KMS key that the appendable writer would ignore."""
    fs, init_aaow = default_fs
    fs._storage_layout_cache[BUCKET] = layouts.STANDARD_RCU

    with (
        mock.patch.object(
            fs, "_call", new_callable=mock.AsyncMock, return_value={}
        ) as http_call,
        caplog.at_level(logging.WARNING, logger="gcsfs"),
    ):
        await simple_upload(
            fs,
            BUCKET,
            "key",
            b"data",
            metadatain={"k": "v"},
            content_type="text/plain",
            kms_key_name="kms-key",
        )

    init_aaow.assert_not_awaited()
    request = http_call.await_args.kwargs
    assert request["kmsKeyName"] == "kms-key"
    assert b'"metadata": {"k": "v"}' in request["data"]
    assert b"Content-Type: text/plain" in request["data"]
    assert not any("will be ignored" in r.getMessage() for r in caplog.records)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "cached, fresh",
    [
        (layouts.STANDARD_RCU, layouts.STANDARD),
        (layouts.HIERARCHICAL_RCU, layouts.STANDARD_HIERARCHICAL),
    ],
)
async def test_init_aaow_rcu_disabled_raises_and_refreshes_cache(
    rcu_fs, open_error, cached, fresh
):
    """An open failure after RCU was disabled caches the fresh layout."""
    fs, init_aaow = rcu_fs
    fs._storage_layout_cache[BUCKET] = cached
    init_aaow.side_effect = open_error
    fs._get_bucket_layout.return_value = fresh

    with pytest.raises(
        RapidCacheUltraDisabledError,
        match=f"Rapid Cache Ultra is no longer enabled on bucket '{BUCKET}'",
    ) as excinfo:
        await fs._init_aaow(BUCKET, "key")

    assert excinfo.value.__cause__ is open_error
    assert fs._storage_layout_cache[BUCKET] == fresh
    fs._get_bucket_layout.assert_awaited_once_with(BUCKET)
    assert init_aaow.await_args.kwargs["storage_class"] == "RAPID"


@pytest.mark.asyncio
async def test_init_aaow_rcu_still_enabled_reraises(rcu_fs, open_error):
    """An open failure while RCU is still enabled re-raises the original error."""
    fs, init_aaow = rcu_fs
    fs._storage_layout_cache[BUCKET] = layouts.STANDARD_RCU
    init_aaow.side_effect = open_error
    fs._get_bucket_layout.return_value = layouts.STANDARD_RCU

    with pytest.raises(type(open_error)) as excinfo:
        await fs._init_aaow(BUCKET, "key")

    assert excinfo.value is open_error
    assert fs._storage_layout_cache[BUCKET] == layouts.STANDARD_RCU
    fs._get_bucket_layout.assert_awaited_once_with(BUCKET)


@pytest.mark.asyncio
async def test_init_aaow_rcu_open_failure_lookup_unknown_reraises(rcu_fs, open_error):
    """A failed re-check lookup keeps the cached RCU layout and the original error."""
    fs, init_aaow = rcu_fs
    fs._storage_layout_cache[BUCKET] = layouts.STANDARD_RCU
    init_aaow.side_effect = open_error
    fs._get_bucket_layout.return_value = BucketLayout.UNKNOWN

    with pytest.raises(type(open_error)) as excinfo:
        await fs._init_aaow(BUCKET, "key")

    assert excinfo.value is open_error
    assert fs._storage_layout_cache[BUCKET] == layouts.STANDARD_RCU
    fs._get_bucket_layout.assert_awaited_once_with(BUCKET)


@pytest.mark.asyncio
async def test_init_aaow_zonal_open_failure_skips_recheck(rcu_fs, open_error):
    """Open failures on zonal buckets surface without a layout re-check."""
    fs, init_aaow = rcu_fs
    fs._storage_layout_cache[BUCKET] = layouts.ZONAL_HIERARCHICAL
    init_aaow.side_effect = open_error

    with pytest.raises(type(open_error)) as excinfo:
        await fs._init_aaow(BUCKET, "key")

    assert excinfo.value is open_error
    fs._get_bucket_layout.assert_not_awaited()


@pytest.mark.asyncio
async def test_init_aaow_precondition_failure_skips_recheck(rcu_fs):
    """A failed generation precondition on an RCU bucket is not a layout change."""
    fs, init_aaow = rcu_fs
    fs._storage_layout_cache[BUCKET] = layouts.STANDARD_RCU
    precondition_error = FileExistsError(PATH)
    init_aaow.side_effect = precondition_error

    with pytest.raises(FileExistsError) as excinfo:
        await fs._init_aaow(BUCKET, "key", if_generation_match=0)

    assert excinfo.value is precondition_error
    fs._get_bucket_layout.assert_not_awaited()


@pytest.mark.asyncio
async def test_init_aaow_layout_already_refreshed_raises_without_open(rcu_fs):
    """A writer opening after the cached layout lost RCU fails without an open."""
    fs, init_aaow = rcu_fs
    fs._storage_layout_cache[BUCKET] = layouts.STANDARD

    with pytest.raises(RapidCacheUltraDisabledError):
        await fs._init_aaow(BUCKET, "key")

    init_aaow.assert_not_awaited()
    fs._get_bucket_layout.assert_not_awaited()


@pytest.mark.asyncio
async def test_init_aaow_unknown_layout_still_opens(rcu_fs):
    """A failed layout lookup does not block the open, which skips RAPID."""
    fs, init_aaow = rcu_fs
    fs._get_bucket_layout.return_value = BucketLayout.UNKNOWN
    writer = _writer()
    init_aaow.return_value = writer

    assert await fs._init_aaow(BUCKET, "key") is writer

    init_aaow.assert_awaited_once()
    assert init_aaow.await_args.kwargs["storage_class"] is None


@pytest.mark.asyncio
async def test_init_aaow_rcu_failure_reuses_layout_refreshed_by_another_writer(
    rcu_fs, open_error
):
    """An open failing after another writer refreshed the cache sends no lookup."""
    fs, init_aaow = rcu_fs
    fs._storage_layout_cache[BUCKET] = layouts.STANDARD_RCU
    fs._get_bucket_layout.return_value = layouts.STANDARD

    def refresh_then_fail(*args, **kwargs):
        # Another writer refreshes the cached layout while this open is in flight.
        fs._storage_layout_cache[BUCKET] = layouts.STANDARD
        raise open_error

    init_aaow.side_effect = refresh_then_fail

    with pytest.raises(RapidCacheUltraDisabledError) as excinfo:
        await fs._init_aaow(BUCKET, "key")

    assert excinfo.value.__cause__ is open_error
    fs._get_bucket_layout.assert_not_awaited()


@pytest.mark.asyncio
async def test_pipe_file_retries_over_http_when_rcu_disabled(
    rcu_fs, open_error, caplog
):
    """pipe_file retries once over HTTP after RCU was disabled on the bucket."""
    fs, init_aaow = rcu_fs
    fs._storage_layout_cache[BUCKET] = layouts.STANDARD_RCU
    init_aaow.side_effect = open_error
    fs._get_bucket_layout.return_value = layouts.STANDARD

    with (
        mock.patch(
            "gcsfs.core.GCSFileSystem._pipe_file", new_callable=mock.AsyncMock
        ) as http_pipe_file,
        caplog.at_level(logging.WARNING, logger="gcsfs"),
    ):
        await fs._pipe_file(PATH, b"data", mode="create", metadata={"k": "v"})

    http_pipe_file.assert_awaited_once()
    assert http_pipe_file.await_args.args[:2] == (PATH, b"data")
    assert http_pipe_file.await_args.kwargs["mode"] == "create"
    assert http_pipe_file.await_args.kwargs["metadata"] == {"k": "v"}
    assert any(
        r.name == "gcsfs"
        and r.levelno == logging.WARNING
        and "Retrying the write over HTTP" in r.getMessage()
        for r in caplog.records
    )
    # The HTTP retry honors metadata, so the ignored-parameters warning is not logged.
    assert not any(
        "These parameters will be ignored" in r.getMessage() for r in caplog.records
    )


@pytest.mark.asyncio
async def test_pipe_file_warns_on_ignored_params_when_writer_opens(rcu_fs, caplog):
    """The ignored-parameters warning is still logged when the gRPC writer opens."""
    fs, init_aaow = rcu_fs
    fs._storage_layout_cache[BUCKET] = layouts.STANDARD_RCU
    init_aaow.return_value = _writer()

    with (
        mock.patch(
            "gcsfs.core.GCSFileSystem._pipe_file", new_callable=mock.AsyncMock
        ) as http_pipe_file,
        caplog.at_level(logging.WARNING, logger="gcsfs"),
    ):
        await fs._pipe_file(PATH, b"data", metadata={"k": "v"})

    http_pipe_file.assert_not_awaited()
    assert any(
        r.name == "gcsfs"
        and r.levelno == logging.WARNING
        and "These parameters will be ignored" in r.getMessage()
        for r in caplog.records
    )


@pytest.mark.asyncio
async def test_put_file_retries_over_http_when_rcu_disabled(rcu_fs, open_error):
    """put_file retries once over HTTP after RCU was disabled on the bucket."""
    fs, init_aaow = rcu_fs
    fs._storage_layout_cache[BUCKET] = layouts.STANDARD_RCU
    init_aaow.side_effect = open_error
    fs._get_bucket_layout.return_value = layouts.STANDARD

    with tmpfile() as lpath:
        with open(lpath, "wb") as f:
            f.write(b"data")
        with mock.patch(
            "gcsfs.core.GCSFileSystem._put_file", new_callable=mock.AsyncMock
        ) as http_put_file:
            await fs._put_file(lpath, PATH)

    http_put_file.assert_awaited_once()
    assert http_put_file.await_args.args[:2] == (lpath, PATH)


@pytest.mark.asyncio
@pytest.mark.parametrize("func", ["simple_upload", "initiate_upload"])
async def test_module_upload_helpers_retry_over_http_when_rcu_disabled(
    rcu_fs, open_error, caplog, func
):
    """simple_upload and initiate_upload retry over HTTP after RCU was disabled."""
    fs, init_aaow = rcu_fs
    fs._storage_layout_cache[BUCKET] = layouts.STANDARD_RCU
    init_aaow.side_effect = open_error
    fs._get_bucket_layout.return_value = layouts.STANDARD

    with (
        mock.patch(f"gcsfs.core.{func}", new_callable=mock.AsyncMock) as http_func,
        caplog.at_level(logging.WARNING, logger="gcsfs"),
    ):
        if func == "simple_upload":
            await simple_upload(fs, BUCKET, "key", b"data", {"k": "v"})
        else:
            await initiate_upload(fs, BUCKET, "key", metadata={"k": "v"})

    http_func.assert_awaited_once()
    assert {"k": "v"} in http_func.await_args.args
    assert any("Retrying the write over HTTP" in r.getMessage() for r in caplog.records)
    assert not any("will be ignored" in r.getMessage() for r in caplog.records)


@pytest.mark.asyncio
async def test_core_simple_upload_hand_off_ends_after_one_http_post(rcu_fs, open_error):
    """core.simple_upload hands off to gRPC and back once, then posts over HTTP."""
    fs, init_aaow = rcu_fs
    fs._storage_layout_cache[BUCKET] = layouts.STANDARD_RCU
    init_aaow.side_effect = open_error
    fs._get_bucket_layout.return_value = layouts.STANDARD

    with mock.patch.object(
        fs, "_call", new_callable=mock.AsyncMock, return_value={}
    ) as http_call:
        await core.simple_upload(fs, BUCKET, "key", b"data")

    http_call.assert_awaited_once()
    assert http_call.await_args.kwargs["uploadType"] == "multipart"
    init_aaow.assert_awaited_once()
    assert fs._storage_layout_cache[BUCKET] == layouts.STANDARD


@pytest.mark.asyncio
async def test_pipe_file_rcu_still_enabled_propagates_error(rcu_fs, open_error):
    """pipe_file surfaces the open error without an HTTP retry while RCU is enabled."""
    fs, init_aaow = rcu_fs
    fs._storage_layout_cache[BUCKET] = layouts.STANDARD_RCU
    init_aaow.side_effect = open_error
    fs._get_bucket_layout.return_value = layouts.STANDARD_RCU

    with mock.patch(
        "gcsfs.core.GCSFileSystem._pipe_file", new_callable=mock.AsyncMock
    ) as http_pipe_file:
        with pytest.raises(type(open_error)) as excinfo:
            await fs._pipe_file(PATH, b"data")

    assert excinfo.value is open_error
    fs._get_bucket_layout.assert_awaited_once_with(BUCKET)
    http_pipe_file.assert_not_awaited()


@pytest.mark.asyncio
async def test_pipe_file_append_failure_after_open_does_not_retry(rcu_fs, open_error):
    """A failure after the writer opened triggers neither a re-check nor a retry."""
    fs, init_aaow = rcu_fs
    fs._storage_layout_cache[BUCKET] = layouts.STANDARD_RCU
    writer = mock.AsyncMock(spec=AsyncAppendableObjectWriter)
    writer.append.side_effect = open_error
    init_aaow.return_value = writer

    with mock.patch(
        "gcsfs.core.GCSFileSystem._pipe_file", new_callable=mock.AsyncMock
    ) as http_pipe_file:
        with pytest.raises(type(open_error)) as excinfo:
            await fs._pipe_file(PATH, b"data")

    assert excinfo.value is open_error
    writer.close.assert_awaited_once()
    fs._get_bucket_layout.assert_not_awaited()
    http_pipe_file.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["pipe_file", "put_file", "simple_upload"])
async def test_layout_invalidated_during_upload_keeps_finalize_on_close(rcu_fs, method):
    """Invalidating the layout mid-upload does not change finalize_on_close."""
    fs, init_aaow = rcu_fs
    fs._storage_layout_cache[BUCKET] = layouts.STANDARD_RCU
    fs._get_bucket_layout.return_value = layouts.STANDARD
    writer = _writer()

    def invalidate(*args, **kwargs):
        fs.invalidate_storage_layout_cache(BUCKET)

    writer.append.side_effect = invalidate
    init_aaow.return_value = writer

    if method == "pipe_file":
        await fs._pipe_file(PATH, b"data")
    elif method == "put_file":
        with tmpfile() as lpath:
            with open(lpath, "wb") as f:
                f.write(b"data")
            await fs._put_file(lpath, PATH)
    else:
        await simple_upload(fs, BUCKET, "key", b"data")

    assert init_aaow.await_args.kwargs["storage_class"] == "RAPID"
    writer.close.assert_awaited_once_with(finalize_on_close=True)
    fs._get_bucket_layout.assert_not_awaited()


def test_zonal_file_write_after_rcu_disabled_raises_clear_error(rcu_fs, open_error):
    """A write on an open handle raises the RCU error and close does not reopen."""
    fs, init_aaow = rcu_fs
    fs._storage_layout_cache[BUCKET] = layouts.STANDARD_RCU
    init_aaow.side_effect = open_error
    fs._get_bucket_layout.return_value = layouts.STANDARD

    with pytest.raises(RapidCacheUltraDisabledError):
        with fs.open(PATH, "wb") as f:
            f.write(b"x")

    assert init_aaow.await_count == 1
    assert fs._sync_lookup_bucket_layout(BUCKET) == layouts.STANDARD


def test_zonal_file_opened_before_refresh_raises_clear_error(rcu_fs):
    """A handle opened while the bucket was cached as RCU fails fast after a refresh."""
    fs, init_aaow = rcu_fs
    fs._storage_layout_cache[BUCKET] = layouts.STANDARD_RCU
    init_aaow.return_value = _writer()
    f = fs.open(PATH, "wb")
    # Another write found RCU disabled and refreshed the cached layout.
    fs._storage_layout_cache[BUCKET] = layouts.STANDARD

    with pytest.raises(RapidCacheUltraDisabledError):
        with f:
            f.write(b"x")

    init_aaow.assert_not_awaited()


def test_zonal_file_commit_after_rcu_error_raises_value_error(rcu_fs, open_error):
    """commit() on a handle whose writer could not open raises; close() still works."""
    fs, init_aaow = rcu_fs
    fs._storage_layout_cache[BUCKET] = layouts.STANDARD_RCU
    init_aaow.side_effect = open_error
    fs._get_bucket_layout.return_value = layouts.STANDARD
    f = fs.open(PATH, "wb")
    with pytest.raises(RapidCacheUltraDisabledError):
        f.write(b"x")

    with pytest.raises(ValueError, match="I/O operation on closed file"):
        f.commit()

    assert init_aaow.await_count == 1
    f.close()


@pytest.mark.parametrize(
    "error, fresh",
    [
        ("open_error", layouts.STANDARD_RCU),
        ("open_error", BucketLayout.UNKNOWN),
        (FileExistsError(PATH), None),
    ],
)
def test_zonal_file_failed_open_is_not_retried_on_close(
    rcu_fs, open_error, error, fresh
):
    """close() after a failed writer open neither reopens nor re-checks the layout."""
    fs, init_aaow = rcu_fs
    fs._storage_layout_cache[BUCKET] = layouts.STANDARD_RCU
    init_aaow.side_effect = open_error if error == "open_error" else error
    fs._get_bucket_layout.return_value = fresh

    with pytest.raises(type(init_aaow.side_effect)):
        with fs.open(PATH, "wb") as f:
            f.write(b"x")

    assert init_aaow.await_count == 1
    assert fs._get_bucket_layout.await_count == (1 if error == "open_error" else 0)


def test_zonal_file_empty_after_rcu_disabled_raises_on_close(rcu_fs, open_error):
    """Closing an empty handle after RCU was disabled raises once and closes it."""
    fs, init_aaow = rcu_fs
    fs._storage_layout_cache[BUCKET] = layouts.STANDARD_RCU
    init_aaow.side_effect = open_error
    fs._get_bucket_layout.return_value = layouts.STANDARD

    with pytest.raises(RapidCacheUltraDisabledError):
        with fs.open(PATH, "wb") as f:
            pass

    assert f.closed
    f.close()  # A second close is a no-op.
    assert init_aaow.await_count == 1


def test_gcs_file_opened_before_rcu_enabled_keeps_http_upload(rcu_fs):
    """A GCSFile opened before the layout changed to RCU still uploads over HTTP."""
    fs, init_aaow = rcu_fs
    fs._storage_layout_cache[BUCKET] = layouts.STANDARD
    fs._get_bucket_layout.return_value = layouts.STANDARD_RCU
    location = (
        f"https://storage.googleapis.com/upload/storage/v1/b/{BUCKET}/o?upload_id=1"
    )

    with (
        mock.patch.object(
            fs,
            "_call",
            new_callable=mock.AsyncMock,
            return_value=({"Location": location}, b""),
        ) as http_call,
        # fsspec's sync call() wraps the original _call, so patch it too.
        mock.patch.object(
            fs, "call", return_value=({}, '{"generation": "1"}')
        ) as http_upload,
    ):
        f = fs.open(PATH, "wb")
        # RCU was enabled on the bucket after the handle opened.
        fs.invalidate_storage_layout_cache(BUCKET)
        with f:
            f.write(b"data")

    init_aaow.assert_not_awaited()
    http_call.assert_awaited_once()
    assert http_call.await_args.kwargs["uploadType"] == "resumable"
    assert http_upload.call_args.args[:2] == ("POST", location)
    # call_args holds a memoryview of the upload buffer.
    http_upload.reset_mock()


def test_cross_zone_rcu_bucket_setting_ignores_xdist_worker_suffix(monkeypatch):
    """TEST_CROSS_ZONE_RCU_BUCKET reads GCSFS_CROSS_ZONE_RCU_TEST_BUCKET without -gwN."""
    import importlib

    from gcsfs.tests import settings

    try:
        with monkeypatch.context() as mp:
            mp.setenv("PYTEST_XDIST_WORKER", "gw2")
            mp.delenv("GCSFS_CROSS_ZONE_RCU_TEST_BUCKET", raising=False)
            importlib.reload(settings)
            assert settings.TEST_CROSS_ZONE_RCU_BUCKET == ""

            mp.setenv("GCSFS_CROSS_ZONE_RCU_TEST_BUCKET", "my-cross-zone-rcu")
            importlib.reload(settings)
            assert settings.TEST_CROSS_ZONE_RCU_BUCKET == "my-cross-zone-rcu"
    finally:
        importlib.reload(settings)


@pytest.mark.parametrize("rcu_rapid_write", [False, True])
@pytest.mark.parametrize("layout", layouts.ALL_BUCKET_LAYOUTS)
def test_skip_if_zonal_skips_only_grpc_write_layouts(layout, rcu_rapid_write):
    """skip_if_zonal skips HTTP-write tests only when writes route over gRPC."""
    from _pytest.outcomes import Skipped

    from gcsfs.tests import conftest

    fake_gcs = ExtendedGcsFileSystem(
        token="anon", skip_instance_cache=True, rcu_rapid_write=rcu_rapid_write
    )
    expected_skip = layout.is_zonal or (layout.is_rcu and rcu_rapid_write)

    with mock.patch.object(fake_gcs, "_sync_lookup_bucket_layout", return_value=layout):
        if expected_skip:
            with pytest.raises(Skipped):
                conftest.skip_if_zonal.__wrapped__(fake_gcs)
        else:
            try:
                conftest.skip_if_zonal.__wrapped__(fake_gcs)
            except Skipped as exc:
                pytest.fail(f"Unexpected skip for {layout} ({rcu_rapid_write=}): {exc}")


@pytest.mark.parametrize(
    "live_layout, expected_rcu_rapid_write",
    [
        (layouts.STANDARD_RCU, True),
        (layouts.HIERARCHICAL_RCU, True),
        (layouts.ZONAL_HIERARCHICAL, False),
    ],
)
def test_zonal_write_mocks_live_rcu_enables_rcu_rapid_write(
    monkeypatch, live_layout, expected_rcu_rapid_write
):
    """Live zonal_write_mocks sets rcu_rapid_write on RCU fixtures and restores it."""
    from types import SimpleNamespace

    from gcsfs.tests import conftest

    fake_extended = ExtendedGcsFileSystem(
        token="anon", skip_instance_cache=True, rcu_rapid_write=False
    )
    fake_async = ExtendedGcsFileSystem(
        token="anon", skip_instance_cache=True, rcu_rapid_write=False
    )
    fake_lookup_fs = mock.MagicMock()
    fake_lookup_fs._sync_lookup_bucket_layout.return_value = live_layout

    monkeypatch.setattr(conftest, "is_real_gcs", lambda: True)
    monkeypatch.setattr(conftest, "_live_bucket_layouts", {})
    monkeypatch.setattr(conftest, "ExtendedGcsFileSystem", lambda **kw: fake_lookup_fs)
    monkeypatch.setattr(conftest, "_close_gcs", lambda fs: None)

    fixtures = {"extended_gcsfs": fake_extended, "async_gcs": fake_async}
    request = SimpleNamespace(
        fixturenames=("extended_gcsfs", "async_gcs"),
        getfixturevalue=fixtures.__getitem__,
    )

    gen = conftest.zonal_write_mocks.__wrapped__(request)
    assert _no_skip(next, gen) is None
    assert fake_extended.rcu_rapid_write is expected_rcu_rapid_write
    assert fake_async.rcu_rapid_write is expected_rcu_rapid_write
    with pytest.raises(StopIteration):
        next(gen)

    assert fake_extended.rcu_rapid_write is False
    assert fake_async.rcu_rapid_write is False


@pytest.mark.parametrize("live_layout", layouts.ALL_BUCKET_LAYOUTS)
def test_zonal_write_mocks_live_gcs_matches_layout_or_grpc_data_path(
    monkeypatch, live_layout
):
    """On real GCS, zonal_write_mocks runs matching param layouts and unparametrized gRPC layouts."""
    from types import SimpleNamespace

    from _pytest.outcomes import Skipped

    from gcsfs.tests import conftest

    monkeypatch.setattr(conftest, "is_real_gcs", lambda: True)
    monkeypatch.setattr(conftest, "_live_bucket_layouts", {})
    fake_fs = mock.MagicMock()
    fake_fs._sync_lookup_bucket_layout.return_value = live_layout
    monkeypatch.setattr(conftest, "ExtendedGcsFileSystem", lambda **kw: fake_fs)
    monkeypatch.setattr(conftest, "_close_gcs", lambda fs: None)

    # Unparametrized request: runs iff live_layout.uses_grpc_data_path.
    unparam_gen = conftest.zonal_write_mocks.__wrapped__(SimpleNamespace())
    if live_layout.uses_grpc_data_path:
        assert _no_skip(next, unparam_gen) is None
    else:
        with pytest.raises(Skipped):
            next(unparam_gen)

    # Parametrized request: runs iff request.param == live_layout.
    for target in conftest.RAPID_WRITE_LAYOUTS:
        param_gen = conftest.zonal_write_mocks.__wrapped__(
            SimpleNamespace(param=target)
        )
        if live_layout == target:
            assert _no_skip(next, param_gen) is None
        else:
            with pytest.raises(Skipped):
                next(param_gen)


@pytest.mark.parametrize(
    "fixture_name, expected_rcu_rapid_write",
    [("rcu_fs", None), ("rcu_rapid_fs", True)],
)
@pytest.mark.parametrize("layout", layouts.ALL_BUCKET_LAYOUTS)
def test_rcu_fs_fixture_skips_non_rcu_buckets(
    monkeypatch, layout, fixture_name, expected_rcu_rapid_write
):
    """rcu_fs and rcu_rapid_fs yield ExtendedGcsFileSystem only on RCU buckets."""
    from _pytest.outcomes import Skipped

    from gcsfs.tests import test_rcu

    fake_fs = mock.MagicMock()
    fake_fs._sync_lookup_bucket_layout.return_value = layout
    created_kwargs = []
    closed = []

    def _make_fs(**kw):
        created_kwargs.append(kw)
        return fake_fs

    monkeypatch.setattr(test_rcu, "ExtendedGcsFileSystem", _make_fs)
    monkeypatch.setattr(test_rcu, "_close_gcs", lambda fs: closed.append(fs))

    fixture = getattr(test_rcu, fixture_name)
    gen = fixture.__wrapped__()
    if layout.is_rcu:
        assert _no_skip(next, gen) is fake_fs
        with pytest.raises(StopIteration):
            next(gen)
    else:
        with pytest.raises(Skipped):
            next(gen)
    assert created_kwargs[0].get("rcu_rapid_write") is expected_rcu_rapid_write
    assert closed == [fake_fs]


def test_gcs_factory_closes_created_instances_on_teardown(monkeypatch):
    """Function-scoped gcs_factory closes every unique instance it created on teardown."""
    from gcsfs.tests import conftest

    fs1 = ExtendedGcsFileSystem(token="anon", skip_instance_cache=True)
    fs2 = ExtendedGcsFileSystem(token="anon", skip_instance_cache=True)
    grpc_close = mock.AsyncMock()
    ctrl_close = mock.AsyncMock()
    fs1._grpc_client = mock.MagicMock()
    fs1._grpc_client.grpc_client.transport.close = grpc_close
    fs2._storage_control_client = mock.MagicMock()
    fs2._storage_control_client.transport.close = ctrl_close

    calls = iter([fs1, fs2, fs1])
    gen = conftest.gcs_factory.__wrapped__(lambda **kw: next(calls))
    factory = next(gen)

    assert factory() is fs1
    assert factory(requester_pays=True) is fs2
    assert factory() is fs1
    # Simulate a fixture (such as `gcs`) closing fs1 before gcs_factory teardown runs.
    conftest._close_gcs(fs1)

    with pytest.raises(StopIteration):
        next(gen)

    assert fs1._mrd_pool_cache._closed is True
    assert fs2._mrd_pool_cache._closed is True
    assert fs1._grpc_client is None
    assert fs2._storage_control_client is None
    grpc_close.assert_awaited_once()
    ctrl_close.assert_awaited_once()


@pytest.mark.parametrize("on_google", [False, True])
@pytest.mark.parametrize(
    "layout",
    [layouts.ZONAL_HIERARCHICAL, layouts.STANDARD_RCU, layouts.HIERARCHICAL_RCU],
)
def test_zonal_unfinalized_helpers_skip_live_rcu_and_mock_gcs(
    monkeypatch, on_google, layout
):
    """Unfinalized MRD tests skip on mock GCS, xfail on live RCU, and run on live Zonal."""
    from _pytest.outcomes import Skipped

    from gcsfs.tests import test_zonal

    fake_fs = ExtendedGcsFileSystem(token="anon", skip_instance_cache=True)
    monkeypatch.setattr(
        ExtendedGcsFileSystem, "on_google", property(lambda self: on_google)
    )
    request = mock.Mock()
    request.getfixturevalue.return_value = monkeypatch
    with mock.patch.object(fake_fs, "_sync_lookup_bucket_layout", return_value=layout):
        if not on_google:
            with pytest.raises(Skipped):
                test_zonal._require_live_non_rcu_zonal(fake_fs, request)
        else:
            _no_skip(test_zonal._require_live_non_rcu_zonal, fake_fs, request)
    live_rcu = on_google and layout.is_rcu
    assert fake_fs.rcu_rapid_write is live_rcu
    if live_rcu:
        (marker,) = request.applymarker.call_args.args
        assert marker.name == "xfail"
        assert marker.kwargs["strict"] is False
    else:
        request.applymarker.assert_not_called()


@pytest.mark.parametrize(
    "layout, expected_rcu_rapid_write",
    [
        (layouts.ZONAL_HIERARCHICAL, False),
        (layouts.STANDARD_RCU, True),
        (layouts.HIERARCHICAL_RCU, True),
    ],
)
def test_zonal_file_real_gcs_enables_rcu_rapid_write_for_rcu(
    layout, expected_rcu_rapid_write
):
    """TestZonalFileRealGCS opts live RCU fixtures into rcu_rapid_write and restores them."""
    from gcsfs.tests import test_zonal_file

    fake_fs = ExtendedGcsFileSystem(
        token="anon", skip_instance_cache=True, rcu_rapid_write=False
    )
    instance = test_zonal_file.TestZonalFileRealGCS()

    with mock.patch.object(fake_fs, "_sync_lookup_bucket_layout", return_value=layout):
        gen = instance._enable_rcu_rapid_write.__wrapped__(instance, fake_fs)
        next(gen)
        assert fake_fs.rcu_rapid_write is expected_rcu_rapid_write
        with pytest.raises(StopIteration):
            next(gen)

    assert fake_fs.rcu_rapid_write is False
