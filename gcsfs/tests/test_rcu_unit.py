"""Unit tests for write-open failures on Rapid Cache Ultra (RCU) buckets.

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

import logging
from unittest import mock

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


def _writer():
    """A mock writer whose open succeeded."""
    writer = mock.AsyncMock(spec=AsyncAppendableObjectWriter)
    writer._is_stream_open = True
    return writer


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


@pytest.fixture
def rcu_fs():
    """Yield ``(fs, init_aaow)`` with the gRPC client and layout lookup mocked.

    Tests seed ``fs._storage_layout_cache[BUCKET]``, set
    ``fs._get_bucket_layout.return_value`` to the layout a fresh lookup
    returns, and configure the ``gcsfs.zb_hns_utils.init_aaow`` mock.
    """
    fs = ExtendedGcsFileSystem(token="anon", skip_instance_cache=True)
    fs._grpc_client = mock.MagicMock()
    with (
        mock.patch.object(fs, "_get_grpc_client", new_callable=mock.AsyncMock),
        mock.patch.object(fs, "_get_bucket_layout", new_callable=mock.AsyncMock),
        mock.patch(
            "gcsfs.zb_hns_utils.init_aaow", new_callable=mock.AsyncMock
        ) as init_aaow,
    ):
        yield fs, init_aaow


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


@pytest.mark.parametrize("layout", layouts.ALL_BUCKET_LAYOUTS)
def test_skip_if_zonal_skips_all_grpc_data_path_layouts(layout):
    """skip_if_zonal skips HTTP-mechanics tests on both Zonal and RCU buckets."""
    from _pytest.outcomes import Skipped

    from gcsfs.tests import conftest

    fake_gcs = mock.MagicMock()
    fake_gcs._sync_lookup_bucket_layout.return_value = layout

    if layout.uses_grpc_data_path:
        with pytest.raises(Skipped):
            conftest.skip_if_zonal.__wrapped__(fake_gcs)
    else:
        conftest.skip_if_zonal.__wrapped__(fake_gcs)


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
        assert next(unparam_gen) is None
    else:
        with pytest.raises(Skipped):
            next(unparam_gen)

    # Parametrized request: runs iff request.param == live_layout.
    for target in conftest.RAPID_WRITE_LAYOUTS:
        param_gen = conftest.zonal_write_mocks.__wrapped__(
            SimpleNamespace(param=target)
        )
        if live_layout == target:
            assert next(param_gen) is None
        else:
            with pytest.raises(Skipped):
                next(param_gen)


@pytest.mark.parametrize("layout", layouts.ALL_BUCKET_LAYOUTS)
def test_rcu_fs_fixture_skips_non_rcu_buckets(monkeypatch, layout):
    """rcu_fs yields ExtendedGcsFileSystem only when TEST_ZONAL_BUCKET is an RCU bucket."""
    from _pytest.outcomes import Skipped

    from gcsfs.tests import test_rcu

    fake_fs = mock.MagicMock()
    fake_fs._sync_lookup_bucket_layout.return_value = layout
    closed = []
    monkeypatch.setattr(test_rcu, "ExtendedGcsFileSystem", lambda **kw: fake_fs)
    monkeypatch.setattr(test_rcu, "_close_gcs", lambda fs: closed.append(fs))

    gen = test_rcu.rcu_fs.__wrapped__()
    if layout.is_rcu:
        assert next(gen) is fake_fs
        with pytest.raises(StopIteration):
            next(gen)
    else:
        with pytest.raises(Skipped):
            next(gen)
    assert closed == [fake_fs]
