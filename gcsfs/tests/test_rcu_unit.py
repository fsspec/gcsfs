"""Unit tests for write-open failures on Rapid Cache Ultra (RCU) buckets.

When an appendable write to a bucket cached as RCU fails to open,
ExtendedGcsFileSystem looks the bucket layout up once more. If RCU is no
longer enabled, it caches the fresh layout and raises
``RapidCacheUltraDisabledError``, which ``pipe_file`` and ``put_file`` answer
with one retry over HTTP; otherwise the original error surfaces. The gRPC
writer, the layout lookup, and the HTTP upload are mocked.
"""

import logging
from unittest import mock

import pytest
from google.api_core import exceptions as api_exceptions
from google.cloud.storage.asyncio.async_appendable_object_writer import (
    AsyncAppendableObjectWriter,
)

from gcsfs.extended_gcsfs import (
    BucketLayout,
    ExtendedGcsFileSystem,
    RapidCacheUltraDisabledError,
)
from gcsfs.tests import layouts
from gcsfs.tests.utils import is_real_gcs, tmpfile

pytestmark = pytest.mark.skipif(
    is_real_gcs(), reason="Unit tests with mocks; no real GCS needed."
)

BUCKET = "rcu-bucket"
PATH = f"{BUCKET}/key"


@pytest.fixture
def open_error():
    """The error a writer open returns once RCU is disabled on the bucket."""
    return api_exceptions.InvalidArgument(
        "This bucket does not support appendable objects."
    )


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

    with pytest.raises(api_exceptions.InvalidArgument) as excinfo:
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

    with pytest.raises(api_exceptions.InvalidArgument) as excinfo:
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

    with pytest.raises(api_exceptions.InvalidArgument) as excinfo:
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
        await fs._pipe_file(PATH, b"data", mode="create")

    http_pipe_file.assert_awaited_once()
    assert http_pipe_file.await_args.args[:2] == (PATH, b"data")
    assert http_pipe_file.await_args.kwargs["mode"] == "create"
    assert any(
        r.name == "gcsfs"
        and r.levelno == logging.WARNING
        and "Retrying the write over HTTP" in r.getMessage()
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
async def test_pipe_file_rcu_still_enabled_propagates_error(rcu_fs, open_error):
    """pipe_file surfaces the open error without an HTTP retry while RCU is enabled."""
    fs, init_aaow = rcu_fs
    fs._storage_layout_cache[BUCKET] = layouts.STANDARD_RCU
    init_aaow.side_effect = open_error
    fs._get_bucket_layout.return_value = layouts.STANDARD_RCU

    with mock.patch(
        "gcsfs.core.GCSFileSystem._pipe_file", new_callable=mock.AsyncMock
    ) as http_pipe_file:
        with pytest.raises(api_exceptions.InvalidArgument) as excinfo:
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
        with pytest.raises(api_exceptions.InvalidArgument) as excinfo:
            await fs._pipe_file(PATH, b"data")

    assert excinfo.value is open_error
    writer.close.assert_awaited_once()
    fs._get_bucket_layout.assert_not_awaited()
    http_pipe_file.assert_not_awaited()


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
