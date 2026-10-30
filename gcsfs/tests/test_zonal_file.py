"""Tests for zonal write operations.

Test placement: keep zonal-specific write behavior here, including ZonalFile
tests and ExtendedGcsFileSystem zonal write routing tests. Standard filesystem
behavior belongs in test_core.py; HNS-specific behavior belongs in
test_hns_unit.py or test_hns.py.
"""

import asyncio
import logging
import os
import sys
import threading
from unittest import mock

import fsspec.asyn
import pytest
from fsspec.asyn import FSTimeoutError
from google.cloud.storage.asyncio.async_appendable_object_writer import (
    _DEFAULT_FLUSH_INTERVAL_BYTES,
)

from gcsfs import zb_hns_utils
from gcsfs.extended_gcsfs import BucketLayout, CachingTier, ExtendedGcsFileSystem
from gcsfs.retry import GenerationMismatchError, HttpError
from gcsfs.tests.conftest import RAPID_WRITE_LAYOUTS, requires_rapid
from gcsfs.tests.settings import TEST_ZONAL_BUCKET
from gcsfs.tests.utils import is_real_gcs, tempdir, tmpfile
from gcsfs.zb_hns_utils import DEFAULT_TEARDOWN_TIMEOUT_SECONDS
from gcsfs.zonal_file import ZonalFile

test_data = b"hello world"

pytestmark = [requires_rapid]


class TestZonalGcsFileSystemWriteFileCacheUpdate:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("bucket_layout", RAPID_WRITE_LAYOUTS)
    async def test_pipe_file_zonal_routes_through_write_cache_update(
        self, bucket_layout
    ):
        """The zonal pipe_file path routes through the shared cache updater."""
        fs = ExtendedGcsFileSystem(token="anon", skip_instance_cache=True)
        fs._grpc_client = mock.MagicMock()
        path = f"{TEST_ZONAL_BUCKET}/dir/file.txt"

        writer = mock.MagicMock()
        writer.append = mock.AsyncMock()

        with (
            mock.patch.object(
                fs,
                "_lookup_bucket_layout",
                new_callable=mock.AsyncMock,
                return_value=bucket_layout,
            ),
            mock.patch.object(fs, "_get_grpc_client", new_callable=mock.AsyncMock),
            mock.patch(
                "gcsfs.extended_gcsfs.zb_hns_utils.init_aaow",
                new_callable=mock.AsyncMock,
                return_value=writer,
            ) as mock_init_aaow,
            mock.patch(
                "gcsfs.extended_gcsfs.zb_hns_utils.close_aaow",
                new_callable=mock.AsyncMock,
            ) as mock_close_aaow,
            mock.patch.object(
                fs, "_write_file_cache_update", new_callable=mock.AsyncMock
            ) as mock_update,
        ):
            await fs._pipe_file(path, b"some-data")

        mock_update.assert_awaited_once_with(path)
        expected_rcu = bucket_layout.caching_tier == CachingTier.RAPID_CACHE_ULTRA
        assert mock_init_aaow.await_args.kwargs["storage_class"] == (
            "RAPID" if expected_rcu else None
        )
        mock_close_aaow.assert_awaited_once_with(writer, finalize_on_close=expected_rcu)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("bucket_layout", RAPID_WRITE_LAYOUTS)
    async def test_put_file_zonal_routes_through_write_cache_update(
        self, bucket_layout
    ):
        """The zonal put_file path routes through the shared cache updater."""
        fs = ExtendedGcsFileSystem(token="anon", skip_instance_cache=True)
        fs._grpc_client = mock.MagicMock()
        rpath = f"{TEST_ZONAL_BUCKET}/dir/file.txt"

        writer = mock.MagicMock()
        writer.append_from_file = mock.AsyncMock()

        with tmpfile() as lpath:
            with open(lpath, "wb") as f:
                f.write(b"some-data")

            with (
                mock.patch.object(
                    fs,
                    "_lookup_bucket_layout",
                    new_callable=mock.AsyncMock,
                    return_value=bucket_layout,
                ),
                mock.patch.object(fs, "_get_grpc_client", new_callable=mock.AsyncMock),
                mock.patch(
                    "gcsfs.extended_gcsfs.zb_hns_utils.init_aaow",
                    new_callable=mock.AsyncMock,
                    return_value=writer,
                ) as mock_init_aaow,
                mock.patch(
                    "gcsfs.extended_gcsfs.zb_hns_utils.close_aaow",
                    new_callable=mock.AsyncMock,
                ) as mock_close_aaow,
                mock.patch.object(
                    fs, "_write_file_cache_update", new_callable=mock.AsyncMock
                ) as mock_update,
            ):
                await fs._put_file(lpath, rpath)

        mock_update.assert_awaited_once_with(rpath)
        expected_rcu = bucket_layout.caching_tier == CachingTier.RAPID_CACHE_ULTRA
        assert mock_init_aaow.await_args.kwargs["storage_class"] == (
            "RAPID" if expected_rcu else None
        )
        mock_close_aaow.assert_awaited_once_with(writer, finalize_on_close=expected_rcu)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("finalize_on_close", [None, True])
    @pytest.mark.parametrize("bucket_layout", RAPID_WRITE_LAYOUTS)
    async def test_pipe_file_failure_closes_without_finalizing(
        self, bucket_layout, finalize_on_close
    ):
        """A failed pipe_file closes the writer without finalizing the partial object."""
        fs = ExtendedGcsFileSystem(token="anon", skip_instance_cache=True)
        fs._grpc_client = mock.MagicMock()
        path = f"{TEST_ZONAL_BUCKET}/dir/file.txt"

        writer = mock.MagicMock()
        writer.append = mock.AsyncMock(side_effect=ConnectionError("append failed"))

        with (
            mock.patch.object(
                fs,
                "_lookup_bucket_layout",
                new_callable=mock.AsyncMock,
                return_value=bucket_layout,
            ),
            mock.patch.object(fs, "_get_grpc_client", new_callable=mock.AsyncMock),
            mock.patch(
                "gcsfs.extended_gcsfs.zb_hns_utils.init_aaow",
                new_callable=mock.AsyncMock,
                return_value=writer,
            ),
            mock.patch(
                "gcsfs.extended_gcsfs.zb_hns_utils.close_aaow",
                new_callable=mock.AsyncMock,
            ) as mock_close_aaow,
        ):
            with pytest.raises(ConnectionError, match="append failed"):
                await fs._pipe_file(
                    path, b"some-data", finalize_on_close=finalize_on_close
                )

        mock_close_aaow.assert_awaited_once_with(writer, finalize_on_close=False)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("finalize_on_close", [None, True])
    @pytest.mark.parametrize("bucket_layout", RAPID_WRITE_LAYOUTS)
    async def test_put_file_failure_closes_without_finalizing(
        self, bucket_layout, finalize_on_close
    ):
        """A failed put_file closes the writer without finalizing the partial object."""
        fs = ExtendedGcsFileSystem(token="anon", skip_instance_cache=True)
        fs._grpc_client = mock.MagicMock()
        rpath = f"{TEST_ZONAL_BUCKET}/dir/file.txt"

        writer = mock.MagicMock()
        writer.append_from_file = mock.AsyncMock(
            side_effect=ConnectionError("append failed")
        )

        with tmpfile() as lpath:
            with open(lpath, "wb") as f:
                f.write(b"some-data")

            with (
                mock.patch.object(
                    fs,
                    "_lookup_bucket_layout",
                    new_callable=mock.AsyncMock,
                    return_value=bucket_layout,
                ),
                mock.patch.object(fs, "_get_grpc_client", new_callable=mock.AsyncMock),
                mock.patch(
                    "gcsfs.extended_gcsfs.zb_hns_utils.init_aaow",
                    new_callable=mock.AsyncMock,
                    return_value=writer,
                ),
                mock.patch(
                    "gcsfs.extended_gcsfs.zb_hns_utils.close_aaow",
                    new_callable=mock.AsyncMock,
                ) as mock_close_aaow,
            ):
                with pytest.raises(ConnectionError, match="append failed"):
                    await fs._put_file(
                        lpath, rpath, finalize_on_close=finalize_on_close
                    )

        mock_close_aaow.assert_awaited_once_with(writer, finalize_on_close=False)


@pytest.fixture
def mock_gcsfs():
    fs = mock.Mock()
    fs._split_path.return_value = ("test-bucket", "test-key", "123")
    fs.split_path.return_value = ("test-bucket", "test-key", "123")
    fs.info.return_value = {"size": 1000, "generation": "123", "name": "test-key"}
    fs.loop = mock.Mock()
    # Stub the MRDPoolCache so ZonalFile.__init__ doesn't try real RPCs.
    mock_pool = mock.Mock()
    mock_pool.persisted_size = 1000
    mock_pool.details = None
    fs._mrd_pool_cache = mock.Mock()
    fs._mrd_pool_cache.get = mock.AsyncMock(return_value=mock_pool)
    fs._cat_file = mock.AsyncMock(return_value=b"")
    fs._fetch_range_split = mock.AsyncMock(return_value=[b""])
    return fs


@mock.patch("gcsfs.zonal_file.asyn.sync")
def test_zonal_file_generation_kwarg_handling(mock_sync, mock_gcsfs):
    """Test that ZonalFile explicitly provided generation kwarg is not dropped."""
    from gcsfs.zonal_file import ZonalFile

    # split_path returns None for generation when it's not in the path
    mock_gcsfs.split_path.return_value = ("test-bucket", "test-key", None)
    zf = ZonalFile(
        gcsfs=mock_gcsfs, path="gs://test-bucket/test-key", mode="rb", generation="456"
    )

    expected_call = mock.call(
        mock_gcsfs.loop,
        mock_gcsfs._mrd_pool_cache.get,
        "test-bucket",
        "test-key",
        "456",
        pool_size=mock.ANY,
        cache_type=mock.ANY,
        cache_source=mock.ANY,
    )
    assert expected_call in mock_sync.call_args_list
    zf.close()


@mock.patch("gcsfs.zonal_file.asyn.sync")
def test_zonal_file_generation_path_handling(mock_sync, mock_gcsfs):
    """Test that ZonalFile parses generation from the path when provided."""
    from gcsfs.zonal_file import ZonalFile

    mock_gcsfs.split_path.return_value = ("test-bucket", "test-key", "123")
    zf = ZonalFile(gcsfs=mock_gcsfs, path="gs://test-bucket/test-key#123", mode="rb")

    expected_call = mock.call(
        mock_gcsfs.loop,
        mock_gcsfs._mrd_pool_cache.get,
        "test-bucket",
        "test-key",
        "123",
        pool_size=mock.ANY,
        cache_type=mock.ANY,
        cache_source=mock.ANY,
    )
    assert expected_call in mock_sync.call_args_list
    zf.close()


@pytest.mark.parametrize(
    "setup_action, error_match",
    [
        (lambda f: setattr(f, "mode", "rb"), "File not in write mode"),
        (lambda f: setattr(f, "closed", True), "I/O operation on closed file"),
        (
            lambda f: setattr(f, "forced", True),
            "This file has been force-flushed, can only close",
        ),
    ],
    ids=["not_writable", "closed", "force_flushed"],
)
def test_zonal_file_write_value_errors(
    extended_gcsfs, zonal_write_mocks, setup_action, error_match, file_path
):
    """Test ZonalFile.write raises ValueError for invalid states."""
    with extended_gcsfs.open(file_path, "wb") as f:
        setup_action(f)
        with pytest.raises(ValueError, match=error_match):
            f.write(test_data)


def test_zonal_file_write_success(extended_gcsfs, zonal_write_mocks, file_path):
    """Test that writing to a ZonalFile works (mock: calls append, real: writes data)."""
    data1 = b"first part "
    data2 = b"second part"
    with extended_gcsfs.open(file_path, "wb", finalize_on_close=True) as f:
        bytes_written1 = f.write(data1)
        assert bytes_written1 == len(data1)
        assert f.loc == len(data1)

        bytes_written2 = f.write(data2)
        assert bytes_written2 == len(data2)
        assert f.loc == len(data1) + len(data2)

    if zonal_write_mocks:
        zonal_write_mocks["aaow"].append.assert_has_awaits(
            [mock.call(data1), mock.call(data2)]
        )
    else:
        assert extended_gcsfs.cat(file_path) == data1 + data2


@pytest.mark.parametrize("zonal_write_mocks", RAPID_WRITE_LAYOUTS, indirect=True)
def test_zonal_file_open_write_mode(extended_gcsfs, zonal_write_mocks, file_path):
    """Test that opening a ZonalFile in write mode initializes the writer."""
    bucket, key, _ = extended_gcsfs.split_path(file_path)
    with extended_gcsfs.open(file_path, "wb", finalize_on_close=True):
        pass

    if zonal_write_mocks:
        zonal_write_mocks["init_aaow"].assert_called_once_with(
            extended_gcsfs.grpc_client,
            bucket,
            key,
            None,
            _DEFAULT_FLUSH_INTERVAL_BYTES,
            if_generation_match=None,
            storage_class=zonal_write_mocks["storage_class"],
        )
    else:
        assert extended_gcsfs.exists(file_path)


@pytest.mark.parametrize("zonal_write_mocks", RAPID_WRITE_LAYOUTS, indirect=True)
def test_zonal_file_open_write_mode_with_custom_flush_interval_bytes(
    extended_gcsfs, zonal_write_mocks, file_path
):
    """Test that opening a ZonalFile in write mode initializes the writer."""
    bucket, key, _ = extended_gcsfs.split_path(file_path)
    custom_flush_interval_bytes = 4 * 1024 * 1024
    with extended_gcsfs.open(
        file_path,
        "wb",
        finalize_on_close=True,
        flush_interval_bytes=custom_flush_interval_bytes,
    ):
        pass

    if zonal_write_mocks:
        zonal_write_mocks["init_aaow"].assert_called_once_with(
            extended_gcsfs.grpc_client,
            bucket,
            key,
            None,
            custom_flush_interval_bytes,
            if_generation_match=None,
            storage_class=zonal_write_mocks["storage_class"],
        )
    else:
        assert extended_gcsfs.exists(file_path)


@pytest.mark.parametrize("zonal_write_mocks", RAPID_WRITE_LAYOUTS, indirect=True)
def test_zonal_file_open_create_mode(extended_gcsfs, zonal_write_mocks, file_path):
    """mode="xb" opens the writer with the exclusive-create precondition."""
    bucket, key, _ = extended_gcsfs.split_path(file_path)
    with extended_gcsfs.open(file_path, "xb", finalize_on_close=True):
        pass

    if zonal_write_mocks:
        zonal_write_mocks["init_aaow"].assert_called_once_with(
            extended_gcsfs.grpc_client,
            bucket,
            key,
            None,
            _DEFAULT_FLUSH_INTERVAL_BYTES,
            if_generation_match=0,
            storage_class=zonal_write_mocks["storage_class"],
        )
    else:
        assert extended_gcsfs.exists(file_path)


@pytest.mark.parametrize("zonal_write_mocks", RAPID_WRITE_LAYOUTS, indirect=True)
def test_zonal_file_open_write_mode_with_if_generation_match(
    extended_gcsfs, zonal_write_mocks, file_path
):
    """An explicit precondition is forwarded to the writer."""
    bucket, key, _ = extended_gcsfs.split_path(file_path)
    if zonal_write_mocks:
        with extended_gcsfs.open(
            file_path, "wb", finalize_on_close=True, if_generation_match=7
        ) as f:
            f.write(test_data)
        zonal_write_mocks["init_aaow"].assert_called_once_with(
            extended_gcsfs.grpc_client,
            bucket,
            key,
            None,
            _DEFAULT_FLUSH_INTERVAL_BYTES,
            if_generation_match=7,
            storage_class=zonal_write_mocks["storage_class"],
        )
    else:
        # file_path does not exist, so generation 7 cannot match; SDK releases
        # whose writer lacks the parameter raise NotImplementedError instead.
        with pytest.raises((NotImplementedError, GenerationMismatchError)):
            with extended_gcsfs.open(
                file_path, "wb", finalize_on_close=True, if_generation_match=7
            ) as f:
                f.write(test_data)


@mock.patch("gcsfs.zonal_file.asyn.sync")
def test_zonal_file_rejects_if_generation_match_before_opening_pool(
    mock_sync, mock_gcsfs
):
    """A read-mode precondition is rejected before any MRD pool lookup."""
    mock_gcsfs.split_path.return_value = ("test-bucket", "test-key", None)
    with pytest.raises(ValueError, match="only valid for write modes"):
        ZonalFile(
            gcsfs=mock_gcsfs,
            path="gs://test-bucket/test-key",
            mode="rb",
            if_generation_match=5,
        )
    mock_sync.assert_not_called()


@pytest.mark.parametrize("zonal_write_mocks", RAPID_WRITE_LAYOUTS, indirect=True)
def test_zonal_file_open_append_mode(extended_gcsfs, zonal_write_mocks, file_path):
    """Test that opening a ZonalFile in append mode initializes the writer with generation."""
    bucket, key, _ = extended_gcsfs.split_path(file_path)

    with extended_gcsfs.open(file_path, "ab", finalize_on_close=True) as f:
        f.write(b"data")

    if zonal_write_mocks:
        # check _info is called to get the generation
        zonal_write_mocks["_gcsfs_info"].assert_awaited_once_with(file_path)
        zonal_write_mocks["init_aaow"].assert_called_once_with(
            extended_gcsfs.grpc_client,
            bucket,
            key,
            "12345",
            _DEFAULT_FLUSH_INTERVAL_BYTES,
            if_generation_match=None,
            storage_class=zonal_write_mocks["storage_class"],
        )
    else:
        assert extended_gcsfs.cat(file_path) == b"data"


@pytest.mark.parametrize("zonal_write_mocks", RAPID_WRITE_LAYOUTS, indirect=True)
def test_zonal_file_open_append_mode_nonexistent_file(
    extended_gcsfs, zonal_write_mocks, file_path
):
    """Test that opening a non-existent ZonalFile in append mode initializes the writer without generation."""
    bucket, key, _ = extended_gcsfs.split_path(file_path)

    if zonal_write_mocks:
        # Configure _info to raise FileNotFoundError to simulate non-existent file
        extended_gcsfs._info.side_effect = FileNotFoundError
    else:
        try:
            extended_gcsfs.rm(file_path)
        except FileNotFoundError:
            pass

    with extended_gcsfs.open(file_path, "ab", finalize_on_close=True) as f:
        f.write(test_data)

    if zonal_write_mocks:
        # init_aaow should be called with generation=None
        zonal_write_mocks["init_aaow"].assert_called_once_with(
            extended_gcsfs.grpc_client,
            bucket,
            key,
            None,
            _DEFAULT_FLUSH_INTERVAL_BYTES,
            if_generation_match=None,
            storage_class=zonal_write_mocks["storage_class"],
        )
        # _info is called to get the generation, but it fails
        extended_gcsfs._info.assert_awaited_once()
    else:
        assert extended_gcsfs.cat(file_path) == test_data


@pytest.mark.parametrize("mode", ["wb", "xb", "ab"])
@pytest.mark.parametrize(
    "zonal_write_mocks",
    [BucketLayout.STANDARD_RCU, BucketLayout.HIERARCHICAL_RCU],
    indirect=True,
)
def test_open_write_modes_use_zonal_file_on_rcu(
    extended_gcsfs, zonal_write_mocks, file_path, mode
):
    """Every write mode on an RCU bucket opens a ZonalFile that writes RAPID and finalizes on close by default."""
    with extended_gcsfs.open(file_path, mode) as f:
        assert isinstance(f, ZonalFile)
        assert f.finalize_on_close is True
        f.write(test_data)

    assert zonal_write_mocks["init_aaow"].await_args.kwargs["storage_class"] == "RAPID"
    zonal_write_mocks["aaow"].close.assert_awaited_once_with(finalize_on_close=True)


@pytest.mark.parametrize("zonal_write_mocks", RAPID_WRITE_LAYOUTS, indirect=True)
def test_zonal_file_write_mode_warns_on_ignored_upload_params(
    extended_gcsfs, zonal_write_mocks, file_path, caplog
):
    """Write modes warn when upload parameters the appendable writer ignores are set."""
    with caplog.at_level(logging.WARNING, logger="gcsfs.zonal_file"):
        with extended_gcsfs.open(
            file_path,
            "wb",
            finalize_on_close=True,
            metadata={"k": "v"},
            kms_key_name="projects/p/locations/l/keyRings/r/cryptoKeys/k",
        ):
            pass
    assert any(
        "These parameters will be ignored" in r.message and r.levelname == "WARNING"
        for r in caplog.records
    )

    caplog.clear()
    with caplog.at_level(logging.WARNING, logger="gcsfs.zonal_file"):
        with extended_gcsfs.open(file_path, "wb", finalize_on_close=True):
            pass
    assert not any(
        "These parameters will be ignored" in r.message for r in caplog.records
    )


def test_zonal_file_flush(extended_gcsfs, zonal_write_mocks, file_path):
    """Test that flush calls the underlying writer's flush method."""
    with extended_gcsfs.open(file_path, "wb") as f:
        f.flush()

    if zonal_write_mocks:
        zonal_write_mocks["aaow"].flush.assert_awaited()


def test_zonal_file_lazy_init_creates_empty_file_on_close(
    extended_gcsfs, zonal_write_mocks, file_path
):
    """Test that opening a file does not immediately create it on the server (lazy init),
    but empty file is created when the file is closed."""
    f = extended_gcsfs.open(file_path, "wb")
    assert f.aaow is None  # AAOW should not be initialized since no data is written

    if zonal_write_mocks:
        zonal_write_mocks["init_aaow"].assert_not_called()
    else:
        assert not extended_gcsfs.exists(file_path)
    f.close()

    if zonal_write_mocks:
        zonal_write_mocks["init_aaow"].assert_awaited_once()
    else:
        assert extended_gcsfs.exists(file_path)


def test_zonal_file_commit(extended_gcsfs, zonal_write_mocks, file_path):
    """Test that commit finalizes the write, sets finalized to True and does not finalize on close."""
    with extended_gcsfs.open(file_path, "wb", finalize_on_close=True) as f:
        f.write(test_data)
        f.commit()
    if zonal_write_mocks:
        zonal_write_mocks["aaow"].finalize.assert_awaited_once()
        assert f.finalize_on_close is False
        assert f.finalized is True
        # commit already closes the writer, so close should
        # not be called again
        zonal_write_mocks["aaow"].close.assert_not_awaited()
    else:
        assert extended_gcsfs.cat(file_path) == test_data


def test_zonal_file_finalize_on_close_true(
    extended_gcsfs, zonal_write_mocks, file_path
):
    """Test that finalize_on_close is correctly passed as True."""
    with extended_gcsfs.open(file_path, "wb", finalize_on_close=True) as f:
        assert f.finalize_on_close is True
    if zonal_write_mocks:
        zonal_write_mocks["aaow"].close.assert_awaited_with(finalize_on_close=True)


def test_zonal_file_finalize_on_close_default_false(
    extended_gcsfs, zonal_write_mocks, file_path
):
    """Test that finalize_on_close is False by default for Zonal buckets."""
    with extended_gcsfs.open(file_path, "wb") as f:
        assert f.finalize_on_close is False
    if zonal_write_mocks:
        zonal_write_mocks["aaow"].close.assert_awaited_with(finalize_on_close=False)


@pytest.mark.parametrize(
    "zonal_write_mocks",
    [BucketLayout.STANDARD_RCU, BucketLayout.HIERARCHICAL_RCU],
    indirect=True,
)
def test_zonal_file_finalize_on_close_rcu_default_and_overrides(
    extended_gcsfs, zonal_write_mocks, file_path
):
    """On RCU buckets finalize_on_close defaults to True, but explicit False overrides it."""
    if not zonal_write_mocks:
        pytest.skip("Unit test for finalize_on_close resolution with mocks.")

    aaow = zonal_write_mocks["aaow"]

    # 1. Default via open() is True
    with extended_gcsfs.open(file_path, "wb") as f:
        assert f.finalize_on_close is True
    aaow.close.assert_awaited_once_with(finalize_on_close=True)

    # 2. Direct ZonalFile instantiation also defaults to True on RCU
    aaow.close.reset_mock()
    aaow._is_stream_open = True
    with ZonalFile(extended_gcsfs, file_path, mode="wb") as f_direct:
        assert f_direct.finalize_on_close is True
    aaow.close.assert_awaited_once_with(finalize_on_close=True)

    # 3. Per-call finalize_on_close=False overrides RCU default
    aaow.close.reset_mock()
    aaow._is_stream_open = True
    with extended_gcsfs.open(file_path, "wb", finalize_on_close=False) as f_false:
        assert f_false.finalize_on_close is False
    aaow.close.assert_awaited_once_with(finalize_on_close=False)

    # 4. Filesystem-level finalize_on_close=False overrides RCU default
    orig = getattr(extended_gcsfs, "finalize_on_close", None)
    try:
        extended_gcsfs.finalize_on_close = False
        aaow.close.reset_mock()
        aaow._is_stream_open = True
        with extended_gcsfs.open(file_path, "wb") as f_fs_false:
            assert f_fs_false.finalize_on_close is False
        aaow.close.assert_awaited_once_with(finalize_on_close=False)
    finally:
        extended_gcsfs.finalize_on_close = orig


@pytest.mark.asyncio
@pytest.mark.parametrize("bucket_layout", RAPID_WRITE_LAYOUTS)
async def test_simple_upload_finalize_on_close_defaults_by_bucket_layout(bucket_layout):
    """simple_upload defaults finalize_on_close to True on RCU and False on Zonal, with override support."""
    from gcsfs.extended_gcsfs import simple_upload

    fs = ExtendedGcsFileSystem(token="anon", skip_instance_cache=True)
    fs._grpc_client = mock.MagicMock()
    writer = mock.MagicMock()
    writer.append = mock.AsyncMock()
    expected_rcu = bucket_layout.caching_tier == CachingTier.RAPID_CACHE_ULTRA

    with (
        mock.patch.object(
            fs,
            "_lookup_bucket_layout",
            new_callable=mock.AsyncMock,
            return_value=bucket_layout,
        ),
        mock.patch.object(fs, "_get_grpc_client", new_callable=mock.AsyncMock),
        mock.patch(
            "gcsfs.extended_gcsfs.zb_hns_utils.init_aaow",
            new_callable=mock.AsyncMock,
            return_value=writer,
        ),
        mock.patch(
            "gcsfs.extended_gcsfs.zb_hns_utils.close_aaow",
            new_callable=mock.AsyncMock,
        ) as mock_close_aaow,
    ):
        await simple_upload(fs, TEST_ZONAL_BUCKET, "k", b"data")
        mock_close_aaow.assert_awaited_once_with(writer, finalize_on_close=expected_rcu)

        mock_close_aaow.reset_mock()
        await simple_upload(
            fs, TEST_ZONAL_BUCKET, "k", b"data", finalize_on_close=not expected_rcu
        )
        mock_close_aaow.assert_awaited_once_with(
            writer, finalize_on_close=not expected_rcu
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("finalize_on_close", [None, True])
@pytest.mark.parametrize("bucket_layout", RAPID_WRITE_LAYOUTS)
async def test_simple_upload_failure_closes_without_finalizing(
    bucket_layout, finalize_on_close
):
    """A failed simple_upload closes the writer without finalizing the partial object."""
    from gcsfs.extended_gcsfs import simple_upload

    fs = ExtendedGcsFileSystem(token="anon", skip_instance_cache=True)
    fs._grpc_client = mock.MagicMock()
    writer = mock.MagicMock()
    writer.append = mock.AsyncMock(side_effect=ConnectionError("append failed"))

    with (
        mock.patch.object(
            fs,
            "_lookup_bucket_layout",
            new_callable=mock.AsyncMock,
            return_value=bucket_layout,
        ),
        mock.patch.object(fs, "_get_grpc_client", new_callable=mock.AsyncMock),
        mock.patch(
            "gcsfs.extended_gcsfs.zb_hns_utils.init_aaow",
            new_callable=mock.AsyncMock,
            return_value=writer,
        ),
        mock.patch(
            "gcsfs.extended_gcsfs.zb_hns_utils.close_aaow",
            new_callable=mock.AsyncMock,
        ) as mock_close_aaow,
    ):
        with pytest.raises(ConnectionError, match="append failed"):
            await simple_upload(
                fs,
                TEST_ZONAL_BUCKET,
                "k",
                b"data",
                finalize_on_close=finalize_on_close,
            )

    mock_close_aaow.assert_awaited_once_with(writer, finalize_on_close=False)


def test_zonal_file_flush_after_finalize_logs_warning(
    extended_gcsfs, zonal_write_mocks, file_path
):
    """Test that flushing after finalizing logs a warning."""
    with mock.patch("gcsfs.zonal_file.logger") as mock_logger:
        with extended_gcsfs.open(file_path, "wb") as f:
            f.commit()
        # The file is closed automatically on exiting the 'with' block, which
        # triggers a final flush. This should log a warning.
        mock_logger.warning.assert_called_once_with(
            "File is already finalized. Ignoring flush call."
        )


def test_zonal_file_double_finalize_warning(
    extended_gcsfs, zonal_write_mocks, file_path
):
    """Test that finalizing a file twice raises a ValueError."""
    with extended_gcsfs.open(file_path, "wb") as f:
        f.commit()
        with mock.patch("gcsfs.zonal_file.logger") as mock_logger:
            f.commit()
        mock_logger.warning.assert_called_once_with(
            "This file has already been finalized. Ignoring commit call."
        )


def test_zonal_file_commit_not_writable_warning(
    extended_gcsfs, zonal_write_mocks, file_path
):
    """Test that calling commit on a non-writable file logs a warning."""
    with extended_gcsfs.open(file_path, "wb") as f:
        # Simulate file not being writable
        f.mode = "rb"
        with mock.patch("gcsfs.zonal_file.logger") as mock_logger:
            f.commit()
        mock_logger.warning.assert_called_once_with(
            "File not in write mode. Ignoring commit call."
        )


def test_zonal_file_discard(extended_gcsfs, zonal_write_mocks, file_path):
    """Test that discard on a ZonalFile logs a warning."""
    with mock.patch("gcsfs.zonal_file.logger") as mock_logger:
        with extended_gcsfs.open(file_path, "wb") as f:
            f.discard()
        mock_logger.warning.assert_called_once()
        assert (
            "Discard is not applicable for Zonal and RCU buckets"
            in mock_logger.warning.call_args[0][0]
        )


def test_zonal_file_not_implemented_methods(
    extended_gcsfs, zonal_write_mocks, file_path
):
    """Test that some GCSFile methods are not implemented for ZonalFile."""
    method_name = "_upload_chunk"
    with extended_gcsfs.open(file_path, "wb") as f:
        method_to_call = getattr(f, method_name)
        with pytest.raises(NotImplementedError):
            method_to_call()


def test_zonal_file_overwrite(extended_gcsfs, zonal_write_mocks, file_path):
    """Tests simple writes to a ZonalFile and verifies the content is overwritten"""
    with extended_gcsfs.open(file_path, "wb", finalize_on_close=True) as f:
        f.write(test_data)
    with extended_gcsfs.open(
        file_path, "wb", content_type="text/plain", finalize_on_close=True
    ) as f:
        f.write(b"Sample text data.")

    if not zonal_write_mocks:
        assert extended_gcsfs.cat(file_path) == b"Sample text data."


def test_zonal_file_large_upload(extended_gcsfs, zonal_write_mocks, file_path):
    """Tests writing a large chunk of data to a ZonalFile."""
    large_data = b"a" * (5 * 1024 * 1024)  # 5MB

    with extended_gcsfs.open(file_path, "wb", finalize_on_close=True) as f:
        f.write(large_data)

    if not zonal_write_mocks:
        assert extended_gcsfs.cat(file_path) == large_data


def test_zonal_file_append_multiple(extended_gcsfs, zonal_write_mocks, file_path):
    """Tests that append mode correctly adds data to an existing ZonalFile with multiple writes."""
    data1 = b"initial data. "
    data2 = b"appended data."
    data3 = b"more appended data."

    with extended_gcsfs.open(file_path, "wb") as f:
        f.write(data1)

    with extended_gcsfs.open(file_path, "ab", finalize_on_close=True) as f:
        f.write(data2)
        f.write(data3)

    if not zonal_write_mocks:
        assert extended_gcsfs.cat(file_path) == data1 + data2 + data3


def test_zonal_file_append_to_empty(extended_gcsfs, zonal_write_mocks, file_path):
    """Tests appending to an explicitly created empty file."""
    try:
        extended_gcsfs.rm(file_path)
    except FileNotFoundError:
        pass

    if not zonal_write_mocks:
        with extended_gcsfs.open(file_path, "wb") as f:
            f.write(b"")

    with extended_gcsfs.open(file_path, "ab", finalize_on_close=True) as f:
        f.write(test_data)

    if not zonal_write_mocks:
        assert extended_gcsfs.cat(file_path) == test_data


@pytest.mark.skipif(
    not is_real_gcs(),
    reason="This test class is for real GCS only.",
)
class TestZonalFileRealGCS:
    """
    Contains tests for ZonalFile write operations that run only against a
    real GCS backend. These tests validate end-to-end write behavior.
    `finalize_on_close` is set to `True` in these tests because the current
    implementation does not return the correct size for unfinalized objects,
    which would cause assertion failures in `cat()` when it checks the object's
    size using HTTP call.
    """

    def test_put_file_to_zonal_bucket(self, extended_gcsfs, file_path):
        """Test putting a large file to a Zonal bucket."""
        remote_path = file_path
        data = os.urandom(1 * 1024 * 1024)  # 1MB random data

        with tmpfile() as local_f:
            with open(local_f, "wb") as f:
                f.write(data)
            extended_gcsfs.put(local_f, remote_path, finalize_on_close=True)

        assert extended_gcsfs.exists(remote_path)
        assert extended_gcsfs.cat(remote_path) == data
        assert extended_gcsfs.du(remote_path) == len(data)

    def test_put_overwrite_in_zonal_bucket(self, extended_gcsfs, file_path):
        """Test that put overwrites an existing file in a Zonal bucket."""
        remote_path = file_path
        initial_data = b"initial data for put overwrite"
        overwrite_data = b"overwritten data for put"

        with tmpfile() as local_f:
            with open(local_f, "wb") as f:
                f.write(initial_data)
            extended_gcsfs.put(local_f, remote_path, finalize_on_close=True)

        assert extended_gcsfs.cat(remote_path) == initial_data

        with tmpfile() as local_f_overwrite:
            with open(local_f_overwrite, "wb") as f:
                f.write(overwrite_data)
            extended_gcsfs.put(local_f_overwrite, remote_path, finalize_on_close=True)

        assert extended_gcsfs.cat(remote_path) == overwrite_data

    def test_put_directory_to_zonal_bucket(self, extended_gcsfs, file_path):
        """Test putting a directory recursively to a Zonal bucket."""
        remote_dir = file_path + "_dir"
        data1 = b"file one content"
        data2 = b"file two content"

        with tempdir() as local_dir:
            # Create a local directory structure
            os.makedirs(os.path.join(local_dir, "subdir"))
            with open(os.path.join(local_dir, "subdir", "file1.txt"), "wb") as f:
                f.write(data1)
            with open(os.path.join(local_dir, "subdir", "file2.txt"), "wb") as f:
                f.write(data2)

            # Upload the directory
            extended_gcsfs.put(
                os.path.join(local_dir, "subdir"),
                remote_dir,
                recursive=True,
                finalize_on_close=True,
            )

        # Verify the upload
        assert extended_gcsfs.isdir(remote_dir)
        remote_files = extended_gcsfs.ls(remote_dir)
        assert len(remote_files) == 2
        assert f"{remote_dir}/file1.txt" in remote_files
        assert f"{remote_dir}/file2.txt" in remote_files

        assert extended_gcsfs.cat(f"{remote_dir}/file1.txt") == data1
        assert extended_gcsfs.cat(f"{remote_dir}/file2.txt") == data2

    def test_put_list_to_zonal_bucket(self, extended_gcsfs):
        """Test batch uploading a list of files."""
        # Setup: Create two local files
        with tmpfile() as l1, tmpfile() as l2:
            data1, data2 = b"batch1", b"batch2"
            with open(l1, "wb") as f:
                f.write(data1)
            with open(l2, "wb") as f:
                f.write(data2)

            r1 = f"{TEST_ZONAL_BUCKET}/batch_1"
            r2 = f"{TEST_ZONAL_BUCKET}/batch_2"

            # Action: Pass lists to put
            extended_gcsfs.put([l1, l2], [r1, r2], finalize_on_close=True)

            # Verify
            assert extended_gcsfs.cat(r1) == data1
            assert extended_gcsfs.cat(r2) == data2

    def test_put_file_into_zonal_directory_syntax(self, extended_gcsfs, file_path):
        """Test putting a file into a remote directory (implicit filename)."""
        data = b"implicit filename test"
        remote_dir = file_path + "/"  # The trailing slash indicates a directory

        with tmpfile() as local_path:
            with open(local_path, "wb") as f:
                f.write(data)

            # Action: Put 'local_path' into 'remote_dir/'
            # Should result in 'remote_dir/basename(local_path)'
            extended_gcsfs.put(local_path, remote_dir, finalize_on_close=True)

            expected_remote_path = f"{remote_dir}{os.path.basename(local_path)}"
            assert extended_gcsfs.cat(expected_remote_path) == data

    def test_pipe_data_to_zonal_bucket(self, extended_gcsfs, file_path):
        """Test piping a small amount of data to a Zonal bucket."""
        remote_path = file_path
        data = b"some small piped data"

        extended_gcsfs.pipe(remote_path, data, finalize_on_close=True)

        assert extended_gcsfs.exists(remote_path)
        assert extended_gcsfs.cat(remote_path) == data

    def test_pipe_overwrite_in_zonal_bucket(self, extended_gcsfs, file_path):
        """Test that pipe overwrites an existing file in a Zonal bucket."""
        remote_path = file_path
        initial_data = b"initial data for pipe overwrite"
        overwrite_data = b"overwritten piped data for pipe"

        extended_gcsfs.pipe(remote_path, initial_data, finalize_on_close=True)
        assert extended_gcsfs.cat(remote_path) == initial_data

        extended_gcsfs.pipe(remote_path, overwrite_data, finalize_on_close=True)
        assert extended_gcsfs.cat(remote_path) == overwrite_data


def test_zonal_file_fetch_range_without_prefetch_engine(mock_gcsfs):
    """Tests _fetch_range routing to the gcsfs underlying methods when no prefetch engine exists."""

    # We need a custom fake sync function to actually execute the inner `_do_fetch`
    # coroutine so we can assert the routing logic inside of it.
    def fake_sync(loop, func, *args, **kwargs):
        import asyncio
        import inspect

        res = func(*args, **kwargs)
        if inspect.iscoroutine(res):
            return asyncio.run(res)
        return res

    with mock.patch("gcsfs.zonal_file.asyn.sync", side_effect=fake_sync):
        zf = ZonalFile(gcsfs=mock_gcsfs, path="gs://test-bucket/test-key", mode="rb")

        # Explicitly initialize these as AsyncMocks so they can be awaited safely
        mock_gcsfs._fetch_range_split = mock.AsyncMock(return_value=[b"split_data"])
        mock_gcsfs._cat_file = mock.AsyncMock(return_value=b"cat_data")

        result = zf._fetch_range(start=10, chunk_lengths=[5])

        assert result == [b"split_data"]
        mock_gcsfs._fetch_range_split.assert_awaited_once_with(
            zf.path,
            concurrency=4,
            start=10,
            chunk_lengths=[5],
            size=zf.size,
            mrd=zf.mrd_pool,
            cache_type=mock.ANY,
            cache_source=mock.ANY,
        )

        result = zf._fetch_range(start=10, end=20)

        assert result == b"cat_data"
        mock_gcsfs._cat_file.assert_awaited_once_with(
            zf.path,
            start=10,
            end=20,
            concurrency=zf.pool_size,
            mrd=zf.mrd_pool,
            cache_type=mock.ANY,
            cache_source=mock.ANY,
        )

        # Test catch of "not satisfiable"
        mock_gcsfs._cat_file.side_effect = RuntimeError("not satisfiable")
        result = zf._fetch_range(start=10, end=20)
        assert result == b""

        zf.close()


@mock.patch("gcsfs.zonal_file.asyn.sync")
@pytest.mark.asyncio
async def test_zonal_file_async_fetch_range(mock_sync, mock_gcsfs):
    """Tests the native coroutine called by the BackgroundPrefetcher."""
    zf = ZonalFile(gcsfs=mock_gcsfs, path="gs://test-bucket/test-key", mode="rb")
    mock_gcsfs._concurrent_mrd_fetch = mock.AsyncMock(return_value=b"async data")
    result = await zf._async_fetch_range(start_offset=0, total_size=100, split_factor=2)
    assert result == b"async data"
    mock_gcsfs._concurrent_mrd_fetch.assert_awaited_once_with(0, 100, 2, zf.mrd_pool)
    zf.close()


def test_zonal_file_prefetcher_producer_fetcher_integration(mock_gcsfs):
    mock_gcsfs.loop = fsspec.asyn.get_loop()
    mock_pool = mock.Mock(persisted_size=1000, details=None)
    mock_gcsfs._mrd_pool_cache.get = mock.AsyncMock(return_value=mock_pool)
    zf = ZonalFile(gcsfs=mock_gcsfs, path="gs://test-bucket/test-key", mode="rb")
    prefetcher = getattr(zf.cache, "_prefetcher", None)
    assert prefetcher is not None
    assert prefetcher.producer.fetcher == zf._async_fetch_range
    zf.close()


@mock.patch("gcsfs.zonal_file.asyn.sync")
def test_zonal_file_pool_size_initialization(mock_sync, mock_gcsfs):
    """Tests that pool_size is correctly set based on kwargs and env vars."""
    zf1 = ZonalFile(
        gcsfs=mock_gcsfs, path="gs://test-bucket/test-key", mode="rb", pool_size=10
    )
    assert zf1.pool_size == 10
    zf1.close()

    zf2 = ZonalFile(
        gcsfs=mock_gcsfs,
        path="gs://test-bucket/test-key",
        mode="rb",
        concurrency=4,
    )
    assert zf2.pool_size == 4
    zf2.close()


@mock.patch("gcsfs.zonal_file.asyn.sync")
def test_zonal_file_cache_type_default_resolution(mock_sync, mock_gcsfs):
    """Tests dynamic cache_type resolution for ZonalFile."""
    # 1. Default -> cache_type="adaptive"
    zf_default = ZonalFile(
        gcsfs=mock_gcsfs, path="gs://test-bucket/test-key", mode="rb"
    )
    assert zf_default.cache_type == "adaptive"
    zf_default.close()

    # 2. Explicit cache_type="readahead" -> cache_type="readahead"
    zf_readahead = ZonalFile(
        gcsfs=mock_gcsfs,
        path="gs://test-bucket/test-key",
        mode="rb",
        cache_type="readahead",
    )
    assert zf_readahead.cache_type == "readahead"
    zf_readahead.close()

    # 3. Explicit cache_type="readahead_chunked" -> cache_type="readahead_chunked"
    zf_chunked = ZonalFile(
        gcsfs=mock_gcsfs,
        path="gs://test-bucket/test-key",
        mode="rb",
        cache_type="readahead_chunked",
    )
    assert zf_chunked.cache_type == "readahead_chunked"
    zf_chunked.close()

    # 4. Explicit cache_type="none" -> cache_type="none"
    zf_explicit_none = ZonalFile(
        gcsfs=mock_gcsfs,
        path="gs://test-bucket/test-key",
        mode="rb",
        cache_type="none",
    )
    assert zf_explicit_none.cache_type == "none"
    zf_explicit_none.close()

    # 5. Explicit cache_type="bytes" -> cache_type="bytes"
    zf_bytes = ZonalFile(
        gcsfs=mock_gcsfs,
        path="gs://test-bucket/test-key",
        mode="rb",
        cache_type="bytes",
    )
    assert zf_bytes.cache_type == "bytes"
    zf_bytes.close()

    # 6. Explicit cache_type="all" -> cache_type="all"
    mock_pool = mock.Mock()
    mock_pool.persisted_size = 1000
    mock_pool.details = None
    mock_sync.return_value = mock_pool
    mock_gcsfs._cat_file = mock.AsyncMock(return_value=b"hello world")
    zf_all = ZonalFile(
        gcsfs=mock_gcsfs,
        path="gs://test-bucket/test-key",
        mode="rb",
        cache_type="all",
    )
    assert zf_all.cache_type == "all"
    zf_all.close()


@mock.patch("gcsfs.zonal_file.asyn.sync")
def test_zonal_file_fetch_range_mutually_exclusive(mock_sync, mock_gcsfs):
    """Tests that providing both end and chunk_lengths raises a ValueError."""
    zf = ZonalFile(gcsfs=mock_gcsfs, path="gs://test-bucket/test-key", mode="rb")
    with pytest.raises(
        ValueError, match="mutually exclusive and cannot be used together"
    ):
        zf._fetch_range(start=0, end=10, chunk_lengths=[10])
    zf.close()


@mock.patch("gcsfs.zonal_file.sync_teardown")
@mock.patch("gcsfs.zonal_file.asyn.sync")
def test_zonal_file_close_cleans_up_new_pools(mock_sync, mock_teardown, mock_gcsfs):
    """Tests that close() properly tears down the MRD pool."""
    zf = ZonalFile(gcsfs=mock_gcsfs, path="gs://test-bucket/test-key", mode="rb")
    mock_pool = mock.Mock()
    zf.mrd_pool = mock_pool
    zf.close()

    mock_teardown.assert_called_once_with(
        mock_gcsfs.loop,
        mock_pool.close,
        timeout=DEFAULT_TEARDOWN_TIMEOUT_SECONDS,
        description="closing mrd_pool for gs://test-bucket/test-key",
    )


def test_sync_teardown_skips_during_interpreter_finalization():
    """Mirrors the prefetcher's sys.is_finalizing() guard: never touch the
    IO loop once the interpreter itself is tearing down.
    """
    loop = fsspec.asyn.get_loop()
    calls = []

    async def coro():
        calls.append(1)

    with mock.patch.object(sys, "is_finalizing", return_value=True):
        zb_hns_utils.sync_teardown(loop, coro, description="test op")

    assert calls == []


def test_sync_teardown_raises_when_loop_not_running():
    loop = fsspec.asyn.get_loop()

    calls = []

    async def coro():
        calls.append(1)

    with mock.patch.object(loop, "is_running", return_value=False):
        with pytest.raises(RuntimeError, match="usable IO loop"):
            zb_hns_utils.sync_teardown(loop, coro, description="test op")

    assert calls == []


def test_sync_teardown_raises_when_loop_missing():
    calls = []

    async def coro():
        calls.append(1)

    with pytest.raises(RuntimeError, match="usable IO loop"):
        zb_hns_utils.sync_teardown(None, coro, description="test op")

    assert calls == []


def test_sync_teardown_runs_coroutine_on_healthy_loop():
    loop = fsspec.asyn.get_loop()

    calls = []

    async def coro():
        calls.append(1)

    zb_hns_utils.sync_teardown(loop, coro, description="test op")

    assert calls == [1]


def test_sync_teardown_passes_args_and_kwargs_through():
    loop = fsspec.asyn.get_loop()

    seen = {}

    async def coro(aaow, finalize_on_close=False):
        seen["aaow"] = aaow
        seen["finalize_on_close"] = finalize_on_close

    sentinel = object()
    zb_hns_utils.sync_teardown(
        loop, coro, sentinel, finalize_on_close=True, description="close_aaow"
    )

    assert seen == {"aaow": sentinel, "finalize_on_close": True}


def test_sync_teardown_on_loop_thread_schedules_task_instead_of_raising():
    """asyn.sync() raises NotImplementedError when called from the loop
    it's supposed to wait on -- this is the same class of bug the
    prefetcher fix addresses. Verify sync_teardown avoids it by scheduling
    a task instead of blocking.
    """
    loop = fsspec.asyn.get_loop()

    calls = []

    async def coro():
        calls.append(1)

    async def run_from_loop():
        zb_hns_utils.sync_teardown(loop, coro, description="test op")
        await asyncio.sleep(0.05)  # let the scheduled task complete

    fsspec.asyn.sync(loop, run_from_loop)

    assert calls == [1]


def test_sync_teardown_raises_on_timeout():
    """A hung teardown must not block the caller forever.
    Write finalization surfaces the timeout as an error the caller can see.
    """
    loop = fsspec.asyn.get_loop()

    async def coro():
        await asyncio.sleep(2.0)

    with pytest.raises(FSTimeoutError, match="slow op"):
        zb_hns_utils.sync_teardown(loop, coro, timeout=0.05, description="slow op")


def test_sync_teardown_reentrant_task_logs_exception(caplog):
    """Verify that when a reentrant teardown task on the IO loop thread
    raises an exception, it is logged with logger.error and not left unretrieved.
    """
    loop = fsspec.asyn.get_loop()

    async def boom():
        raise RuntimeError("reentrant teardown exploded")

    async def run_from_loop():
        zb_hns_utils.sync_teardown(loop, boom, description="failing op")
        await asyncio.sleep(0.05)

    with caplog.at_level(logging.ERROR, logger="gcsfs"):
        fsspec.asyn.sync(loop, run_from_loop)

    assert any(
        "failing op" in r.message and "reentrant teardown exploded" in r.message
        for r in caplog.records
    )


def test_sync_teardown_reraises_exception():
    """sync_teardown re-raises exceptions from the teardown coroutine."""
    loop = fsspec.asyn.get_loop()

    async def failing_op():
        raise ValueError("simulated failure")

    with pytest.raises(ValueError, match="simulated failure"):
        zb_hns_utils.sync_teardown(
            loop,
            failing_op,
            timeout=5.0,
            description="failing teardown",
        )


def test_sync_teardown_fire_and_forget_when_timeout_non_positive():
    """When timeout <= 0, scheduling is fire-and-forget and returns immediately."""
    loop = fsspec.asyn.get_loop()
    executed = threading.Event()

    async def bg_op():
        await asyncio.sleep(0.01)
        executed.set()

    zb_hns_utils.sync_teardown(
        loop,
        bg_op,
        timeout=0,
        description="fire-and-forget teardown",
    )
    assert executed.wait(timeout=5.0)


def test_sync_teardown_accepts_coroutine_object():
    """sync_teardown works with an already instantiated coroutine object."""
    loop = fsspec.asyn.get_loop()
    result = []

    async def coro_func():
        result.append("done")

    coro = coro_func()
    zb_hns_utils.sync_teardown(
        loop,
        coro,
        timeout=5.0,
        description="coro object teardown",
    )
    assert result == ["done"]


@mock.patch("gcsfs.zonal_file.asyn.sync")
def test_close_impl_mrd_pool_failure_does_not_skip_aaow(mock_sync, mock_gcsfs):
    """Verify that if mrd_pool.close fails, aaow teardown is still executed."""
    mock_gcsfs.loop = fsspec.asyn.get_loop()
    zf = ZonalFile(gcsfs=mock_gcsfs, path="gs://test-bucket/test-key", mode="rb")

    async def failing_mrd_close():
        raise RuntimeError("mrd pool failed")

    zf.mrd_pool = mock.Mock(close=failing_mrd_close)
    zf.aaow = mock.Mock(_is_stream_open=True)

    aaow_closed = []

    async def fake_close_aaow(aaow, finalize_on_close=False):
        aaow_closed.append(True)

    with mock.patch("gcsfs.zb_hns_utils.close_aaow", side_effect=fake_close_aaow):
        with pytest.raises(RuntimeError, match="mrd pool failed"):
            zf._close_impl()

    assert aaow_closed == [True]


@mock.patch("gcsfs.zonal_file.asyn.sync")
def test_close_impl_aaow_failure_raises(mock_sync, mock_gcsfs):
    """Verify that if close_aaow fails, the error is recorded and re-raised."""
    mock_gcsfs.loop = fsspec.asyn.get_loop()
    zf = ZonalFile(gcsfs=mock_gcsfs, path="gs://test-bucket/test-key", mode="rb")

    async def fake_mrd_close():
        pass

    zf.mrd_pool = mock.Mock(close=fake_mrd_close)
    zf.aaow = mock.Mock(_is_stream_open=True)

    async def failing_close_aaow(aaow, finalize_on_close=False):
        raise RuntimeError("aaow close failed")

    with mock.patch("gcsfs.zb_hns_utils.close_aaow", side_effect=failing_close_aaow):
        with pytest.raises(RuntimeError, match="aaow close failed"):
            zf._close_impl()


def test_zonal_file_fetch_range_unhandled_runtime_error(mock_gcsfs):
    """Tests that a RuntimeError not containing 'not satisfiable' is re-raised."""

    def fake_sync(loop, func, *args, **kwargs):
        import asyncio
        import inspect

        res = func(*args, **kwargs)
        if inspect.iscoroutine(res):
            return asyncio.run(res)
        return res

    with mock.patch("gcsfs.zonal_file.asyn.sync", side_effect=fake_sync):
        zf = ZonalFile(gcsfs=mock_gcsfs, path="gs://test-bucket/test-key", mode="rb")
        mock_gcsfs._cat_file = mock.AsyncMock(
            side_effect=RuntimeError("A completely different error occurred")
        )
        mock_gcsfs._fetch_range_split = mock.AsyncMock(
            side_effect=RuntimeError("A completely different error occurred")
        )

        with pytest.raises(RuntimeError, match="A completely different error occurred"):
            zf._fetch_range(start=0, end=10)

        with pytest.raises(RuntimeError, match="A completely different error occurred"):
            zf._fetch_range(start=0, chunk_lengths=[10])

        zf.close()


def test_zonal_file_fetch_range_bounds_and_invalid_range(mock_gcsfs):
    """Tests boundary checks and InvalidRange HttpError handling in ZonalFile._fetch_range."""

    def fake_sync(loop, func, *args, **kwargs):
        import asyncio
        import inspect

        res = func(*args, **kwargs)
        if inspect.iscoroutine(res):
            return asyncio.run(res)
        return res

    with mock.patch("gcsfs.zonal_file.asyn.sync", side_effect=fake_sync):
        zf = ZonalFile(gcsfs=mock_gcsfs, path="gs://test-bucket/test-key", mode="rb")
        zf.size = 100

        # Boundary checks handled locally
        assert zf._fetch_range(start=10, end=5) == b""
        assert zf._fetch_range(start=0, end=0) == b""

        # HttpError handling
        # Out-of-bounds / InvalidRange handling via server error
        mock_gcsfs._cat_file = mock.AsyncMock(
            side_effect=HttpError({"code": 416, "message": "InvalidRange"})
        )
        assert zf._fetch_range(start=0, end=10) == b""
        assert zf._fetch_range(start=100, end=200) == b""

        mock_gcsfs._fetch_range_split = mock.AsyncMock(
            side_effect=RuntimeError("Request not satisfiable.")
        )
        assert zf._fetch_range(start=100, end=None, chunk_lengths=[10]) == [b""]

        zf.close()


@pytest.mark.asyncio
@mock.patch("gcsfs.zb_hns_utils.init_mrd", new_callable=mock.AsyncMock)
async def test_zonal_file_open_shares_idle_queue(init_mrd_mock):
    """Two ZonalFiles opened on the same path share idle MRDs."""

    mock_mrds = []

    async def mock_create_mrd(*_a, **_kw):
        m = mock.AsyncMock(persisted_size=0)
        mock_mrds.append(m)
        return m

    init_mrd_mock.side_effect = mock_create_mrd

    fs = mock.Mock()
    fs.loop = asyncio.new_event_loop()
    fs._get_grpc_client = mock.AsyncMock()
    fs.grpc_client = mock.Mock()
    fs._info = mock.AsyncMock(
        return_value={"generation": "1", "timeFinalized": "2026-06-17T00:00:00Z"}
    )

    from gcsfs.zb_hns_utils import MRDPoolCache

    fs._mrd_pool_cache = MRDPoolCache(fs, max_idle_pools=8)

    # Drive two opens through the real cache; bypass the rest of ZonalFile/GCSFile init.
    pool_a = await fs._mrd_pool_cache.get("bucket", "key", "1", pool_size=1)
    pool_b = await fs._mrd_pool_cache.get("bucket", "key", "1", pool_size=1)

    assert fs._mrd_pool_cache._refcounts[("bucket", "key", "1", None)] == 2
    assert init_mrd_mock.await_count == 2

    a_mrd = pool_a._all_mrds[0]
    b_mrd = pool_b._all_mrds[0]

    await pool_a.close()
    await pool_b.close()

    # Verify reuse after close
    pool_c = await fs._mrd_pool_cache.get("bucket", "key", "1", pool_size=2)

    assert (
        init_mrd_mock.await_count == 2
    )  # 2 from before, pool_c init reuses from cache

    async with pool_c.get_mrd() as m1:
        assert m1 is a_mrd  # Reused from cache during pool_c.initialize()

        async with pool_c.get_mrd() as m2:
            assert m2 is b_mrd  # Reused from cache during get_mrd()

    assert init_mrd_mock.await_count == 2  # No new calls at all

    await pool_c.close()
    await fs._mrd_pool_cache.close()
    fs.loop.close()


@pytest.mark.asyncio
async def test_mrd_pool_cache_sets_pool_details():
    fs = mock.Mock()
    fs._info = mock.AsyncMock(
        return_value={
            "generation": "123",
            "size": 100,
            "timeFinalized": "2026-06-17T00:00:00Z",
        }
    )

    from gcsfs.zb_hns_utils import MRDPoolCache

    cache = MRDPoolCache(fs)

    with mock.patch("gcsfs.zb_hns_utils.MRDPool") as mock_pool:
        # Prevent actually calling mrd_pool.initialize() which would fail on a mock
        mock_pool.return_value.initialize = mock.AsyncMock()
        pool = await cache.get("bucket", "key", generation=None, pool_size=1)

    assert pool.details == {
        "generation": "123",
        "size": 100,
        "timeFinalized": "2026-06-17T00:00:00Z",
    }


def test_zonal_file_adaptive_cache_reuses_fs_loop_after_reset_lock(mock_gcsfs):
    """Verify ZonalFile adaptive prefetcher uses gcsfs.loop after fsspec.asyn.reset_lock().

    Hugging Face datasets IterableDataset._iter_pytorch() calls fsspec.asyn.reset_lock()
    in each DataLoader worker, clearing fsspec.asyn.loop[0] = None while a cached
    ExtendedGcsFileSystem instance still has fs.loop (and its gRPC MRD stream futures)
    bound to the original event loop.
    """
    loop1 = fsspec.asyn.get_loop()
    orig_iothread = fsspec.asyn.iothread[0]
    mock_gcsfs.loop = loop1
    payload = b"zonal-adaptive-payload-" * 64
    mock_gcsfs._mrd_pool_cache.get.return_value.persisted_size = len(payload)

    async def fake_concurrent_mrd_fetch(start_offset, total_size, split_factor, mrd):
        # Simulate gRPC Cython _StreamMultiplexer._recv_loop awaiting a Future
        # explicitly attached to loop1 (mock_gcsfs.loop). If BackgroundPrefetcher
        # runs _async_fetch_range on a different loop, awaiting fut raises:
        # RuntimeError: Task ... got Future <Future pending> attached to a different loop
        fut = loop1.create_future()
        loop1.call_soon(
            fut.set_result, payload[start_offset : start_offset + total_size]
        )
        return await fut

    mock_gcsfs._concurrent_mrd_fetch = fake_concurrent_mrd_fetch

    loop2 = None
    thread2 = None
    try:
        fsspec.asyn.reset_lock()
        assert fsspec.asyn.loop[0] is None

        zf = ZonalFile(
            gcsfs=mock_gcsfs,
            path="gs://test-bucket/test-key",
            mode="rb",
            cache_type="adaptive",
        )
        # AdaptiveReadaheadCache called fsspec.asyn.get_loop(), which created a
        # second global loop; ZonalFile must rebind the prefetcher to gcsfs.loop.
        loop2 = fsspec.asyn.loop[0]
        thread2 = fsspec.asyn.iothread[0]
        try:
            assert loop2 is not loop1
            assert zf.cache._prefetcher is not None
            assert zf.cache._prefetcher.loop is loop1
            assert zf.read(128) == payload[:128]
            assert zf.read(128) == payload[128:256]
        finally:
            zf.close()
    finally:
        fsspec.asyn.loop[0] = loop1
        fsspec.asyn.iothread[0] = orig_iothread
        if loop2 is not None and loop2 is not loop1:
            if loop2.is_running():
                loop2.call_soon_threadsafe(loop2.stop)
            if thread2 is not None:
                thread2.join(timeout=5)
            if not loop2.is_running():
                loop2.close()


def test_zonal_file_adaptive_cache_rebinds_when_global_loop_differs_from_fs_loop(
    mock_gcsfs, monkeypatch
):
    """Verify ZonalFile adaptive prefetcher rebinds to gcsfs.loop when fsspec.asyn.loop[0] differs."""
    loop1 = fsspec.asyn.get_loop()
    orig_iothread = fsspec.asyn.iothread[0]
    mock_gcsfs.loop = loop1
    payload = b"zonal-custom-loop-data-" * 64
    mock_gcsfs._mrd_pool_cache.get.return_value.persisted_size = len(payload)

    async def fake_concurrent_mrd_fetch(start_offset, total_size, split_factor, mrd):
        fut = loop1.create_future()
        loop1.call_soon(
            fut.set_result, payload[start_offset : start_offset + total_size]
        )
        return await fut

    mock_gcsfs._concurrent_mrd_fetch = fake_concurrent_mrd_fetch

    # Simulate fsspec <= 2026.9.0 where AdaptiveReadaheadCache does not inspect
    # fetcher.__self__.fs.loop and instead binds its prefetcher to get_loop().
    orig_cache_init = fsspec.caching.AdaptiveReadaheadCache.__init__
    monkeypatch.setattr(
        fsspec.caching.AdaptiveReadaheadCache,
        "__init__",
        lambda self, blocksize, fetcher, size, *args, **kwargs: orig_cache_init(
            self, blocksize, lambda s, e: fetcher(s, e), size, *args, **kwargs
        ),
    )

    loop2 = None
    thread2 = None
    try:
        fsspec.asyn.reset_lock()
        # Force creation of a second global loop before ZonalFile is opened
        loop2 = fsspec.asyn.get_loop()
        thread2 = fsspec.asyn.iothread[0]
        assert loop2 is not loop1

        zf = ZonalFile(
            gcsfs=mock_gcsfs,
            path="gs://test-bucket/test-key",
            mode="rb",
            cache_type="adaptive",
        )
        try:
            assert zf.cache._prefetcher is not None
            assert zf.cache._prefetcher.loop is loop1
            assert zf.read(256) == payload[:256]
        finally:
            zf.close()
    finally:
        fsspec.asyn.loop[0] = loop1
        fsspec.asyn.iothread[0] = orig_iothread
        if loop2 is not None:
            if loop2.is_running():
                loop2.call_soon_threadsafe(loop2.stop)
            if thread2 is not None:
                thread2.join(timeout=5)
            if not loop2.is_running():
                loop2.close()
