"""Tests for ZonalFile write operations."""

import asyncio
import contextlib
import os
from unittest import mock

import pytest
from google.cloud.storage.asyncio.async_appendable_object_writer import (
    _DEFAULT_FLUSH_INTERVAL_BYTES,
)

from gcsfs.caching import Prefetcher
from gcsfs.tests.settings import TEST_ZONAL_BUCKET
from gcsfs.tests.utils import tempdir, tmpfile
from gcsfs.zonal_file import ZonalFile

test_data = b"hello world"

REQUIRED_ENV_VAR = "GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT"

# If the condition is True, only then tests in this file are run.
should_run = os.getenv(REQUIRED_ENV_VAR, "false").lower() in (
    "true",
    "1",
)
pytestmark = pytest.mark.skipif(
    not should_run, reason=f"Skipping tests: {REQUIRED_ENV_VAR} env variable is not set"
)


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
        f.write(data1)
        f.write(data2)

    if zonal_write_mocks:
        zonal_write_mocks["aaow"].append.assert_has_awaits(
            [mock.call(data1), mock.call(data2)]
        )
    else:
        assert extended_gcsfs.cat(file_path) == data1 + data2


def test_zonal_file_open_write_mode(extended_gcsfs, zonal_write_mocks, file_path):
    """Test that opening a ZonalFile in write mode initializes the writer."""
    bucket, key, _ = extended_gcsfs.split_path(file_path)
    with extended_gcsfs.open(file_path, "wb", finalize_on_close=True):
        pass

    if zonal_write_mocks:
        zonal_write_mocks["init_aaow"].assert_called_once_with(
            extended_gcsfs.grpc_client, bucket, key, None, _DEFAULT_FLUSH_INTERVAL_BYTES
        )
    else:
        assert extended_gcsfs.exists(file_path)


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
            extended_gcsfs.grpc_client, bucket, key, None, custom_flush_interval_bytes
        )
    else:
        assert extended_gcsfs.exists(file_path)


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
        )
    else:
        assert extended_gcsfs.cat(file_path) == b"data"


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
            extended_gcsfs.grpc_client, bucket, key, None, _DEFAULT_FLUSH_INTERVAL_BYTES
        )
        # _info is called to get the generation, but it fails
        extended_gcsfs._info.assert_awaited_once()
    else:
        assert extended_gcsfs.cat(file_path) == test_data


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
    """Test that finalize_on_close is False by default."""
    with extended_gcsfs.open(file_path, "wb") as f:
        assert f.finalize_on_close is False
    if zonal_write_mocks:
        zonal_write_mocks["aaow"].close.assert_awaited_with(finalize_on_close=False)


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
            "Discard is not applicable for Zonal Buckets"
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
    os.environ.get("STORAGE_EMULATOR_HOST") != "https://storage.googleapis.com",
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


@pytest.fixture
def mock_gcsfs():
    fs = mock.Mock()
    fs._split_path.return_value = ("test-bucket", "test-key", "123")
    fs.split_path.return_value = ("test-bucket", "test-key", "123")
    fs.info.return_value = {"size": 1000, "generation": "123", "name": "test-key"}
    fs.loop = mock.Mock()
    return fs


def test_zonal_file_prefetcher_initialization(mock_gcsfs):
    """Test that setting cache_type to 'prefetcher' injects the logical chunk fetcher."""

    with (
        mock.patch("gcsfs.zonal_file.MRDPool") as mrd_pool_mock,
        mock.patch("gcsfs.zonal_file.asyn.sync"),
    ):

        mrd_pool_instance = mock.Mock()
        mrd_pool_instance.persisted_size = 1000
        mrd_pool_mock.return_value = mrd_pool_instance

        cache_options = {"concurrency": 2}

        zf = ZonalFile(
            gcsfs=mock_gcsfs,
            path="gs://test-bucket/test-key",
            mode="rb",
            cache_type="prefetcher",
            pool_size=1,
            cache_options=cache_options,
        )

        assert zf.pool_size == 1
        assert zf.cache.name == Prefetcher.name
        assert zf.cache.size == 1000

        zf.close()


@pytest.mark.asyncio
async def test_fetch_logical_chunk_split_logic(mock_gcsfs):
    """Test that chunks larger than 16MB are split correctly."""

    with (
        mock.patch("gcsfs.zonal_file.MRDPool"),
        mock.patch("gcsfs.zonal_file.asyn.sync"),
        mock.patch("gcsfs.zonal_file.DirectMemmoveBuffer"),
    ):

        zf = ZonalFile(gcsfs=mock_gcsfs, path="gs://test-bucket/test-key", mode="rb")
        zf.pool_size = 4

        zf.mrd_pool = mock.Mock()
        zf.gcsfs.memmove_executor = mock.Mock()

        mrd_mock = mock.AsyncMock()

        @contextlib.asynccontextmanager
        async def fake_get_mrd():
            yield mrd_mock

        zf.mrd_pool.get_mrd = fake_get_mrd

        total_size = 32 * 1024 * 1024
        await zf._fetch_logical_chunk(
            start_offset=0, total_size=total_size, split_factor=2
        )

        # Assert the split logic directly on the downloader mock
        assert mrd_mock.download_ranges.call_count == 2

        # Sort calls by offset to ensure consistent assertions (tasks can run in any order)
        calls = mrd_mock.download_ranges.call_args_list
        args = [c[0][0][0] for c in calls]  # extracts the (offset, size, buffer) tuple
        args.sort(key=lambda x: x[0])

        assert args[0][0] == 0  # Offset 1
        assert args[0][1] == 16 * 1024 * 1024  # Size 1

        assert args[1][0] == 16 * 1024 * 1024  # Offset 2
        assert args[1][1] == 16 * 1024 * 1024  # Size 2

        # Explicitly close while asyn.sync is still mocked
        zf.close()


@pytest.mark.asyncio
async def test_zonal_fetch_logical_chunk_cancellation(mock_gcsfs):
    """Test the BaseException block (cancellation) cleans up and cancels inner tasks."""
    with (
        mock.patch("gcsfs.zonal_file.MRDPool"),
        mock.patch("gcsfs.zonal_file.asyn.sync"),
        mock.patch("gcsfs.zonal_file.DirectMemmoveBuffer"),
    ):
        zf = ZonalFile(gcsfs=mock_gcsfs, path="gs://test-bucket/test-key", mode="rb")
        zf.mrd_pool = mock.Mock()
        zf.gcsfs.memmove_executor = mock.Mock()

        mrd_mock = mock.AsyncMock()

        # Create a side effect that hangs to simulate pending downloads
        async def slow_download(*args, **kwargs):
            await asyncio.sleep(10)

        mrd_mock.download_ranges = mock.AsyncMock(side_effect=slow_download)

        @contextlib.asynccontextmanager
        async def fake_get_mrd():
            yield mrd_mock

        zf.mrd_pool.get_mrd = fake_get_mrd

        # Spawn the fetcher as a task
        task = asyncio.create_task(
            zf._fetch_logical_chunk(start_offset=0, total_size=100, split_factor=2)
        )

        # Yield control to let the inner tasks get created and block on sleep
        await asyncio.sleep(0.1)

        # Cancel the outer task
        task.cancel()

        # Ensure the CancelledError bubbles out exactly as expected
        with pytest.raises(asyncio.CancelledError):
            await task

        zf.close()


@pytest.mark.asyncio
async def test_zonal_fetch_logical_chunk_single(mock_gcsfs):
    """Test successful single chunk download (split_factor=1)."""
    with (
        mock.patch("gcsfs.zonal_file.MRDPool"),
        mock.patch("gcsfs.zonal_file.asyn.sync"),
        mock.patch("gcsfs.zonal_file.DirectMemmoveBuffer") as mem_buf_mock,
        mock.patch(
            "gcsfs.zonal_file.PyBytes_FromStringAndSize", return_value=b"0" * 100
        ),
        mock.patch("gcsfs.zonal_file.PyBytes_AsString", return_value=12345),
    ):
        zf = ZonalFile(gcsfs=mock_gcsfs, path="gs://test-bucket/test-key", mode="rb")
        zf.mrd_pool = mock.Mock()

        mrd_mock = mock.AsyncMock()

        @contextlib.asynccontextmanager
        async def fake_get_mrd():
            yield mrd_mock

        zf.mrd_pool.get_mrd = fake_get_mrd

        res = await zf._fetch_logical_chunk(
            start_offset=0, total_size=100, split_factor=1
        )

        assert res == b"0" * 100
        mrd_mock.download_ranges.assert_awaited_once()
        # Verify the finally block ran and closed the buffer
        mem_buf_mock.return_value.close.assert_called_once()
        zf.close()


@pytest.mark.asyncio
async def test_zonal_fetch_logical_chunk_single_exception(mock_gcsfs):
    """Test exception handling and buffer cleanup in single chunk download."""
    with (
        mock.patch("gcsfs.zonal_file.MRDPool"),
        mock.patch("gcsfs.zonal_file.asyn.sync"),
        mock.patch("gcsfs.zonal_file.DirectMemmoveBuffer") as mem_buf_mock,
        mock.patch(
            "gcsfs.zonal_file.PyBytes_FromStringAndSize", return_value=b"0" * 100
        ),
        mock.patch("gcsfs.zonal_file.PyBytes_AsString", return_value=12345),
    ):
        zf = ZonalFile(gcsfs=mock_gcsfs, path="gs://test-bucket/test-key", mode="rb")
        zf.mrd_pool = mock.Mock()

        mrd_mock = mock.AsyncMock()
        mrd_mock.download_ranges.side_effect = RuntimeError("Single chunk failure")

        @contextlib.asynccontextmanager
        async def fake_get_mrd():
            yield mrd_mock

        zf.mrd_pool.get_mrd = fake_get_mrd

        with pytest.raises(RuntimeError, match="Single chunk failure"):
            await zf._fetch_logical_chunk(
                start_offset=0, total_size=100, split_factor=1
            )

        # Verify the finally block ran despite the error
        mem_buf_mock.return_value.close.assert_called_once()
        zf.close()


@pytest.mark.asyncio
async def test_zonal_fetch_logical_chunk_multi_exception(mock_gcsfs):
    """Test that standard Exceptions in concurrent downloads are caught and propagated."""
    with (
        mock.patch("gcsfs.zonal_file.MRDPool"),
        mock.patch("gcsfs.zonal_file.asyn.sync"),
        mock.patch("gcsfs.zonal_file.DirectMemmoveBuffer") as mem_buf_mock,
        mock.patch(
            "gcsfs.zonal_file.PyBytes_FromStringAndSize", return_value=b"0" * 100
        ),
        mock.patch("gcsfs.zonal_file.PyBytes_AsString", return_value=12345),
    ):
        zf = ZonalFile(gcsfs=mock_gcsfs, path="gs://test-bucket/test-key", mode="rb")
        zf.mrd_pool = mock.Mock()

        mrd_mock = mock.AsyncMock()

        call_count = 0

        async def fake_download_ranges(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 2:
                raise RuntimeError("Simulated chunk download failure")
            return None

        mrd_mock.download_ranges = mock.AsyncMock(side_effect=fake_download_ranges)

        @contextlib.asynccontextmanager
        async def fake_get_mrd():
            yield mrd_mock

        zf.mrd_pool.get_mrd = fake_get_mrd

        with pytest.raises(RuntimeError, match="Simulated chunk download failure"):
            await zf._fetch_logical_chunk(
                start_offset=0, total_size=100, split_factor=2
            )

        assert mem_buf_mock.return_value.close.call_count == 2
        zf.close()


@pytest.mark.asyncio
async def test_zonal_fetch_logical_chunk_multi_cancellation(mock_gcsfs):
    """Test the BaseException block (cancellation) cleans up and cancels inner tasks."""
    with (
        mock.patch("gcsfs.zonal_file.MRDPool"),
        mock.patch("gcsfs.zonal_file.asyn.sync"),
        mock.patch("gcsfs.zonal_file.DirectMemmoveBuffer") as mem_buf_mock,
        mock.patch(
            "gcsfs.zonal_file.PyBytes_FromStringAndSize", return_value=b"0" * 100
        ),
        mock.patch("gcsfs.zonal_file.PyBytes_AsString", return_value=12345),
        mock.patch("asyncio.gather", new_callable=mock.AsyncMock) as gather_mock,
    ):
        zf = ZonalFile(gcsfs=mock_gcsfs, path="gs://test-bucket/test-key", mode="rb")
        zf.mrd_pool = mock.Mock()

        @contextlib.asynccontextmanager
        async def fake_get_mrd():
            yield mock.AsyncMock()

        zf.mrd_pool.get_mrd = fake_get_mrd

        gather_mock.side_effect = [asyncio.CancelledError("Cancelled by test"), []]

        with pytest.raises(asyncio.CancelledError):
            await zf._fetch_logical_chunk(
                start_offset=0, total_size=100, split_factor=2
            )
        assert gather_mock.call_count == 2
        assert mem_buf_mock.return_value.close.call_count == 2

        zf.close()
