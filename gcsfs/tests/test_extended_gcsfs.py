import contextlib
import io
import logging
import multiprocessing
import os
import random
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from itertools import chain
from unittest import mock

import pytest
from google.api_core.exceptions import NotFound
from google.cloud.storage.asyncio.async_appendable_object_writer import (
    AsyncAppendableObjectWriter,
)
from google.cloud.storage.asyncio.async_multi_range_downloader import (
    AsyncMultiRangeDownloader,
)
from google.cloud.storage.exceptions import DataCorruption

from gcsfs import caching
from gcsfs.checkers import ConsistencyChecker, MD5Checker, SizeChecker
from gcsfs.extended_gcsfs import (
    BucketType,
    ExtendedGcsFileSystem,
    initiate_upload,
    simple_upload,
    upload_chunk,
)
from gcsfs.tests.conftest import (
    _MULTI_THREADED_TEST_DATA_SIZE,
    csv_files,
    files,
    text_files,
)
from gcsfs.tests.settings import TEST_BUCKET, TEST_ZONAL_BUCKET
from gcsfs.tests.utils import tempdir, tmpfile
from gcsfs.zb_hns_utils import MRD_MAX_RANGES

file = "test/accounts.1.json"
file_path = f"{TEST_ZONAL_BUCKET}/{file}"
json_data = files[file]
lines = io.BytesIO(json_data).readlines()
file_size = len(json_data)

file2 = "test/accounts.2.json"
file2_path = f"{TEST_ZONAL_BUCKET}/{file2}"
json_data2 = files[file2]

REQUIRED_ENV_VAR = "GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT"

a = TEST_ZONAL_BUCKET + "/zonal/test/a"
b = TEST_ZONAL_BUCKET + "/zonal/test/b"
c = TEST_ZONAL_BUCKET + "/zonal/test/c"

# If the condition is True, only then tests in this file are run.
should_run = os.getenv(REQUIRED_ENV_VAR, "false").lower() in (
    "true",
    "1",
)
pytestmark = pytest.mark.skipif(
    not should_run, reason=f"Skipping tests: {REQUIRED_ENV_VAR} env variable is not set"
)


@pytest.fixture
def gcs_bucket_mocks():
    """A factory fixture for mocking bucket functionality for different bucket types."""

    @contextlib.contextmanager
    def _gcs_bucket_mocks_factory(file_data, bucket_type_val):
        """Creates mocks for a given file content and bucket type."""
        is_real_gcs = (
            os.environ.get("STORAGE_EMULATOR_HOST") == "https://storage.googleapis.com"
        )
        if is_real_gcs:
            yield None
            return
        patch_target_lookup_bucket_type = (
            "gcsfs.extended_gcsfs.ExtendedGcsFileSystem._lookup_bucket_type"
        )
        patch_target_sync_lookup_bucket_type = (
            "gcsfs.extended_gcsfs.ExtendedGcsFileSystem._sync_lookup_bucket_type"
        )
        patch_target_create_mrd = (
            "google.cloud.storage._experimental.asyncio.async_multi_range_downloader"
            ".AsyncMultiRangeDownloader.create_mrd"
        )
        patch_target_gcsfs_cat_file = "gcsfs.core.GCSFileSystem._cat_file"

        async def download_side_effect(read_requests, **kwargs):
            for param_offset, param_length, buffer_arg in read_requests:
                if hasattr(buffer_arg, "write"):
                    buffer_arg.write(
                        file_data[param_offset : param_offset + param_length]
                    )

        mock_downloader = mock.Mock(spec=AsyncMultiRangeDownloader)
        mock_downloader.download_ranges = mock.AsyncMock(
            side_effect=download_side_effect
        )
        mock_downloader.persisted_size = None

        mock_create_mrd = mock.AsyncMock(return_value=mock_downloader)
        with (
            mock.patch(
                patch_target_sync_lookup_bucket_type, return_value=bucket_type_val
            ) as mock_sync_lookup_bucket_type,
            mock.patch(
                patch_target_lookup_bucket_type,
                return_value=bucket_type_val,
            ),
            mock.patch(patch_target_create_mrd, mock_create_mrd),
            mock.patch(
                patch_target_gcsfs_cat_file, new_callable=mock.AsyncMock
            ) as mock_cat_file,
        ):
            mocks = {
                "sync_lookup_bucket_type": mock_sync_lookup_bucket_type,
                "create_mrd": mock_create_mrd,
                "downloader": mock_downloader,
                "cat_file": mock_cat_file,
            }
            yield mocks
            # Common assertion for all tests using this mock
            mock_cat_file.assert_not_called()

    return _gcs_bucket_mocks_factory


read_block_params = [
    # Read specific chunk
    pytest.param(3, 10, None, json_data[3 : 3 + 10], id="offset=3, length=10"),
    # Read from beginning up to length
    pytest.param(0, 5, None, json_data[0:5], id="offset=0, length=5"),
    # Read from offset to end (simulate large length)
    pytest.param(15, 5000, None, json_data[15:], id="offset=15, length=large"),
    # Read beyond end of file (should return empty bytes)
    pytest.param(file_size + 10, 5, None, b"", id="offset>size, length=5"),
    # Read exactly at the end (zero length)
    pytest.param(file_size, 10, None, b"", id="offset=size, length=10"),
    # Read with delimiter
    pytest.param(1, 35, b"\n", lines[1], id="offset=1, length=35, delimiter=newline"),
    pytest.param(0, 30, b"\n", lines[0], id="offset=0, length=35, delimiter=newline"),
    pytest.param(
        0, 35, b"\n", lines[0] + lines[1], id="offset=0, length=35, delimiter=newline"
    ),
]


def test_read_block_zb(extended_gcsfs, gcs_bucket_mocks, subtests):
    file_size = len(
        json_data
    )  # We need the file size to predict if readahead will trigger

    for param in read_block_params:
        with subtests.test(id=param.id):
            offset, length, delimiter, expected_data = param.values
            path = file_path

            with gcs_bucket_mocks(
                json_data, bucket_type_val=BucketType.ZONAL_HIERARCHICAL
            ) as mocks:
                result = extended_gcsfs.read_block(path, offset, length, delimiter)

                assert result == expected_data

                if mocks:
                    mocks["sync_lookup_bucket_type"].assert_called_once_with(
                        TEST_ZONAL_BUCKET
                    )

                    if expected_data:
                        call_args = mocks["downloader"].download_ranges.call_args
                        assert call_args is not None, "download_ranges was not called"

                        # Get the actual list of ranges passed: [(start, end, buffer), ...]
                        actual_ranges = call_args[0][0]

                        if delimiter:
                            assert len(actual_ranges) >= 1
                            assert actual_ranges[0][0] == offset
                        else:
                            req_end = offset + length
                            if req_end >= file_size:
                                expected_chunks = 1
                            else:
                                expected_chunks = 2

                            assert (
                                len(actual_ranges) == expected_chunks
                            ), f"Expected {expected_chunks} chunks (Request + Readahead), got {len(actual_ranges)}"
                            assert actual_ranges[0][0] == offset
                            if len(actual_ranges) == 2:
                                assert actual_ranges[1][0] == offset + length
                    else:
                        mocks["downloader"].download_ranges.assert_not_called()


@pytest.mark.parametrize("bucket_type_val", list(BucketType))
def test_open_uses_correct_blocksize_and_consistency_for_all_bucket_types(
    extended_gcs_factory, gcs_bucket_mocks, bucket_type_val
):
    csv_file = "2014-01-01.csv"
    csv_file_path = f"{TEST_ZONAL_BUCKET}/{csv_file}"
    csv_data = csv_files[csv_file]

    custom_filesystem_block_size = 100 * 1024 * 1024
    extended_gcsfs = extended_gcs_factory(
        block_size=custom_filesystem_block_size, consistency="md5"
    )

    with gcs_bucket_mocks(csv_data, bucket_type_val=bucket_type_val):
        with extended_gcsfs.open(csv_file_path, "rb") as f:
            assert f.blocksize == custom_filesystem_block_size
            assert isinstance(f.checker, MD5Checker)

        file_block_size = 1024 * 1024
        with extended_gcsfs.open(
            csv_file_path, "rb", block_size=file_block_size, consistency="size"
        ) as f:
            assert f.blocksize == file_block_size
            assert isinstance(f.checker, SizeChecker)


@pytest.mark.parametrize("bucket_type_val", list(BucketType))
def test_open_uses_default_blocksize_and_consistency_from_fs(
    extended_gcsfs, gcs_bucket_mocks, bucket_type_val
):
    csv_file = "2014-01-01.csv"
    csv_file_path = f"{TEST_ZONAL_BUCKET}/{csv_file}"
    csv_data = csv_files[csv_file]

    with gcs_bucket_mocks(csv_data, bucket_type_val=bucket_type_val):
        with extended_gcsfs.open(csv_file_path, "rb") as f:
            assert f.blocksize == extended_gcsfs.default_block_size
            assert type(f.checker) is ConsistencyChecker


def test_read_small_zb(extended_gcsfs, gcs_bucket_mocks):
    csv_file = "2014-01-01.csv"
    csv_file_path = f"{TEST_ZONAL_BUCKET}/{csv_file}"
    csv_data = csv_files[csv_file]

    with gcs_bucket_mocks(
        csv_data, bucket_type_val=BucketType.ZONAL_HIERARCHICAL
    ) as mocks:
        with extended_gcsfs.open(csv_file_path, "rb", block_size=10) as f:
            out = []
            i = 1
            while True:
                i += 1
                data = f.read(3)
                if data == b"":
                    break
                out.append(data)
            assert extended_gcsfs.cat(csv_file_path) == b"".join(out)
            # cache drop
            assert len(f.cache.cache) < len(out)
            if mocks:
                mocks["sync_lookup_bucket_type"].assert_called_once_with(
                    TEST_ZONAL_BUCKET
                )


def test_readline_zb(extended_gcsfs, gcs_bucket_mocks):
    all_items = chain.from_iterable(
        [files.items(), csv_files.items(), text_files.items()]
    )
    for k, data in all_items:
        with gcs_bucket_mocks(data, bucket_type_val=BucketType.ZONAL_HIERARCHICAL):
            with extended_gcsfs.open("/".join([TEST_ZONAL_BUCKET, k]), "rb") as f:
                result = f.readline()
                expected = data.split(b"\n")[0] + (b"\n" if data.count(b"\n") else b"")
            assert result == expected


def test_readline_from_cache_zb(extended_gcsfs, gcs_bucket_mocks):
    data = text_files["zonal/test/a"]
    with gcs_bucket_mocks(data, bucket_type_val=BucketType.ZONAL_HIERARCHICAL):
        with extended_gcsfs.open(a, "rb") as f:
            result = f.readline()
            assert result == b"a,b\n"
            assert f.loc == 4
            assert f.cache.cache == data

            result = f.readline()
            assert result == b"11,22\n"
            assert f.loc == 10
            assert f.cache.cache == data

            result = f.readline()
            assert result == b"3,4"
            assert f.loc == 13
            assert f.cache.cache == data


def test_readline_empty_zb(extended_gcsfs, gcs_bucket_mocks):
    data = text_files["zonal/test/b"]
    with gcs_bucket_mocks(data, bucket_type_val=BucketType.ZONAL_HIERARCHICAL):
        with extended_gcsfs.open(b, "rb") as f:
            result = f.readline()
            assert result == data


def test_readline_blocksize_zb(extended_gcsfs, gcs_bucket_mocks):
    data = text_files["zonal/test/c"]
    with gcs_bucket_mocks(data, bucket_type_val=BucketType.ZONAL_HIERARCHICAL):
        with extended_gcsfs.open(c, "rb", block_size=2**18) as f:
            result = f.readline()
            expected = b"ab\n"
            assert result == expected

            result = f.readline()
            expected = b"a" * (2**18) + b"\n"
            assert result == expected

            result = f.readline()
            expected = b"ab"
            assert result == expected


@pytest.mark.parametrize(
    "start, end, exp_offset, exp_length",
    [
        # --- Standard Slicing ---
        (None, None, 0, file_size),  # Full file: data[:]
        (10, 20, 10, 10),  # Middle slice: data[10:20]
        (None, 10, 0, 10),  # Prefix: data[:10]
        (10, None, 10, file_size - 10),  # Suffix: data[10:]
        # --- Negative Indices ---
        (-10, None, file_size - 10, 10),  # Last N bytes: data[-10:]
        (None, -10, 0, file_size - 10),  # All except last N: data[:-10]
        (10, -10, 10, file_size - 20),  # Positive start, Negative end: data[10:-10]
        # --- Zero Length & Empty Reads ---
        (20, 20, 20, 0),  # Zero-length (Start == End): data[20:20]
        (50, 40, 50, 0),  # Crossover (Start > End): data[50:40] -> Empty
        (
            file_size + 10,
            None,
            file_size + 10,
            0,
        ),  # Start past EOF: data[110:] -> Empty
        # --- Overshoot & Clamping ---
        (
            -file_size * 2,
            None,
            0,
            file_size,
        ),  # Start Overshoot: data[-200:] -> Whole file
        (
            file_size - 10,
            file_size + 100,
            file_size - 10,
            10,
        ),  # End Overshoot: data[90:200] -> Last 10 bytes
    ],
)
def test_process_limits_parametrized(
    extended_gcsfs, start, end, exp_offset, exp_length
):
    """
    Verifies that start/end limits are correctly converted to offset/length
    """
    offset, length = extended_gcsfs.sync_process_limits_to_offset_and_length(
        file_path, start, end
    )

    assert offset == exp_offset
    assert length == exp_length


@pytest.mark.parametrize(
    "exception_to_raise",
    [ValueError, DataCorruption, Exception],
)
def test_mrd_exception_handling(extended_gcsfs, gcs_bucket_mocks, exception_to_raise):
    """
    Tests that _cat_file correctly propagates exceptions from mrd.download_ranges.
    """
    with gcs_bucket_mocks(
        json_data, bucket_type_val=BucketType.ZONAL_HIERARCHICAL
    ) as mocks:
        if extended_gcsfs.on_google:
            pytest.skip("Cannot mock exceptions on real GCS")

        # Configure the mock to raise a specified exception
        if exception_to_raise is DataCorruption:
            # The first argument is 'response', the message is in '*args'
            mocks["downloader"].download_ranges.side_effect = exception_to_raise(
                None, "Test exception raised"
            )
        else:
            mocks["downloader"].download_ranges.side_effect = exception_to_raise(
                "Test exception raised"
            )

        with pytest.raises(exception_to_raise, match="Test exception raised"):
            extended_gcsfs.read_block(file_path, 0, 10)

        mocks["downloader"].download_ranges.assert_called_once()


def test_mrd_stream_cleanup(extended_gcsfs, gcs_bucket_mocks):
    """
    Tests that mrd stream is properly closed with file closure.
    """
    with gcs_bucket_mocks(
        json_data, bucket_type_val=BucketType.ZONAL_HIERARCHICAL
    ) as mocks:
        if not extended_gcsfs.on_google:

            def close_side_effect():
                mocks["downloader"].is_stream_open = False

            mocks["downloader"].close.side_effect = close_side_effect

        with extended_gcsfs.open(file_path, "rb") as f:
            assert f.mrd is not None

        assert True is f.closed
        assert False is f.mrd.is_stream_open


def test_mrd_created_once_for_zonal_file(extended_gcsfs, gcs_bucket_mocks):
    """
    Tests that the AsyncMultiRangeDownloader (MRD) is created only once when a
    ZonalFile is opened, and not for each subsequent read operation.
    """
    if extended_gcsfs.on_google:
        pytest.skip("Internal call counts cannot be verified against real GCS.")

    with gcs_bucket_mocks(
        json_data, bucket_type_val=BucketType.ZONAL_HIERARCHICAL
    ) as mocks:
        with extended_gcsfs.open(file_path, "rb") as f:
            # The MRD should be created upon opening the file.
            mocks["create_mrd"].assert_called_once()

            f.read(10)
            f.read(20)
            f.seek(5)
            f.read(5)

        # Verify that create_mrd was not called again.
        mocks["create_mrd"].assert_called_once()


def test_read_unfinalized_file_using_mrd(extended_gcsfs, file_path):
    "Tests that mrd can read from an unfinalized file successfully"
    if not extended_gcsfs.on_google:
        pytest.skip("Cannot simulate unfinalized files on mock GCS.")

    # Files are not finalized by default
    with extended_gcsfs.open(file_path, "wb") as f:
        f.write(b"Hello, ")
        f.write(b"world!")

    with extended_gcsfs.open(file_path, "rb") as f:
        assert f.read() == b"Hello, world!"
        f.seek(4)
        assert f.read() == b"o, world!"  # Check cache works as well


def test_zonal_file_warning_on_missing_persisted_size(
    extended_gcsfs, gcs_bucket_mocks, caplog
):
    """
    Tests that a warning is logged when MRD has no 'persisted_size' attribute when opening ZonalFile.
    """
    if extended_gcsfs.on_google:
        pytest.skip("Cannot simulate missing attributes on real GCS.")

    with gcs_bucket_mocks(json_data, bucket_type_val=BucketType.ZONAL_HIERARCHICAL):
        # 'persisted_size' is set to None in the mock downloader
        with caplog.at_level(logging.WARNING, logger="gcsfs"):
            with extended_gcsfs.open(file_path, "rb"):
                pass
            assert "has no 'persisted_size'" in caplog.text


def test_process_limits_when_file_size_passed(extended_gcsfs):
    """
    Tests that process_limits works correctly when file_size is provided,
    without calling _info().
    """
    with mock.patch.object(
        extended_gcsfs, "_info", new_callable=mock.AsyncMock
    ) as mock_info:
        test_file_size = 1000

        # Case: start and end provided
        offset, length = extended_gcsfs.sync_process_limits_to_offset_and_length(
            file_path, start=100, end=200, file_size=test_file_size
        )
        assert offset == 100
        assert length == 100

        # Case: only end provided
        offset, length = extended_gcsfs.sync_process_limits_to_offset_and_length(
            file_path, start=None, end=50, file_size=test_file_size
        )
        assert offset == 0
        assert length == 50

        # Case: only start provided
        offset, length = extended_gcsfs.sync_process_limits_to_offset_and_length(
            file_path, start=950, end=None, file_size=test_file_size
        )
        assert offset == 950
        assert length == 50

        mock_info.assert_not_called()


@pytest.mark.asyncio
async def test_cat_file_warning_on_missing_persisted_size(
    extended_gcsfs, gcs_bucket_mocks, caplog
):
    """
    Tests that a warning is logged in cat_file when MRD has no 'persisted_size' attribute.
    """
    if extended_gcsfs.on_google:
        pytest.skip("Cannot simulate missing attributes on real GCS.")

    with gcs_bucket_mocks(json_data, bucket_type_val=BucketType.ZONAL_HIERARCHICAL):
        # 'persisted_size' is set to None in the mock downloader
        with (
            caplog.at_level(logging.WARNING, logger="gcsfs"),
            mock.patch.object(
                extended_gcsfs, "_info", new_callable=mock.AsyncMock
            ) as mock_info,
        ):
            mock_info.return_value = {"size": len(json_data)}
            result = await extended_gcsfs._cat_file(file_path, start=0, end=10)
            assert "Falling back to _info() to get the file size" in caplog.text
            assert result == json_data[:10]


@pytest.mark.asyncio
async def test_cat_file_on_unfinalized_file(extended_gcsfs, file_path):
    """
    Tests that cat_file can read from an unfinalized file successfully
    """
    if not extended_gcsfs.on_google:
        pytest.skip("Cannot simulate unfinalized files on mock GCS.")

    # Files are not finalized by default
    await extended_gcsfs._pipe_file(file_path, b"Hello, world!")

    data = await extended_gcsfs._cat_file(file_path)
    assert data == b"Hello, world!"


# ========================== Zonal Multithreaded Read Tests ===========================
_MULTI_THREADED_TEST_FILE = "multi_threaded_test_file"
_MULTI_THREADED_TEST_DATA = text_files[_MULTI_THREADED_TEST_FILE]
_MULTI_THREADED_TEST_FILE_PATH = f"{TEST_ZONAL_BUCKET}/{_MULTI_THREADED_TEST_FILE}"

_TEST_BLOCK_SIZE_FOR_CHUNK_BOUNDARY = 1 * 1024 * 1024  # 1MB
_NUM_CONCURRENCY_THREADS = 10
_READ_LENGTH_CONCURRENCY = 1024  # 1KB
_NUM_FAIL_SURVIVE_THREADS = 5


def run_in_threads(func, args_list, num_threads):
    """Runs a function in multiple threads and collects results or exceptions."""
    results = []
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(func, *args) for args in args_list]
        for future in futures:
            results.append(
                future.result()
            )  # This will re-raise exceptions from threads
    return results


def _read_range_from_fs(fs, path, offset, length, block_size=None):
    """Helper function for Case when each thread opens its own file handle."""
    with fs.open(path, "rb", block_size=block_size) as f:
        f.seek(offset)
        return f.read(length)


def test_multithreaded_read_disjoint_ranges_zb(extended_gcsfs, gcs_bucket_mocks):
    """
    Tests concurrent reads of disjoint ranges from the same file.
    Verifies that different parts of the file can be fetched simultaneously without data mix-up.
    """
    with gcs_bucket_mocks(
        _MULTI_THREADED_TEST_DATA, bucket_type_val=BucketType.ZONAL_HIERARCHICAL
    ) as mocks:
        read_tasks = [
            (extended_gcsfs, _MULTI_THREADED_TEST_FILE_PATH, 0, 1024),
            (extended_gcsfs, _MULTI_THREADED_TEST_FILE_PATH, 2048, 1024),
            (extended_gcsfs, _MULTI_THREADED_TEST_FILE_PATH, 4096, 1024),
        ]

        results = run_in_threads(
            _read_range_from_fs, read_tasks, num_threads=len(read_tasks)
        )

        assert results[0] == _MULTI_THREADED_TEST_DATA[0:1024]
        assert results[1] == _MULTI_THREADED_TEST_DATA[2048:3072]
        assert results[2] == _MULTI_THREADED_TEST_DATA[4096:5120]

        if mocks:
            assert mocks["create_mrd"].call_count == len(read_tasks)
            assert mocks["downloader"].download_ranges.call_count == len(read_tasks)
            assert mocks["downloader"].close.call_count == len(read_tasks)


def test_multithreaded_read_overlapping_ranges_zb(extended_gcsfs, gcs_bucket_mocks):
    """
    Tests concurrent reads of overlapping ranges from the same file.
    """
    with gcs_bucket_mocks(
        _MULTI_THREADED_TEST_DATA, bucket_type_val=BucketType.ZONAL_HIERARCHICAL
    ) as mocks:
        read_tasks = [
            (extended_gcsfs, _MULTI_THREADED_TEST_FILE_PATH, 0, 2048),
            (
                extended_gcsfs,
                _MULTI_THREADED_TEST_FILE_PATH,
                1024,
                2048,
            ),  # Overlaps with first
            (
                extended_gcsfs,
                _MULTI_THREADED_TEST_FILE_PATH,
                0,
                2048,
            ),  # Identical to first
        ]

        results = run_in_threads(
            _read_range_from_fs, read_tasks, num_threads=len(read_tasks)
        )

        assert results[0] == _MULTI_THREADED_TEST_DATA[0:2048]
        assert results[1] == _MULTI_THREADED_TEST_DATA[1024:3072]
        assert results[2] == _MULTI_THREADED_TEST_DATA[0:2048]

        if mocks:
            assert mocks["create_mrd"].call_count == len(read_tasks)
            assert mocks["downloader"].download_ranges.call_count == len(read_tasks)
            assert mocks["downloader"].close.call_count == len(read_tasks)


def test_default_cache_is_readahead_chunked(extended_gcsfs, gcs_bucket_mocks):
    data = text_files["zonal/test/b"]
    with gcs_bucket_mocks(data, bucket_type_val=BucketType.ZONAL_HIERARCHICAL):
        with extended_gcsfs.open(b, "rb") as f:
            assert isinstance(f.cache, caching.ReadAheadChunked)


def test_multithreaded_read_chunk_boundary_zb(extended_gcsfs, gcs_bucket_mocks):
    """
    Tests concurrent reads that straddle internal buffering chunk boundaries.
    Verifies correct stitching of data from multiple internal requests.
    """
    with gcs_bucket_mocks(
        _MULTI_THREADED_TEST_DATA, bucket_type_val=BucketType.ZONAL_HIERARCHICAL
    ) as mocks:
        # Read ranges that straddle _TEST_BLOCK_SIZE boundaries
        read_tasks = [
            (
                extended_gcsfs,
                _MULTI_THREADED_TEST_FILE_PATH,
                _TEST_BLOCK_SIZE_FOR_CHUNK_BOUNDARY - 512,
                1024,
                _TEST_BLOCK_SIZE_FOR_CHUNK_BOUNDARY,
            ),  # Crosses 1MB boundary
            (
                extended_gcsfs,
                _MULTI_THREADED_TEST_FILE_PATH,
                _TEST_BLOCK_SIZE_FOR_CHUNK_BOUNDARY + 512,
                1024,
                _TEST_BLOCK_SIZE_FOR_CHUNK_BOUNDARY,
            ),  # Starts after 1MB boundary
            (
                extended_gcsfs,
                _MULTI_THREADED_TEST_FILE_PATH,
                _TEST_BLOCK_SIZE_FOR_CHUNK_BOUNDARY * 2 - 256,
                512,
                _TEST_BLOCK_SIZE_FOR_CHUNK_BOUNDARY,
            ),  # Crosses 2MB boundary
        ]

        results = run_in_threads(
            _read_range_from_fs, read_tasks, num_threads=len(read_tasks)
        )

        assert (
            results[0]
            == _MULTI_THREADED_TEST_DATA[
                _TEST_BLOCK_SIZE_FOR_CHUNK_BOUNDARY
                - 512 : _TEST_BLOCK_SIZE_FOR_CHUNK_BOUNDARY
                - 512
                + 1024
            ]
        )
        assert (
            results[1]
            == _MULTI_THREADED_TEST_DATA[
                _TEST_BLOCK_SIZE_FOR_CHUNK_BOUNDARY
                + 512 : _TEST_BLOCK_SIZE_FOR_CHUNK_BOUNDARY
                + 512
                + 1024
            ]
        )
        assert (
            results[2]
            == _MULTI_THREADED_TEST_DATA[
                _TEST_BLOCK_SIZE_FOR_CHUNK_BOUNDARY * 2
                - 256 : _TEST_BLOCK_SIZE_FOR_CHUNK_BOUNDARY * 2
                - 256
                + 512
            ]
        )

        if mocks:
            assert mocks["create_mrd"].call_count == len(read_tasks)
            assert mocks["downloader"].download_ranges.call_count == len(read_tasks)
            assert mocks["downloader"].close.call_count == len(read_tasks)


def _read_random_range(fs, path, file_size, read_length):
    """Helper function to read a random range from a file."""
    offset = random.randint(0, file_size - read_length)
    with fs.open(path, "rb") as f:
        f.seek(offset)
        return f.read(read_length)


def test_multithreaded_read_high_concurrency_zb(extended_gcsfs, gcs_bucket_mocks):
    """
    Tests high-concurrency reads to stress the connection pooling and handling.
    Verifies that many concurrent requests do not lead to crashes or deadlocks.
    """
    with gcs_bucket_mocks(
        _MULTI_THREADED_TEST_DATA, bucket_type_val=BucketType.ZONAL_HIERARCHICAL
    ) as mocks:
        read_tasks = [
            (
                extended_gcsfs,
                _MULTI_THREADED_TEST_FILE_PATH,
                _MULTI_THREADED_TEST_DATA_SIZE,
                _READ_LENGTH_CONCURRENCY,
            )
            for _ in range(_NUM_CONCURRENCY_THREADS)
        ]

        results = run_in_threads(
            _read_random_range, read_tasks, num_threads=_NUM_CONCURRENCY_THREADS
        )

        assert len(results) == _NUM_CONCURRENCY_THREADS
        for res in results:
            assert len(res) == _READ_LENGTH_CONCURRENCY
            assert res in _MULTI_THREADED_TEST_DATA  # Ensure the content is valid

        if mocks:
            assert mocks["create_mrd"].call_count == _NUM_CONCURRENCY_THREADS
            assert (
                mocks["downloader"].download_ranges.call_count
                == _NUM_CONCURRENCY_THREADS
            )
            assert mocks["downloader"].close.call_count == _NUM_CONCURRENCY_THREADS


def test_multithreaded_read_one_fails_others_survive_zb(
    extended_gcsfs, gcs_bucket_mocks
):
    """
    Tests fault tolerance: one thread's read operation fails, but others complete successfully.
    """
    if extended_gcsfs.on_google:
        pytest.skip("Cannot mock failures on real GCS")

    with gcs_bucket_mocks(
        _MULTI_THREADED_TEST_DATA, bucket_type_val=BucketType.ZONAL_HIERARCHICAL
    ) as mocks:
        original_download_ranges_side_effect = mocks[
            "downloader"
        ].download_ranges.side_effect

        call_counter = 0
        counter_lock = threading.Lock()

        async def failing_download_ranges_side_effect(read_requests):
            nonlocal call_counter
            with counter_lock:
                current_call_idx = call_counter
                call_counter += 1
            if current_call_idx == 2:  # Make the 3rd call (index 2) fail
                raise DataCorruption(None, "Simulated data corruption for thread 3")

            await original_download_ranges_side_effect(read_requests)

        mocks["downloader"].download_ranges.side_effect = (
            failing_download_ranges_side_effect
        )

        read_tasks_args = [
            (extended_gcsfs, _MULTI_THREADED_TEST_FILE_PATH, i * 1024, 1024)
            for i in range(_NUM_FAIL_SURVIVE_THREADS)
        ]

        thread_results = [None] * _NUM_FAIL_SURVIVE_THREADS
        thread_exceptions = [None] * _NUM_FAIL_SURVIVE_THREADS

        def _run_task_and_store_result(task_idx, fs, path, offset, length):
            try:
                thread_results[task_idx] = _read_range_from_fs(fs, path, offset, length)
            except Exception as e:
                thread_exceptions[task_idx] = e

        with ThreadPoolExecutor(max_workers=_NUM_FAIL_SURVIVE_THREADS) as executor:
            futures = [
                executor.submit(_run_task_and_store_result, i, *read_tasks_args[i])
                for i in range(_NUM_FAIL_SURVIVE_THREADS)
            ]
            for future in futures:
                future.result()  # Wait for all threads to complete or for exceptions to be raised

        failed_count = sum(1 for exc in thread_exceptions if exc is not None)
        succeeded_count = sum(1 for exc in thread_exceptions if exc is None)

        assert failed_count == 1, f"Expected 1 failure, got {failed_count}"
        assert succeeded_count == _NUM_FAIL_SURVIVE_THREADS - 1

        failed_thread_idx = next(
            i for i, exc in enumerate(thread_exceptions) if exc is not None
        )

        for i in range(_NUM_FAIL_SURVIVE_THREADS):
            if i == failed_thread_idx:
                assert isinstance(thread_exceptions[i], DataCorruption)
                assert "Simulated data corruption" in str(thread_exceptions[i])
                assert thread_results[i] is None
            else:  # Other threads should have succeeded
                assert thread_exceptions[i] is None
                assert (
                    thread_results[i]
                    == _MULTI_THREADED_TEST_DATA[i * 1024 : i * 1024 + 1024]
                )

        assert mocks["create_mrd"].call_count == _NUM_FAIL_SURVIVE_THREADS
        assert (
            mocks["downloader"].download_ranges.call_count == _NUM_FAIL_SURVIVE_THREADS
        )
        assert mocks["downloader"].close.call_count == _NUM_FAIL_SURVIVE_THREADS


# =========================== Zonal Multiprocess Read Tests ===========================
def run_in_processes(func, args_list):
    """Runs a function in multiple processes and collects results."""
    # Use 'spawn' context to avoid deadlocks from libraries that are not fork-safe.
    ctx = multiprocessing.get_context("spawn")
    with ctx.Pool(processes=len(args_list)) as pool:
        results = pool.starmap(func, args_list)
    return results


def _read_range_and_get_pid(path, offset, length, block_size=None):
    """
    Helper function for multiprocessing tests. Creates a new fs instance
    in the new process and reads a range from a file.
    Returns the data and the process ID.
    """
    fs = ExtendedGcsFileSystem()
    with fs.open(path, "rb", block_size=block_size) as f:
        f.seek(offset)
        data = f.read(length)
    return data, os.getpid()


def test_multiprocess_read_disjoint_ranges_zb(extended_gcsfs):
    """
    Tests concurrent reads of disjoint ranges from the same file in different processes.
    """
    if not extended_gcsfs.on_google:
        pytest.skip("Multiprocessing tests require a live GCS backend.")

    read_tasks = [
        (_MULTI_THREADED_TEST_FILE_PATH, 0, 1024),
        (_MULTI_THREADED_TEST_FILE_PATH, 2048, 1024),
        (_MULTI_THREADED_TEST_FILE_PATH, 4096, 1024),
    ]

    results = run_in_processes(_read_range_and_get_pid, read_tasks)

    # Unpack results
    read_data = [res[0] for res in results]
    pids = [res[1] for res in results]

    # Verify data correctness
    assert read_data[0] == _MULTI_THREADED_TEST_DATA[0:1024]
    assert read_data[1] == _MULTI_THREADED_TEST_DATA[2048:3072]
    assert read_data[2] == _MULTI_THREADED_TEST_DATA[4096:5120]

    # Verify that reads happened in different processes
    assert len(set(pids)) == len(read_tasks)
    assert os.getpid() not in pids


def test_multiprocess_read_overlapping_ranges_zb(extended_gcsfs):
    """
    Tests concurrent reads of overlapping ranges from the same file in different processes.
    """
    if not extended_gcsfs.on_google:
        pytest.skip("Multiprocessing tests require a live GCS backend.")

    read_tasks = [
        (_MULTI_THREADED_TEST_FILE_PATH, 0, 2048),
        (_MULTI_THREADED_TEST_FILE_PATH, 1024, 2048),  # Overlaps with first
        (_MULTI_THREADED_TEST_FILE_PATH, 0, 2048),  # Identical to first
    ]

    results = run_in_processes(_read_range_and_get_pid, read_tasks)

    # Unpack results
    read_data = [res[0] for res in results]
    pids = [res[1] for res in results]

    # Verify data correctness
    assert read_data[0] == _MULTI_THREADED_TEST_DATA[0:2048]
    assert read_data[1] == _MULTI_THREADED_TEST_DATA[1024:3072]
    assert read_data[2] == _MULTI_THREADED_TEST_DATA[0:2048]

    # Verify that reads happened in different processes
    assert len(set(pids)) == len(read_tasks)
    assert os.getpid() not in pids


def _read_with_passed_fs(fs, path, offset, length):
    """
    Worker that receives an existing FS instance.
    Tests if ExtendedGcsFileSystem can be shared correctly.
    """
    with fs.open(path, "rb") as f:
        f.seek(offset)
        return f.read(length)


def test_multiprocess_shared_fs_zb(extended_gcsfs):
    """
    Tests passing the filesystem object itself to child processes.
    """
    if not extended_gcsfs.on_google:
        pytest.skip("Multiprocessing tests require a live GCS backend.")

    # Force initialization of the client/session beforehand
    # to ensure we test handling of active connections
    extended_gcsfs.ls(TEST_ZONAL_BUCKET)

    read_tasks = [
        (extended_gcsfs, _MULTI_THREADED_TEST_FILE_PATH, 0, 100),
        (extended_gcsfs, _MULTI_THREADED_TEST_FILE_PATH, 100, 100),
    ]

    results = run_in_processes(_read_with_passed_fs, read_tasks)

    assert results[0] == _MULTI_THREADED_TEST_DATA[0:100]
    assert results[1] == _MULTI_THREADED_TEST_DATA[100:200]


def _cat_file_worker(fs, path):
    """Simple worker to cat a file."""
    return fs.cat(path)


def test_multiprocess_shared_fs_read_multiple_files_zb(extended_gcsfs, file_path):
    """
    Tests reading completely different files in parallel using same filesystem.
    """
    if not extended_gcsfs.on_google:
        pytest.skip("Multiprocessing tests require a live GCS backend.")

    # Setup: Create 3 distinct files
    files = {
        f"{file_path}_1": b"data_one",
        f"{file_path}_2": b"data_two",
        f"{file_path}_3": b"data_three",
    }

    for path, data in files.items():
        extended_gcsfs.pipe(path, data, finalize_on_close=True)

    # Run list of paths in parallel
    task_args = [(extended_gcsfs, path) for path in files.keys()]
    results = run_in_processes(_cat_file_worker, task_args)

    # Verify we got the correct data for each file
    # Note: Results order matches input order in starmap
    assert results == [b"data_one", b"data_two", b"data_three"]


def test_multiprocess_error_handling_zb(extended_gcsfs):
    """
    Tests that exceptions in child processes are correctly raised in the parent.
    """
    if not extended_gcsfs.on_google:
        pytest.skip("Multiprocessing tests require a live GCS backend.")

    missing_file = f"{TEST_ZONAL_BUCKET}/this_does_not_exist"

    with pytest.raises(NotFound):
        run_in_processes(_cat_file_worker, [(extended_gcsfs, missing_file)])


# =========================== Zonal Upload Tests ===========================
@pytest.mark.asyncio
async def test_simple_upload_zonal(async_gcs, zonal_write_mocks, file_path):
    """Test simple_upload for Zonal buckets calls the correct writer methods."""
    data = b"test data for simple_upload"
    bucket, object_name, _ = async_gcs.split_path(file_path)

    await simple_upload(
        async_gcs,
        bucket=bucket,
        key=object_name,
        datain=data,
        finalize_on_close=True,  # Finalize on close to make the object immediately readable
    )
    if zonal_write_mocks:
        zonal_write_mocks["init_aaow"].assert_awaited_once_with(
            async_gcs.grpc_client, bucket, object_name
        )
        zonal_write_mocks["aaow"].append.assert_awaited_once_with(data)
        zonal_write_mocks["aaow"].close.assert_awaited_once_with(finalize_on_close=True)
    else:
        assert await async_gcs._cat(file_path) == data


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "unsupported_kwarg",
    [
        {"metadatain": {"key": "value"}},
        {"fixed_key_metadata": {"key": "value"}},
        {"kms_key_name": "key_name"},
        {"consistency": "md5"},
        {"content_type": "text/plain"},
    ],
)
async def test_simple_upload_zonal_unsupported_params(
    async_gcs, zonal_write_mocks, unsupported_kwarg, caplog, file_path
):
    """Test simple_upload for Zonal buckets warns on unsupported parameters."""
    bucket, object_name, _ = async_gcs.split_path(file_path)
    # Ensure caplog captures the warning by setting the level
    with caplog.at_level(logging.WARNING, logger="gcsfs"):
        await simple_upload(
            async_gcs,
            bucket=bucket,
            key=object_name,
            datain=b"",
            **unsupported_kwarg,
        )

    assert any(
        "will be ignored" in r.message and r.levelname == "WARNING"
        for r in caplog.records
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "unsupported_kwarg",
    [
        {"metadata": {"key": "value"}},
        {"fixed_key_metadata": {"key": "value"}},
        {"kms_key_name": "key_name"},
        {"content_type": "text/plain"},
    ],
)
async def test_initiate_upload_zonal_unsupported_params(
    async_gcs, zonal_write_mocks, unsupported_kwarg, caplog, file_path
):
    """Test initiate_upload for Zonal buckets warns on unsupported parameters."""
    bucket, object_name, _ = async_gcs.split_path(file_path)
    with caplog.at_level(logging.WARNING, logger="gcsfs"):
        await initiate_upload(
            fs=async_gcs,
            bucket=bucket,
            key=object_name,
            **unsupported_kwarg,
        )
    assert any(
        "will be ignored" in r.message and r.levelname == "WARNING"
        for r in caplog.records
    )


@pytest.mark.asyncio
async def test_initiate_upload_zonal(async_gcs, zonal_write_mocks, file_path):
    """Test initiate_upload for Zonal buckets returns a writer instance."""
    bucket, object_name, _ = async_gcs.split_path(file_path)
    writer = await initiate_upload(fs=async_gcs, bucket=bucket, key=object_name)
    if zonal_write_mocks:
        zonal_write_mocks["init_aaow"].assert_awaited_once_with(
            async_gcs.grpc_client, bucket, object_name
        )
        assert writer is zonal_write_mocks["aaow"]
    else:
        assert isinstance(writer, AsyncAppendableObjectWriter)
        # Close the writer to avoid leaving an open session.
        await writer.close()


@pytest.mark.asyncio
async def test_initiate_and_upload_chunk_zonal(async_gcs, zonal_write_mocks, file_path):
    """Test upload_chunk for Zonal buckets appends data."""
    size_in_bytes = 1024  # 1KB
    data1 = os.urandom(size_in_bytes - 1)
    data2 = os.urandom(size_in_bytes)
    bucket, object_name, _ = async_gcs.split_path(file_path)
    writer = await initiate_upload(fs=async_gcs, bucket=bucket, key=object_name)
    await upload_chunk(
        fs=async_gcs,
        location=writer,
        data=data1,
        offset=0,
        size=2048,
        content_type=None,
    )

    await upload_chunk(
        fs=async_gcs,
        location=writer,
        data=data2,
        offset=0,
        size=2048,
        content_type=None,
    )
    assert writer.offset == (len(data1) + len(data2))
    if zonal_write_mocks:
        assert writer.append.await_args_list == [mock.call(data1), mock.call(data2)]
        writer.close.assert_not_awaited()
    else:
        assert writer._is_stream_open
        # Finalizing is required to be able to read the object instantly
        await writer.close(finalize_on_close=True)
        assert await async_gcs._cat(file_path) == data1 + data2


@pytest.mark.asyncio
async def test_upload_chunk_zonal_final_chunk(async_gcs, zonal_write_mocks, file_path):
    """Test upload_chunk for Zonal buckets finalizes on the last chunk."""

    data = b"final chunk"
    bucket, object_name, _ = async_gcs.split_path(file_path)
    writer = await initiate_upload(fs=async_gcs, bucket=bucket, key=object_name)

    await upload_chunk(
        fs=async_gcs,
        location=writer,
        data=b"",
        offset=0,
        size=len(data),
        content_type=None,
    )  # Try uploading empty chunk
    await upload_chunk(
        fs=async_gcs,
        location=writer,
        data=data,
        offset=0,
        size=len(data),
        content_type=None,
    )  # stream should be closed now

    # The writer detects that (offset + len(data)) >= size, so it automatically
    # closes the stream here. Attempts to write again should fail.
    with pytest.raises(
        ValueError, match="Writer is closed. Please initiate a new upload."
    ):
        await upload_chunk(
            fs=async_gcs,
            location=writer,
            data=b"",
            offset=0,
            size=len(data),
            content_type=None,
        )
    if zonal_write_mocks:
        assert writer.append.await_args_list == [mock.call(b""), mock.call(data)]
        writer.close.assert_awaited_once_with(finalize_on_close=True)
    else:
        assert writer._is_stream_open is False
        assert await async_gcs._cat(file_path) == data


@pytest.mark.asyncio
async def test_upload_chunk_zonal_exception_cleanup(
    async_gcs, zonal_write_mocks, file_path
):
    """
    Tests that upload_chunk correctly closes the stream when an
    exception occurs during append, without finalizing the object.
    """
    if zonal_write_mocks is None:
        pytest.skip("Cannot mock exceptions on real GCS")
    bucket, object_name, _ = async_gcs.split_path(file_path)
    writer = await initiate_upload(fs=async_gcs, bucket=bucket, key=object_name)

    error_message = "Simulated network failure"
    writer.append.side_effect = Exception(error_message)

    with pytest.raises(Exception, match=error_message):
        await upload_chunk(
            fs=async_gcs,
            location=writer,
            data=b"some data",
            offset=0,
            size=100,
            content_type=None,
        )

    writer.close.assert_awaited_once_with(finalize_on_close=False)


@pytest.mark.asyncio
async def test_upload_chunk_zonal_wrong_type(async_gcs):
    """Test upload_chunk raises TypeError for incorrect location type."""
    with pytest.raises(TypeError, match="expects an AsyncAppendableObjectWriter"):
        await upload_chunk(
            fs=async_gcs,
            location=AsyncMultiRangeDownloader,
            data=b"",
            offset=0,
            size=0,
            content_type=None,
        )


@pytest.mark.asyncio
async def test_put_file_zonal(async_gcs, zonal_write_mocks, file_path):
    """Test _put_file for Zonal buckets."""
    data = b"test data for put_file"
    bucket, object_name, _ = async_gcs.split_path(file_path)
    with tmpfile() as lpath:
        with open(lpath, "wb") as f:
            f.write(data)

        # Finalize on close to make the object immediately readable
        await async_gcs._put_file(lpath, file_path, finalize_on_close=True)

        if zonal_write_mocks:
            grpc_client = await async_gcs._get_grpc_client()
            zonal_write_mocks["init_aaow"].assert_awaited_once_with(
                grpc_client, bucket, object_name
            )
            zonal_write_mocks["aaow"].append_from_file.assert_awaited_once()
            args, kwargs = zonal_write_mocks["aaow"].append_from_file.call_args

            file_obj_arg = args[0]
            assert hasattr(file_obj_arg, "read"), "Argument must be a file-like object"
            assert (
                file_obj_arg.name == lpath
            ), "File object must point to the correct path"

            zonal_write_mocks["aaow"].close.assert_awaited_once_with(
                finalize_on_close=True
            )
        else:
            assert await async_gcs._cat(file_path) == data


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "unsupported_kwarg",
    [
        {"metadata": {"key": "value"}},
        {"fixed_key_metadata": {"key": "value"}},
        {"consistency": "md5"},
        {"content_type": "text/plain"},
    ],
)
async def test_put_file_zonal_unsupported_params(
    async_gcs, zonal_write_mocks, unsupported_kwarg, caplog, file_path
):
    """Test _put_file for Zonal buckets warns on unsupported parameters."""
    with tmpfile() as lpath:
        with open(lpath, "wb") as f:
            f.write(b"data")

        with caplog.at_level(logging.WARNING, logger="gcsfs"):
            await async_gcs._put_file(lpath, file_path, **unsupported_kwarg)

    assert any(
        "will be ignored" in r.message and r.levelname == "WARNING"
        for r in caplog.records
    )


@pytest.mark.asyncio
async def test_pipe_file_zonal(async_gcs, zonal_write_mocks, file_path):
    """Test _pipe_file for Zonal buckets."""
    data = b"test data for pipe_file"
    bucket, object_name, _ = async_gcs.split_path(file_path)
    # Finalize on close to make the object immediately readable
    await async_gcs._pipe_file(file_path, data, finalize_on_close=True)

    if zonal_write_mocks:
        grpc_client = await async_gcs._get_grpc_client()
        zonal_write_mocks["init_aaow"].assert_awaited_once_with(
            grpc_client, bucket, object_name
        )
        zonal_write_mocks["aaow"].append.assert_awaited_once_with(data)
        zonal_write_mocks["aaow"].close.assert_awaited_once_with(finalize_on_close=True)
    else:
        assert await async_gcs._cat(file_path) == data


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "unsupported_kwarg",
    [
        {"metadata": {"key": "value"}},
        {"fixed_key_metadata": {"key": "value"}},
        {"content_type": "text/plain"},
    ],
)
async def test_pipe_file_zonal_unsupported_params(
    async_gcs, zonal_write_mocks, unsupported_kwarg, caplog, file_path
):
    """Test _pipe_file for Zonal buckets warns on unsupported parameters."""
    data = b"data"

    with caplog.at_level(logging.WARNING, logger="gcsfs"):
        await async_gcs._pipe_file(file_path, data, **unsupported_kwarg)

    assert any(
        "will be ignored" in r.message and r.levelname == "WARNING"
        for r in caplog.records
    )


@pytest.mark.asyncio
async def test_simple_upload_delegates_to_core_for_non_zonal(async_gcs):
    """
    Tests that simple_upload delegates to core.simple_upload
    when the bucket is not zonal.
    """
    key = "test-key"
    data = b"test data"

    with (
        mock.patch.object(async_gcs, "_is_zonal_bucket", return_value=False),
        mock.patch(
            "gcsfs.core.simple_upload", new_callable=mock.AsyncMock
        ) as mock_core_simple,
    ):
        mock_core_simple.return_value = {"generation": "123"}
        result = await simple_upload(
            fs=async_gcs,
            bucket=TEST_BUCKET,
            key=key,
            datain=data,
            metadatain=None,
            consistency=None,
            content_type="application/octet-stream",
            fixed_key_metadata=None,
            mode="overwrite",
            kms_key_name=None,
        )

        # Verify core.simple_upload was called with correct arguments
        mock_core_simple.assert_awaited_once_with(
            async_gcs,
            TEST_BUCKET,
            key,
            data,
            None,
            None,
            "application/octet-stream",
            None,
            "overwrite",
            None,
        )
        assert result == {"generation": "123"}


@pytest.mark.asyncio
async def test_initiate_upload_delegates_to_core_for_non_zonal(async_gcs):
    """
    Tests that initiate_upload delegates to core.initiate_upload
    when the bucket is not zonal.
    """
    key = "test-key"

    with (
        mock.patch.object(async_gcs, "_is_zonal_bucket", return_value=False),
        mock.patch(
            "gcsfs.core.initiate_upload", new_callable=mock.AsyncMock
        ) as mock_core_initiate,
    ):
        mock_core_initiate.return_value = "http://mock-resumable-url"
        result = await initiate_upload(
            fs=async_gcs,
            bucket=TEST_BUCKET,
            key=key,
            content_type="application/octet-stream",
            metadata=None,
            fixed_key_metadata=None,
            mode="overwrite",
            kms_key_name=None,
        )

        # Verify core.initiate_upload was called with correct arguments
        mock_core_initiate.assert_awaited_once_with(
            async_gcs,
            TEST_BUCKET,
            key,
            "application/octet-stream",
            None,
            None,
            "overwrite",
            None,
        )
        assert result == "http://mock-resumable-url"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "http_location",
    [
        pytest.param(
            "https://www.googleapis.com/upload/storage/v1/b/bucket/o?uploadType=resumable&upload_id=abc123",
            id="string_url",
        ),
        pytest.param(
            b"https://www.googleapis.com/upload/storage/v1/b/bucket/o?uploadType=resumable&upload_id=abc123",
            id="bytes_url",
        ),
    ],
)
async def test_upload_chunk_delegates_to_core_for_http_url(async_gcs, http_location):
    """
    Tests that upload_chunk delegates to core.upload_chunk
    when location is an HTTP resumable-upload URL (string or bytes).
    """
    data = b"chunk data"
    offset = 0
    size = 1024
    content_type = "application/octet-stream"

    with mock.patch(
        "gcsfs.core.upload_chunk", new_callable=mock.AsyncMock
    ) as mock_core_upload:
        mock_core_upload.return_value = {"kind": "storage#object"}
        result = await upload_chunk(
            fs=async_gcs,
            location=http_location,
            data=data,
            offset=offset,
            size=size,
            content_type=content_type,
        )

        # Verify core.upload_chunk was called with correct arguments
        mock_core_upload.assert_awaited_once_with(
            async_gcs,
            http_location,
            data,
            offset,
            size,
            content_type,
        )
        assert result == {"kind": "storage#object"}


def test_get_file_from_zonal_bucket(extended_gcsfs, gcs_bucket_mocks):
    """Test getting a file from a Zonal bucket with mocks."""
    with gcs_bucket_mocks(
        json_data, bucket_type_val=BucketType.ZONAL_HIERARCHICAL
    ) as mocks:
        bucket, key, _ = extended_gcsfs.split_path(file_path)

        with tmpfile() as local_f:
            extended_gcsfs.get(file_path, local_f)
            with open(local_f, "rb") as f:
                assert f.read() == json_data
        if mocks:
            mocks["downloader"].download_ranges.assert_awaited()
            mocks["downloader"].close.assert_awaited_once()


async def create_mrd_side_effect(client, bucket, object_name, generation):
    """Side effect function to create a mocked AsyncMultiRangeDownloader."""
    file_data = files[object_name]

    async def download_side_effect(read_requests, **kwargs):
        for param_offset, param_length, buffer_arg in read_requests:
            if hasattr(buffer_arg, "write"):
                buffer_arg.write(file_data[param_offset : param_offset + param_length])

    downloader = mock.Mock(spec=AsyncMultiRangeDownloader)
    downloader.download_ranges = mock.AsyncMock(side_effect=download_side_effect)
    downloader.persisted_size = len(file_data)
    downloader.close = mock.AsyncMock()
    return downloader


def test_get_list_from_zonal_bucket(extended_gcsfs):
    """Test batch downloading a list of files with mocks."""
    if extended_gcsfs.on_google:
        with tmpfile() as l1, tmpfile() as l2:
            extended_gcsfs.get([file_path, file2_path], [l1, l2])

            with open(l1, "rb") as f:
                assert f.read() == files[file]
            with open(l2, "rb") as f:
                assert f.read() == files[file2]
        return

    mock_create_mrd = mock.AsyncMock(side_effect=create_mrd_side_effect)
    with (
        mock.patch(
            "gcsfs.extended_gcsfs.ExtendedGcsFileSystem._is_zonal_bucket",
            return_value=True,
        ),
        mock.patch(
            "google.cloud.storage._experimental.asyncio."
            "async_multi_range_downloader.AsyncMultiRangeDownloader.create_mrd",
            mock_create_mrd,
        ),
    ):
        with tmpfile() as l1, tmpfile() as l2:
            extended_gcsfs.get([file_path, file2_path], [l1, l2])

            with open(l1, "rb") as f:
                assert f.read() == files[file]
            with open(l2, "rb") as f:
                assert f.read() == files[file2]

    assert mock_create_mrd.call_count == 2


def test_get_directory_from_zonal_bucket(extended_gcsfs):
    """Test getting a directory recursively from a Zonal bucket with mocks."""
    remote_dir = f"{TEST_ZONAL_BUCKET}/test"
    file1 = "test/accounts.1.json"
    file2 = "test/accounts.2.json"
    if extended_gcsfs.on_google:
        with tempdir() as tmp_root:
            # define a path that DOES NOT exist yet
            local_dir = os.path.join(tmp_root, "downloaded_data")

            # This should create 'downloaded_data' AND put files inside it
            extended_gcsfs.get(remote_dir, local_dir, recursive=True)

            # Assert folder was created
            assert os.path.isdir(local_dir)
            with open(os.path.join(local_dir, "accounts.1.json"), "rb") as f:
                assert f.read() == files[file1]
            with open(os.path.join(local_dir, "accounts.2.json"), "rb") as f:
                assert f.read() == files[file2]
        return

    mock_create_mrd = mock.AsyncMock(side_effect=create_mrd_side_effect)

    with (
        mock.patch(
            "gcsfs.extended_gcsfs.ExtendedGcsFileSystem._is_zonal_bucket",
            return_value=True,
        ),
        mock.patch(
            "google.cloud.storage._experimental.asyncio."
            "async_multi_range_downloader.AsyncMultiRangeDownloader.create_mrd",
            mock_create_mrd,
        ),
    ):
        with tempdir() as tmp_root:
            local_dir = os.path.join(tmp_root, "downloaded_data")

            extended_gcsfs.get(remote_dir, local_dir, recursive=True)

            assert os.path.isdir(local_dir)
            with open(os.path.join(local_dir, "accounts.1.json"), "rb") as f:
                assert f.read() == files[file1]
            with open(os.path.join(local_dir, "accounts.2.json"), "rb") as f:
                assert f.read() == files[file2]

    assert mock_create_mrd.call_count == 2


@pytest.mark.asyncio
async def test_get_file_warning_on_missing_persisted_size(
    async_gcs, gcs_bucket_mocks, caplog, tmp_path, file_path
):
    """
    Tests that a warning is logged in _get_file when MRD has no 'persisted_size' attribute.
    """
    with gcs_bucket_mocks(
        json_data, bucket_type_val=BucketType.ZONAL_HIERARCHICAL
    ) as mocks:
        if not mocks:
            pytest.skip("Cannot simulate missing attributes on real GCS.")

        lpath = tmp_path / "output.txt"
        with (
            caplog.at_level(logging.WARNING, logger="gcsfs"),
            mock.patch.object(
                async_gcs, "_info", new_callable=mock.AsyncMock
            ) as mock_info,
        ):
            mock_info.return_value = {"size": len(json_data)}
            await async_gcs._get_file(file_path, str(lpath))
            assert "Falling back to _info() to get the file size" in caplog.text
            assert lpath.read_bytes() == json_data


@pytest.mark.asyncio
async def test_get_file_exception_cleanup(
    async_gcs, gcs_bucket_mocks, tmp_path, file_path
):
    """
    Tests that _get_file correctly removes the local file when an
    exception occurs during download.
    """

    with gcs_bucket_mocks(
        json_data, bucket_type_val=BucketType.ZONAL_HIERARCHICAL
    ) as mocks:
        if not mocks:
            pytest.skip("Cannot mock exceptions on real GCS.")

        lpath = tmp_path / "output.txt"
        error_message = "Simulated network failure during download"
        with (
            mock.patch(
                "gcsfs.zb_hns_utils.download_range",
                side_effect=Exception(error_message),
            ),
            mock.patch.object(
                async_gcs, "_info", new_callable=mock.AsyncMock
            ) as mock_info,
        ):
            mock_info.return_value = {"size": len(json_data)}
            with pytest.raises(Exception, match=error_message):
                await async_gcs._get_file(file_path, str(lpath))

            # The local file should not exist after the failed download
            assert not lpath.exists()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "source_bucket, dest_bucket, should_fail",
    [
        (TEST_ZONAL_BUCKET, TEST_ZONAL_BUCKET, True),
        (TEST_ZONAL_BUCKET, TEST_BUCKET, True),
        (TEST_BUCKET, TEST_ZONAL_BUCKET, True),
        (TEST_BUCKET, TEST_BUCKET, False),
    ],
)
async def test_cp_file_not_implemented_error(
    async_gcs, source_bucket, dest_bucket, should_fail
):
    """
    Tests _cp_file behavior for combinations of Zonal and Standard buckets.
    """
    short_uuid = str(uuid.uuid4())[:8]
    source_path = f"{source_bucket}/source_{short_uuid}"
    dest_path = f"{dest_bucket}/dest_{short_uuid}"
    is_real_gcs = os.getenv("STORAGE_EMULATOR_HOST") == "https://storage.googleapis.com"

    # Source file needs to exist for last case when super method is called for standard buckets
    if is_real_gcs:
        await async_gcs._pipe_file(source_path, b"test data", finalize_on_close=True)

    async def mock_is_zonal(bucket):
        return bucket == TEST_ZONAL_BUCKET

    is_zonal_patch_cm = (
        mock.patch.object(async_gcs, "_is_zonal_bucket", side_effect=mock_is_zonal)
        if not is_real_gcs
        else contextlib.nullcontext()
    )

    with is_zonal_patch_cm:
        if should_fail:
            with pytest.raises(
                NotImplementedError,
                match=(
                    r"Server-side copy involving Zonal buckets is not supported. "
                    r"Zonal objects do not support rewrite."
                ),
            ):
                await async_gcs._cp_file(source_path, dest_path)
        else:  # Standard -> Standard
            if is_real_gcs:
                await async_gcs._cp_file(source_path, dest_path)
                assert await async_gcs._cat(dest_path) == b"test data"
            else:
                with mock.patch(
                    "gcsfs.core.GCSFileSystem._cp_file", new_callable=mock.AsyncMock
                ) as mock_super_cp:
                    await async_gcs._cp_file(source_path, dest_path)
                    mock_super_cp.assert_awaited_once()


@pytest.mark.asyncio
async def test_group_requests_by_bucket_type(async_gcs):
    """Test grouping of requests into zonal batches and regional requests."""
    paths = ["gs://zonal/obj1", "gs://regional/obj2", "gs://zonal/obj3"]
    starts = [0, 10, 20]
    ends = [5, 15, 25]

    async def mock_is_zonal(bucket):
        return bucket == "zonal"

    with mock.patch.object(async_gcs, "_is_zonal_bucket", side_effect=mock_is_zonal):
        zonal_batches, regional_requests = (
            await async_gcs._group_requests_by_bucket_type(paths, starts, ends)
        )

    # Check zonal batches
    # Keys are (bucket, object_name, generation)
    # paths[0] -> zonal/obj1 -> key ("zonal", "obj1", None)
    # paths[2] -> zonal/obj3 -> key ("zonal", "obj3", None)
    assert len(zonal_batches) == 2
    assert ("zonal", "obj1", None) in zonal_batches
    assert ("zonal", "obj3", None) in zonal_batches

    # Check batch content: (index, path, start, end)
    assert zonal_batches[("zonal", "obj1", None)] == [(0, "gs://zonal/obj1", 0, 5)]
    assert zonal_batches[("zonal", "obj3", None)] == [(2, "gs://zonal/obj3", 20, 25)]

    # Check regional requests
    assert len(regional_requests) == 1
    assert regional_requests[0] == (1, "gs://regional/obj2", 10, 15)


@pytest.mark.asyncio
async def test_fetch_zonal_batch(async_gcs):
    """Test fetching a batch of ranges for a zonal object."""
    key = ("zonal-bucket", "obj1", "gen1")
    batch = [
        (0, "gs://zonal-bucket/obj1", 0, 10),
        (2, "gs://zonal-bucket/obj1", 20, 30),
    ]

    mock_mrd = mock.AsyncMock()
    mock_mrd.persisted_size = 100

    async def mock_get_grpc_client():
        async_gcs._grpc_client = mock.Mock()
        return async_gcs._grpc_client

    with (
        mock.patch(
            "gcsfs.extended_gcsfs.AsyncMultiRangeDownloader.create_mrd",
            return_value=mock_mrd,
        ) as mock_create_mrd,
        mock.patch(
            "gcsfs.zb_hns_utils.download_ranges", return_value=[b"data1", b"data2"]
        ) as mock_download_ranges,
        mock.patch.object(
            async_gcs, "_get_grpc_client", side_effect=mock_get_grpc_client
        ),
        mock.patch.object(
            async_gcs,
            "_process_limits_to_offset_and_length",
            new_callable=mock.AsyncMock,
            side_effect=[(0, 10), (20, 10)],
        ),
    ):
        results = await async_gcs._fetch_zonal_batch(key, batch)

    assert results == [(0, b"data1"), (2, b"data2")]

    mock_create_mrd.assert_awaited_once()
    mock_download_ranges.assert_awaited_once()
    # Check args to download_ranges: list of (offset, length)
    call_args = mock_download_ranges.call_args
    assert call_args[0][0] == [(0, 10), (20, 10)]
    assert call_args[0][1] == mock_mrd
    mock_mrd.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_fetch_zonal_batch_fallback_info(async_gcs):
    """Test fetching a batch of ranges for a zonal object with fallback to _info."""
    key = ("zonal-bucket", "obj1", "gen1")
    batch = [(0, "gs://zonal-bucket/obj1", 0, 10)]

    mock_mrd = mock.AsyncMock()
    mock_mrd.persisted_size = None  # Trigger fallback

    async def mock_get_grpc_client():
        async_gcs._grpc_client = mock.Mock()
        return async_gcs._grpc_client

    with (
        mock.patch(
            "gcsfs.extended_gcsfs.AsyncMultiRangeDownloader.create_mrd",
            return_value=mock_mrd,
        ),
        mock.patch("gcsfs.zb_hns_utils.download_ranges", return_value=[b"data1"]),
        mock.patch.object(
            async_gcs, "_get_grpc_client", side_effect=mock_get_grpc_client
        ),
        mock.patch.object(
            async_gcs, "_info", new_callable=mock.AsyncMock, return_value={"size": 100}
        ) as mock_info,
        mock.patch.object(
            async_gcs,
            "_process_limits_to_offset_and_length",
            new_callable=mock.AsyncMock,
            return_value=(0, 10),
        ),
        mock.patch("gcsfs.extended_gcsfs.logger") as mock_logger,
    ):
        await async_gcs._fetch_zonal_batch(key, batch)
        mock_logger.warning.assert_called_once()
        assert "no 'persisted_size'" in mock_logger.warning.call_args[0][0]

    mock_info.assert_awaited_once_with("zonal-bucket/obj1")


@pytest.mark.asyncio
async def test_cat_ranges_1001_ranges(extended_gcsfs):
    """
    Test that cat_ranges correctly handles a large number of ranges (1001) without hitting mrd argument limits.
    """
    if extended_gcsfs.on_google:
        pytest.skip(
            "Mock-based test for handling large number of ranges; not suitable for live GCS."
        )
    # Setup Inputs
    num_ranges = MRD_MAX_RANGES + 1  # 1001 ranges
    paths = ["gs://zonal/obj1"] * num_ranges

    # Mock the low level method to simulate Zonal behavior
    async def mock_fetch_zonal(key, batch):
        # Emulate Zonal return: list of (index, data) tuples
        return [(item[0], b"data") for item in batch]

    with (
        mock.patch.object(
            extended_gcsfs, "_fetch_zonal_batch", side_effect=mock_fetch_zonal
        ) as m_fetch,
        mock.patch.object(extended_gcsfs, "_is_zonal_bucket", return_value=True),
    ):
        results = await extended_gcsfs._cat_ranges(
            paths, range(num_ranges), range(1, num_ranges + 1)
        )

    # Assertions
    assert len(results) == num_ranges
    assert m_fetch.call_count == 2

    # args[1] is the batch list. We expect one batch of 1000 and one of 1.
    batch_sizes = [len(call.args[1]) for call in m_fetch.call_args_list]
    assert sorted(batch_sizes, reverse=True) == [MRD_MAX_RANGES, 1]

    # Verify the key was correct for both calls
    assert all(
        call.args[0] == ("zonal", "obj1", None) for call in m_fetch.call_args_list
    )


def test_cat_ranges_sync_mixed_integration(extended_gcsfs):
    """
    Sync Integration Test: Verifies that synchronous 'cat_ranges'
    correctly handles a mix of Zonal and Regional files without event loop issues.
    """
    # Setup Inputs
    paths = ["gs://zonal/obj1", "gs://regional/obj2"]
    starts = [0, 10]
    ends = [5, 15]

    # Mock the low level methods to simulate Zonal and Regional behavior
    async def mock_fetch_zonal(key, batch):
        # Emulate Zonal return: list of (index, data) tuples
        indices = [item[0] for item in batch]
        return [(idx, b"ZONAL_DATA") for idx in indices]

    async def mock_cat_file(path, start, end, **kwargs):
        # Emulate Regional return: single bytes object
        return b"REGIONAL_DATA"

    async def mock_is_zonal(bucket):
        return bucket == "zonal"

    # Apply Mocks & Execute
    with (
        mock.patch.object(
            extended_gcsfs, "_fetch_zonal_batch", side_effect=mock_fetch_zonal
        ) as m_zonal,
        mock.patch(
            "gcsfs.core.GCSFileSystem._cat_file", side_effect=mock_cat_file
        ) as m_regional,
        mock.patch.object(
            extended_gcsfs, "_is_zonal_bucket", side_effect=mock_is_zonal
        ),
    ):
        results = extended_gcsfs.cat_ranges(paths, starts, ends)

    # Assertions
    assert results == [b"ZONAL_DATA", b"REGIONAL_DATA"]
    # Zonal should be called for obj1
    m_zonal.assert_called_once()
    args, _ = m_zonal.call_args
    key, batch = args
    assert key == ("zonal", "obj1", None)
    assert batch[0][1] == "gs://zonal/obj1"  # path check

    # Regional should be called for obj2
    m_regional.assert_called_once()
    assert m_regional.call_args[0][0] == "gs://regional/obj2"
