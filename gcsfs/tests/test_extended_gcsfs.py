import contextlib
import io
import os
from itertools import chain
from unittest import mock

import pytest
from google.cloud.storage._experimental.asyncio.async_multi_range_downloader import (
    AsyncMultiRangeDownloader,
)
from google.cloud.storage.exceptions import DataCorruption

from gcsfs.extended_gcsfs import BucketType
from gcsfs.tests.conftest import csv_files, files, text_files
from gcsfs.tests.settings import TEST_ZONAL_BUCKET

file = "test/accounts.1.json"
file_path = f"{TEST_ZONAL_BUCKET}/{file}"
json_data = files[file]
lines = io.BytesIO(json_data).readlines()
file_size = len(json_data)

REQUIRED_ENV_VAR = "GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT"

a = TEST_ZONAL_BUCKET + "/tmp/test/a"
b = TEST_ZONAL_BUCKET + "/tmp/test/b"
c = TEST_ZONAL_BUCKET + "/tmp/test/c"

# If the condition is True, only then tests in this file are run.
should_run = os.getenv(REQUIRED_ENV_VAR, "false").lower() in (
    "true",
    "1",
)
pytestmark = pytest.mark.skipif(
    not should_run, reason=f"Skipping tests: {REQUIRED_ENV_VAR} env variable is not set"
)


@pytest.fixture
def zonal_mocks():
    """A factory fixture for mocking Zonal bucket functionality."""

    @contextlib.contextmanager
    def _zonal_mocks_factory(file_data):
        """Creates mocks for a given file content."""
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
            if read_requests and len(read_requests) == 1:
                param_offset, param_length, buffer_arg = read_requests[0]
                if hasattr(buffer_arg, "write"):
                    buffer_arg.write(
                        file_data[param_offset : param_offset + param_length]
                    )
            return [mock.Mock(error=None)]

        mock_downloader = mock.Mock(spec=AsyncMultiRangeDownloader)
        mock_downloader.download_ranges = mock.AsyncMock(
            side_effect=download_side_effect
        )

        mock_create_mrd = mock.AsyncMock(return_value=mock_downloader)
        with (
            mock.patch(
                patch_target_sync_lookup_bucket_type,
                return_value=BucketType.ZONAL_HIERARCHICAL,
            ) as mock_sync_lookup_bucket_type,
            mock.patch(
                patch_target_lookup_bucket_type,
                return_value=BucketType.ZONAL_HIERARCHICAL,
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

    yield _zonal_mocks_factory


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


def test_read_block_zb(extended_gcsfs, zonal_mocks, subtests):
    for param in read_block_params:
        with subtests.test(id=param.id):
            offset, length, delimiter, expected_data = param.values
            path = file_path

            with zonal_mocks(json_data) as mocks:
                result = extended_gcsfs.read_block(path, offset, length, delimiter)

                assert result == expected_data
                if mocks:
                    mocks["sync_lookup_bucket_type"].assert_called_once_with(
                        TEST_ZONAL_BUCKET
                    )
                    if expected_data:
                        mocks["downloader"].download_ranges.assert_called_with(
                            [(offset, mock.ANY, mock.ANY)]
                        )
                    else:
                        mocks["downloader"].download_ranges.assert_not_called()


def test_read_small_zb(extended_gcsfs, zonal_mocks):
    csv_file = "2014-01-01.csv"
    csv_file_path = f"{TEST_ZONAL_BUCKET}/{csv_file}"
    csv_data = csv_files[csv_file]

    with zonal_mocks(csv_data) as mocks:
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


def test_readline_zb(extended_gcsfs, zonal_mocks):
    all_items = chain.from_iterable(
        [files.items(), csv_files.items(), text_files.items()]
    )
    for k, data in all_items:
        with zonal_mocks(data):
            with extended_gcsfs.open("/".join([TEST_ZONAL_BUCKET, k]), "rb") as f:
                result = f.readline()
                expected = data.split(b"\n")[0] + (b"\n" if data.count(b"\n") else b"")
            assert result == expected


def test_readline_from_cache_zb(extended_gcsfs, zonal_mocks):
    data = b"a,b\n11,22\n3,4"
    if not extended_gcsfs.on_google:
        with mock.patch.object(
            extended_gcsfs, "_sync_lookup_bucket_type", return_value=BucketType.UNKNOWN
        ):
            with extended_gcsfs.open(a, "wb") as f:
                f.write(data)
    with zonal_mocks(data):
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


def test_readline_empty_zb(extended_gcsfs, zonal_mocks):
    data = b""
    if not extended_gcsfs.on_google:
        with mock.patch.object(
            extended_gcsfs, "_sync_lookup_bucket_type", return_value=BucketType.UNKNOWN
        ):
            with extended_gcsfs.open(b, "wb") as f:
                f.write(data)
    with zonal_mocks(data):
        with extended_gcsfs.open(b, "rb") as f:
            result = f.readline()
            assert result == data


def test_readline_blocksize_zb(extended_gcsfs, zonal_mocks):
    data = b"ab\n" + b"a" * (2**18) + b"\nab"
    if not extended_gcsfs.on_google:
        with mock.patch.object(
            extended_gcsfs, "_sync_lookup_bucket_type", return_value=BucketType.UNKNOWN
        ):
            with extended_gcsfs.open(c, "wb") as f:
                f.write(data)
    with zonal_mocks(data):
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
    "start,end,exp_offset,exp_length,exp_exc",
    [
        (None, None, 0, file_size, None),  # full file
        (-10, None, file_size - 10, 10, None),  # start negative
        (10, -10, 10, file_size - 20, None),  # end negative
        (20, 20, 20, 0, None),  # zero-length slice
        (50, 40, None, None, ValueError),  # end before start -> raises
        (-200, None, None, None, ValueError),  # offset negative -> raises
        (file_size - 10, 200, file_size - 10, 10, None),  # end > size clamps
        (
            file_size + 10,
            file_size + 20,
            file_size + 10,
            0,
            None,
        ),  # offset > size -> empty
    ],
)
def test_process_limits_parametrized(
    extended_gcsfs, start, end, exp_offset, exp_length, exp_exc
):
    if exp_exc is not None:
        with pytest.raises(exp_exc):
            extended_gcsfs.sync_process_limits_to_offset_and_length(
                file_path, start, end
            )
    else:
        offset, length = extended_gcsfs.sync_process_limits_to_offset_and_length(
            file_path, start, end
        )
        assert offset == exp_offset
        assert length == exp_length


@pytest.mark.parametrize(
    "exception_to_raise",
    [ValueError, DataCorruption, Exception],
)
def test_mrd_exception_handling(extended_gcsfs, zonal_mocks, exception_to_raise):
    """
    Tests that _cat_file correctly propagates exceptions from mrd.download_ranges.
    """
    with zonal_mocks(json_data) as mocks:
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


def test_mrd_stream_cleanup(extended_gcsfs, zonal_mocks):
    """
    Tests that mrd stream is properly closed with file closure.
    """
    with zonal_mocks(json_data) as mocks:
        if not extended_gcsfs.on_google:

            def close_side_effect():
                mocks["downloader"].is_stream_open = False

            mocks["downloader"].close.side_effect = close_side_effect

        with extended_gcsfs.open(file_path, "rb") as f:
            assert f.mrd is not None

        assert True is f.closed
        assert False is f.mrd.is_stream_open
