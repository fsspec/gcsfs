# Unit tests for ExtendedGCSFileSystem.
import io
import logging
import os
from unittest import mock

import pytest
from google.cloud.storage.asyncio.async_multi_range_downloader import (
    AsyncMultiRangeDownloader,
)
from google.cloud.storage.exceptions import DataCorruption

from gcsfs.checkers import ConsistencyChecker, MD5Checker, SizeChecker
from gcsfs.extended_gcsfs import (
    BucketType,
    ExtendedGcsFileSystem,
    initiate_upload,
    simple_upload,
    upload_chunk,
)
from gcsfs.tests.conftest import csv_files, files
from gcsfs.tests.settings import TEST_BUCKET, TEST_ZONAL_BUCKET
from gcsfs.tests.test_extended_gcsfs import gcs_bucket_mocks  # noqa: F401
from gcsfs.tests.utils import tmpfile

file = "test/accounts.1.json"
file_path = f"{TEST_ZONAL_BUCKET}/{file}"
json_data = files[file]
lines = io.BytesIO(json_data).readlines()
file_size = len(json_data)

a = TEST_ZONAL_BUCKET + "/zonal/test/a"
b = TEST_ZONAL_BUCKET + "/zonal/test/b"
c = TEST_ZONAL_BUCKET + "/zonal/test/c"

REQUIRED_ENV_VAR = "GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT"

# If the condition is True, only then tests in this file are run.
should_run = os.getenv(REQUIRED_ENV_VAR, "false").lower() in (
    "true",
    "1",
)
pytestmark = [
    pytest.mark.skipif(
        not should_run,
        reason=f"Skipping tests: {REQUIRED_ENV_VAR} env variable is not set",
    ),
    pytest.mark.skipif(
        os.environ.get("STORAGE_EMULATOR_HOST") == "https://storage.googleapis.com",
        reason="Contains Unit tests using mocks, does not require testing on real GCS.",
    ),
]


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


def test_mrd_created_once_for_zonal_file(extended_gcsfs, gcs_bucket_mocks):
    """
    Tests that the AsyncMultiRangeDownloader (MRD) is created only once when a
    ZonalFile is opened, and not for each subsequent read operation.
    """
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


def test_zonal_file_warning_on_missing_persisted_size(
    extended_gcsfs, gcs_bucket_mocks, caplog
):
    """
    Tests that a warning is logged when MRD has no 'persisted_size' attribute when opening ZonalFile.
    """
    with gcs_bucket_mocks(json_data, bucket_type_val=BucketType.ZONAL_HIERARCHICAL):
        # 'persisted_size' is set to None in the mock downloader
        with caplog.at_level(logging.WARNING, logger="gcsfs"):
            with extended_gcsfs.open(file_path, "rb"):
                pass
            assert "has no 'persisted_size'" in caplog.text


@pytest.mark.asyncio
async def test_cat_file_warning_on_missing_persisted_size(
    extended_gcsfs, gcs_bucket_mocks, caplog
):
    """
    Tests that a warning is logged in cat_file when MRD has no 'persisted_size' attribute.
    """
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
async def test_upload_chunk_zonal_exception_cleanup(
    async_gcs, zonal_write_mocks, file_path
):
    """
    Tests that upload_chunk correctly closes the stream when an
    exception occurs during append, without finalizing the object.
    """
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


@pytest.mark.asyncio
async def test_get_file_warning_on_missing_persisted_size(
    async_gcs, gcs_bucket_mocks, caplog, tmp_path, file_path
):
    """
    Tests that a warning is logged in _get_file when MRD has no 'persisted_size' attribute.
    """
    with gcs_bucket_mocks(json_data, bucket_type_val=BucketType.ZONAL_HIERARCHICAL):
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

    with gcs_bucket_mocks(json_data, bucket_type_val=BucketType.ZONAL_HIERARCHICAL):
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
async def test_merge_zonal_not_supported(async_gcs, zonal_write_mocks):
    """Test _merge for Zonal buckets raises NotImplementedError."""
    path = f"{TEST_ZONAL_BUCKET}/merged_file"
    paths = [f"{TEST_ZONAL_BUCKET}/file1", f"{TEST_ZONAL_BUCKET}/file2"]

    with pytest.raises(
        NotImplementedError,
        match="Server-side compose/merge is not supported for Zonal buckets.",
    ):
        await async_gcs._merge(path, paths)


@pytest.mark.asyncio
async def test_merge_delegates_to_core_for_non_zonal(async_gcs):
    """Test _merge delegates to core._merge when the bucket is not zonal."""
    path = f"{TEST_BUCKET}/merged_file"
    paths = [f"{TEST_BUCKET}/file1", f"{TEST_BUCKET}/file2"]

    with (
        mock.patch.object(async_gcs, "_is_zonal_bucket", return_value=False),
        mock.patch(
            "gcsfs.core.GCSFileSystem._merge", new_callable=mock.AsyncMock
        ) as mock_core_merge,
    ):
        await async_gcs._merge(path, paths, acl="public-read")
        mock_core_merge.assert_awaited_once_with(path, paths, acl="public-read")


@pytest.mark.parametrize(
    "requester_pays, expected_quota_project",
    [
        ("requester-project", "requester-project"),  # Case 1: string
        (True, "dummy-billing-project"),  # Case 2: True (fallback)
    ],
)
@pytest.mark.asyncio
async def test_async_grpc_client_init_quota_project(
    requester_pays, expected_quota_project
):
    """
    Verifies that ExtendedGcsFileSystem initializes AsyncGrpcClient
    with the correct quota_project_id in ClientOptions derived from requester_pays.
    """
    project = "dummy-billing-project"

    fs = ExtendedGcsFileSystem(project=project, requester_pays=requester_pays)

    with mock.patch("gcsfs.extended_gcsfs.AsyncGrpcClient") as mock_grpc_client:
        # Trigger client initialization
        await fs._get_grpc_client()

        mock_grpc_client.assert_called_once()
        _, kwargs = mock_grpc_client.call_args
        client_options = kwargs.get("client_options")
        assert client_options is not None
        assert client_options.quota_project_id == expected_quota_project
