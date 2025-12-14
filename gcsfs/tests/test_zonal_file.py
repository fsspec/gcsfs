"""Tests for ZonalFile write operations."""

import os
from unittest import mock

import pytest

from gcsfs.extended_gcsfs import BucketType
from gcsfs.tests.settings import TEST_ZONAL_BUCKET

file_path = f"{TEST_ZONAL_BUCKET}/zonal-file-test"
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


@pytest.fixture
def zonal_write_mocks():
    """A fixture for mocking Zonal bucket write functionality."""

    patch_target_get_bucket_type = (
        "gcsfs.extended_gcsfs.ExtendedGcsFileSystem._get_bucket_type"
    )
    patch_target_init_aaow = "gcsfs.zb_hns_utils.init_aaow"

    mock_aaow = mock.AsyncMock()
    mock_init_aaow = mock.AsyncMock(return_value=mock_aaow)
    with (
        mock.patch(
            patch_target_get_bucket_type,
            return_value=BucketType.ZONAL_HIERARCHICAL,
        ),
        mock.patch(patch_target_init_aaow, mock_init_aaow),
    ):
        mocks = {
            "aaow": mock_aaow,
            "init_aaow": mock_init_aaow,
        }
        yield mocks


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
    extended_gcsfs, zonal_write_mocks, setup_action, error_match  # noqa: F841
):
    """Test ZonalFile.write raises ValueError for invalid states."""
    with extended_gcsfs.open(file_path, "wb") as f:
        setup_action(f)
        with pytest.raises(ValueError, match=error_match):
            f.write(test_data)


def test_zonal_file_write_success(extended_gcsfs, zonal_write_mocks):
    """Test that writing to a ZonalFile calls the underlying writer's append method."""
    with extended_gcsfs.open(file_path, "wb") as f:
        f.write(test_data)

    zonal_write_mocks["aaow"].append.assert_awaited_once_with(test_data)


def test_zonal_file_open_write_mode(extended_gcsfs, zonal_write_mocks):
    """Test that opening a ZonalFile in write mode initializes the writer."""
    bucket, key, _ = extended_gcsfs.split_path(file_path)
    with extended_gcsfs.open(file_path, "wb"):
        pass

    zonal_write_mocks["init_aaow"].assert_called_once_with(
        extended_gcsfs.grpc_client, bucket, key
    )


def test_zonal_file_flush(extended_gcsfs, zonal_write_mocks):
    """Test that flush calls the underlying writer's flush method."""
    with extended_gcsfs.open(file_path, "wb") as f:
        f.flush()

    zonal_write_mocks["aaow"].flush.assert_awaited()


def test_zonal_file_commit(extended_gcsfs, zonal_write_mocks):
    """Test that commit finalizes the write and sets autocommit to True."""
    with extended_gcsfs.open(file_path, "wb") as f:
        f.commit()

    zonal_write_mocks["aaow"].finalize.assert_awaited_once()
    assert f.autocommit is True


def test_zonal_file_discard(extended_gcsfs, zonal_write_mocks):  # noqa: F841
    """Test that discard on a ZonalFile logs a warning."""
    with mock.patch("gcsfs.zonal_file.logger") as mock_logger:
        with extended_gcsfs.open(file_path, "wb") as f:
            f.discard()
        mock_logger.warning.assert_called_once()
        assert (
            "Discard is not applicable for Zonal Buckets"
            in mock_logger.warning.call_args[0][0]
        )


def test_zonal_file_close(extended_gcsfs, zonal_write_mocks):
    """Test that close finalizes the write by default (autocommit=True)."""
    with extended_gcsfs.open(file_path, "wb"):
        pass
    zonal_write_mocks["aaow"].close.assert_awaited_once_with(finalize_on_close=True)


def test_zonal_file_close_with_autocommit_false(extended_gcsfs, zonal_write_mocks):
    """Test that close does not finalize the write when autocommit is False."""

    with extended_gcsfs.open(file_path, "wb", autocommit=False):
        pass  # close is called on exit

    zonal_write_mocks["aaow"].close.assert_awaited_once_with(finalize_on_close=False)


@pytest.mark.parametrize(
    "method_name",
    [
        ("_initiate_upload"),
        ("_simple_upload"),
        ("_upload_chunk"),
    ],
)
def test_zonal_file_not_implemented_methods(
    extended_gcsfs, zonal_write_mocks, method_name  # noqa: F841
):
    """Test that some GCSFile methods are not implemented for ZonalFile."""
    with extended_gcsfs.open(file_path, "wb") as f:
        method_to_call = getattr(f, method_name)
        with pytest.raises(NotImplementedError):
            method_to_call()


@pytest.mark.skipif(
    os.environ.get("STORAGE_EMULATOR_HOST") != "https://storage.googleapis.com",
    reason="This test class is for real GCS only.",
)
class TestZonalFileRealGCS:
    """
    Contains tests for ZonalFile write operations that run only against a
    real GCS backend. These tests validate end-to-end write behavior.
    """

    def test_simple_upload_overwrite_behavior(self, extended_gcsfs):
        """Tests simple writes to a ZonalFile and verifies the content is overwritten"""
        with extended_gcsfs.open(file_path, "wb") as f:
            f.write(test_data)
        with extended_gcsfs.open(file_path, "wb", content_type="text/plain") as f:
            f.write(b"Sample text data.")
        assert extended_gcsfs.cat(file_path) == b"Sample text data."

    def test_large_upload(self, extended_gcsfs):
        """Tests writing a large chunk of data to a ZonalFile."""
        large_data = b"a" * (5 * 1024 * 1024)  # 5MB
        with extended_gcsfs.open(file_path, "wb") as f:
            f.write(large_data)
        assert extended_gcsfs.cat(file_path) == large_data

    def test_multiple_writes(self, extended_gcsfs):
        """Tests multiple write calls to the same ZonalFile handle."""
        data1 = b"first part "
        data2 = b"second part"
        with extended_gcsfs.open(file_path, "wb") as f:
            f.write(data1)
            f.write(data2)
        assert extended_gcsfs.cat(file_path) == data1 + data2
