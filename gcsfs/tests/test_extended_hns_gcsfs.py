"""
This module contains unit tests for the ExtendedGcsFileSystem, focusing on
Hierarchical Namespace (HNS) enabled features.

These tests are designed to run with mocked GCS backends to isolate and verify
the logic within the filesystem extension without making real API calls.
"""

import contextlib
import os
import uuid
from unittest import mock

import pytest
from google.api_core import exceptions as api_exceptions
from google.cloud import storage_control_v2

from gcsfs.extended_gcsfs import BucketType, ExtendedGcsFileSystem
from gcsfs.retry import HttpError
from gcsfs.tests.settings import TEST_HNS_BUCKET

REQUIRED_ENV_VAR = "GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT"

# If the condition is True, only then tests in this file are run.
should_run = os.getenv(REQUIRED_ENV_VAR, "false").lower() in (
    "true",
    "1",
)
pytestmark = pytest.mark.skipif(
    not should_run, reason=f"Skipping tests: {REQUIRED_ENV_VAR} env variable is not set"
)


def get_mock_folder(folder_path):
    """Helper to create a mock folder object from the Storage Control API."""
    mock_folder = mock.Mock()
    mock_folder.name = f"projects/_/buckets/{TEST_HNS_BUCKET}/folders/{folder_path}"
    return mock_folder


class AsyncIter:
    """A helper class to simulate an async iterator from a list."""

    def __init__(self, items):
        self._items = items

    async def __aiter__(self):
        for item in self._items:
            yield item


@pytest.fixture
def gcs_hns_mocks():
    """A factory fixture for mocking bucket functionality for HNS tests."""

    @contextlib.contextmanager
    def _gcs_hns_mocks_factory(bucket_type_val, gcsfs):
        """Creates mocks for a given file content and bucket type."""

        patch_target_lookup_bucket_type = (
            "gcsfs.extended_gcsfs.ExtendedGcsFileSystem._lookup_bucket_type"
        )
        patch_target_sync_lookup_bucket_type = (
            "gcsfs.extended_gcsfs.ExtendedGcsFileSystem._sync_lookup_bucket_type"
        )
        patch_target_super_mv = "gcsfs.core.GCSFileSystem.mv"
        patch_target_super_mkdir = "gcsfs.core.GCSFileSystem._mkdir"
        patch_target_super_rmdir = "gcsfs.core.GCSFileSystem._rmdir"
        patch_target_super_find = "gcsfs.core.GCSFileSystem._find"
        patch_target_super_rm = "gcsfs.core.GCSFileSystem._rm"

        # Mock the async rename_folder method on the storage_control_client
        mock_rename_folder = mock.AsyncMock()
        mock_control_client_instance = mock.AsyncMock()
        mock_control_client_instance.list_folders = mock.AsyncMock()
        mock_control_client_instance.rename_folder = mock_rename_folder

        with (
            mock.patch(
                patch_target_lookup_bucket_type, new_callable=mock.AsyncMock
            ) as mock_async_lookup_bucket_type,
            mock.patch(
                patch_target_sync_lookup_bucket_type
            ) as mock_sync_lookup_bucket_type,
            mock.patch(
                "gcsfs.core.GCSFileSystem._info", new_callable=mock.AsyncMock
            ) as mock_info,
            mock.patch.object(
                gcsfs, "_storage_control_client", mock_control_client_instance
            ),
            mock.patch(patch_target_super_mv, new_callable=mock.Mock) as mock_super_mv,
            mock.patch(
                patch_target_super_mkdir, new_callable=mock.AsyncMock
            ) as mock_super_mkdir,
            mock.patch(
                patch_target_super_rmdir, new_callable=mock.AsyncMock
            ) as mock_super_rmdir,
            mock.patch(
                patch_target_super_find, new_callable=mock.AsyncMock
            ) as mock_super_find,
            mock.patch(
                patch_target_super_rm, new_callable=mock.AsyncMock
            ) as mock_super_rm,
        ):
            mock_async_lookup_bucket_type.return_value = bucket_type_val
            mock_sync_lookup_bucket_type.return_value = bucket_type_val
            mocks = {
                "async_lookup_bucket_type": mock_async_lookup_bucket_type,
                "sync_lookup_bucket_type": mock_sync_lookup_bucket_type,
                "info": mock_info,
                "control_client": mock_control_client_instance,
                "super_mv": mock_super_mv,
                "super_mkdir": mock_super_mkdir,
                "super_rmdir": mock_super_rmdir,
                "super_find": mock_super_find,
                "super_rm": mock_super_rm,
            }
            yield mocks

    return _gcs_hns_mocks_factory


class TestExtendedGcsFileSystemMv:
    """Unit tests for the _mv method in ExtendedGcsFileSystem."""

    def _get_rename_folder_request(self, path1, path2):
        """Constructs a RenameFolderRequest for testing."""
        path1_key = path1.split("/", 1)[1]
        path2_key = path2.split("/", 1)[1]
        return storage_control_v2.RenameFolderRequest(
            name=f"projects/_/buckets/{TEST_HNS_BUCKET}/folders/{path1_key}",
            destination_folder_id=path2_key,
        )

    rename_success_params = [
        pytest.param("old_dir", "new_dir", id="simple_rename_at_root"),
        pytest.param(
            "nested/old_dir",
            "nested/new_dir",
            id="rename_within_nested_dir",
        ),
    ]

    @pytest.mark.parametrize("path1, path2", rename_success_params)
    def test_hns_folder_rename_success(self, gcs_hns, gcs_hns_mocks, path1, path2):
        """Test successful HNS folder rename."""
        gcsfs = gcs_hns
        path1 = f"{TEST_HNS_BUCKET}/{path1}"
        path2 = f"{TEST_HNS_BUCKET}/{path2}"

        # Setup a more complex directory structure
        file_in_root = f"{path1}/file1.txt"
        nested_file = f"{path1}/sub_dir/file2.txt"

        gcsfs.touch(file_in_root)
        gcsfs.touch(nested_file)

        with gcs_hns_mocks(BucketType.HIERARCHICAL, gcsfs) as mocks:
            # Configure mocks
            # 1. First _info call in _mv on path1 should succeed.
            # 2. _info call in exists(path1) after mv should fail.
            # 3. _info call in exists(path2) after mv should succeed.
            # 4. _info call in exists(path2/file1.txt) after mv should succeed.
            # 5. _info call in exists(path2/sub_dir/file2.txt) after mv should succeed.
            mocks["info"].side_effect = [
                {"type": "directory", "name": path1},
                FileNotFoundError(path1),
                {"type": "directory", "name": path2},
                {"type": "file", "name": f"{path2}/file1.txt"},
                {"type": "file", "name": f"{path2}/sub_dir/file2.txt"},
            ]

            gcsfs.mv(path1, path2)

            # Verify that the old path no longer exist
            assert not gcsfs.exists(path1)

            # Verify that the new paths exist
            assert gcsfs.exists(path2)
            assert gcsfs.exists(f"{path2}/file1.txt")
            assert gcsfs.exists(f"{path2}/sub_dir/file2.txt")

            mocks["async_lookup_bucket_type"].assert_called_once_with(TEST_HNS_BUCKET)
            # Verify the sequence of _info calls for mv and exists checks.
            expected_info_calls = [
                mock.call(path1),  # from mv
                mock.call(path1),  # from exists(path1)
                mock.call(path2),  # from exists(path2)
                mock.call(f"{path2}/file1.txt"),  # from exists(path2/file1.txt)
                mock.call(
                    f"{path2}/sub_dir/file2.txt"
                ),  # from exists(path2/sub_dir/file2.txt)
            ]
            mocks["info"].assert_has_awaits(expected_info_calls)

            expected_request = self._get_rename_folder_request(path1, path2)
            mocks["control_client"].rename_folder.assert_called_once_with(
                request=expected_request
            )
            mocks["super_mv"].assert_not_called()

    def test_hns_folder_rename_with_protocol(self, gcs_hns, gcs_hns_mocks):
        """Test successful HNS folder rename when paths include the protocol."""
        gcsfs = gcs_hns
        path1_no_proto = f"{TEST_HNS_BUCKET}/old_dir_proto"
        path2_no_proto = f"{TEST_HNS_BUCKET}/new_dir_proto"
        path1 = f"gs://{path1_no_proto}"
        path2 = f"gs://{path2_no_proto}"

        file_in_root = f"{path1}/file1.txt"
        gcsfs.touch(file_in_root)

        with gcs_hns_mocks(BucketType.HIERARCHICAL, gcsfs) as mocks:
            # The paths passed to info will be stripped of protocol
            mocks["info"].side_effect = [
                {"type": "directory", "name": path1},
                FileNotFoundError(path1),
                {"type": "directory", "name": path2},
            ]

            gcsfs.mv(path1, path2)

            assert not gcsfs.exists(path1)
            assert gcsfs.exists(path2)

            mocks["async_lookup_bucket_type"].assert_called_once_with(TEST_HNS_BUCKET)
            expected_info_calls = [
                mock.call(path1),  # from _mv
                mock.call(path1),  # from exists(path1)
                mock.call(path2),  # from exists(path2)
            ]
            mocks["info"].assert_has_awaits(expected_info_calls)
            expected_request = self._get_rename_folder_request(
                path1_no_proto, path2_no_proto
            )
            mocks["control_client"].rename_folder.assert_called_once_with(
                request=expected_request
            )
            mocks["super_mv"].assert_not_called()

    def test_hns_empty_folder_rename_success(self, gcs_hns, gcs_hns_mocks):
        """Test successful HNS rename of an empty folder."""
        gcsfs = gcs_hns
        path1 = f"{TEST_HNS_BUCKET}/empty_old_dir"
        path2 = f"{TEST_HNS_BUCKET}/empty_new_dir"

        gcsfs.mkdir(path1)

        with gcs_hns_mocks(BucketType.HIERARCHICAL, gcsfs) as mocks:
            # Configure mocks for the sequence of calls
            mocks["info"].side_effect = [
                {"type": "directory", "name": path1},  # _mv check
                FileNotFoundError(path1),  # exists(path1) after move
                {"type": "directory", "name": path2},  # exists(path2) after move
            ]

            gcsfs.mv(path1, path2)

            assert not gcsfs.exists(path1)
            assert gcsfs.exists(path2)

            mocks["async_lookup_bucket_type"].assert_called_once_with(TEST_HNS_BUCKET)
            expected_info_calls = [
                mock.call(path1),  # from _mv
                mock.call(path1),  # from exists(path1)
                mock.call(path2),  # from exists(path2)
            ]
            mocks["info"].assert_has_awaits(expected_info_calls)
            expected_request = self._get_rename_folder_request(path1, path2)
            mocks["control_client"].rename_folder.assert_called_once_with(
                request=expected_request
            )
            mocks["super_mv"].assert_not_called()

    def test_file_rename_fallback_to_super_mv(
        self,
        gcs_hns,
        gcs_hns_mocks,
    ):
        """Test scenarios that should fall back to the parent's mv method."""
        gcsfs = gcs_hns
        path1 = f"{TEST_HNS_BUCKET}/file.txt"
        path2 = f"{TEST_HNS_BUCKET}/new_file.txt"
        with gcs_hns_mocks(BucketType.HIERARCHICAL, gcsfs) as mocks:
            mocks["info"].side_effect = [
                {"type": "file"},
                FileNotFoundError(path1),
                {"type": "file", "name": path2},
            ]

            gcsfs.touch(path1)
            gcsfs.mv(path1, path2)

            assert not gcsfs.exists(path1)
            assert gcsfs.exists(path2)

            mocks["control_client"].rename_folder.assert_not_called()
            mocks["super_mv"].assert_called_once_with(path1, path2)
            expected_info_calls = [
                mock.call(path1),  # from _mv
                mock.call(path1),  # from exists(path1)
                mock.call(path2),  # from exists(path2)
            ]
            mocks["info"].assert_has_awaits(expected_info_calls)

    @pytest.mark.parametrize(
        "bucket_type, info_return, path1, path2, reason",
        [
            (
                BucketType.NON_HIERARCHICAL,
                {"type": "directory"},
                "old_dir_non_hns",
                "new_dir_non_hns",
                "not an HNS bucket",
            ),
            pytest.param(
                BucketType.HIERARCHICAL,
                {"type": "directory"},
                "cross_bucket_dir",
                "another-bucket/d",
                "different bucket",
                marks=pytest.mark.xfail(
                    reason="Cross-bucket move not fully supported in test setup"
                ),
            ),
        ],
    )
    def test_folder_rename_fallback_to_super_mv(
        self,
        gcs_hns,
        gcs_hns_mocks,
        bucket_type,
        info_return,
        path1,
        path2,
        reason,
    ):
        """Test scenarios that should fall back to the parent's mv method."""
        gcsfs = gcs_hns
        path1 = f"{TEST_HNS_BUCKET}/test/{path1}"
        # Handle cross-bucket case where path2 already includes the bucket
        if "/" in path2:
            path2 = path2
        else:
            path2 = (
                f"{TEST_HNS_BUCKET}/{path2}" if path2 != "" else f"{TEST_HNS_BUCKET}/"
            )
        with gcs_hns_mocks(bucket_type, gcsfs) as mocks:
            if bucket_type in [
                BucketType.HIERARCHICAL,
                BucketType.ZONAL_HIERARCHICAL,
            ]:
                mocks["info"].side_effect = [
                    info_return,
                    FileNotFoundError(path1),
                    {"type": "directory", "name": path2},
                    {"type": "file", "name": f"{path2}/file1.txt"},
                ]
            else:
                mocks["info"].side_effect = [
                    FileNotFoundError(path1),
                    {"type": "directory", "name": path2},
                    {"type": "file", "name": f"{path2}/file1.txt"},
                ]

            gcsfs.touch(path1)
            gcsfs.mv(path1, path2)

            assert not gcsfs.exists(path1)
            assert gcsfs.exists(path2)

            mocks["control_client"].rename_folder.assert_not_called()
            mocks["super_mv"].assert_called_once_with(path1, path2)
            expected_info_calls = [
                mock.call(path1),  # from _mv
                mock.call(path1),  # from exists(path1)
                mock.call(path2),  # from exists(path2)
            ]
            if bucket_type not in [
                BucketType.HIERARCHICAL,
                BucketType.ZONAL_HIERARCHICAL,
            ]:
                expected_info_calls.pop(1)  # info is not called from mv for non-HNS

            mocks["info"].assert_has_awaits(expected_info_calls)

    def test_folder_rename_to_root_directory(
        self,
        gcs_hns,
        gcs_hns_mocks,
    ):
        """Test scenarios that should fall back to the parent's mv method."""
        gcsfs = gcs_hns
        dir_name = "root_dir"
        path1 = f"{TEST_HNS_BUCKET}/test/{dir_name}"
        path2 = f"{TEST_HNS_BUCKET}/"
        with gcs_hns_mocks(BucketType.HIERARCHICAL, gcsfs) as mocks:
            mocks["info"].side_effect = [
                {"type": "directory"},
                FileNotFoundError(path1),
                {"type": "directory", "name": path2},
                {"type": "directory", "name": f"{path2}/{dir_name}"},
                {"type": "file", "name": f"{path2}/{dir_name}/file.txt"},
            ]

            gcsfs.touch(f"{path1}/file.txt")
            gcsfs.mv(path1, path2, recursive=True)

            assert not gcsfs.exists(path1)
            assert gcsfs.exists(path2)
            assert gcsfs.exists(f"{path2.rstrip('/')}/{dir_name}")
            assert gcsfs.exists(f"{path2.rstrip('/')}/{dir_name}/file.txt")

            mocks["control_client"].rename_folder.assert_not_called()
            mocks["super_mv"].assert_called_once_with(path1, path2, recursive=True)
            expected_info_calls = [
                mock.call(path1),  # from _mv
                mock.call(path1),  # from exists(path1)
                mock.call(path2),  # from exists(path2)
                mock.call(
                    f"{path2.rstrip('/')}/{dir_name}"
                ),  # from exists(path2/dir_name)
                mock.call(f"{path2.rstrip('/')}/{dir_name}/file.txt"),
            ]
            mocks["info"].assert_has_awaits(expected_info_calls)

    def test_mv_same_path_is_noop(self, gcs_hns, gcs_hns_mocks):
        """Test that mv with the same source and destination path is a no-op."""
        gcsfs = gcs_hns
        path = f"{TEST_HNS_BUCKET}/some_path"

        with gcs_hns_mocks(BucketType.HIERARCHICAL, gcsfs) as mocks:
            gcsfs.mv(path, path)

            mocks["async_lookup_bucket_type"].assert_not_called()
            mocks["info"].assert_not_called()
            mocks["control_client"].rename_folder.assert_not_called()
            mocks["super_mv"].assert_not_called()

    def test_hns_rename_fails_if_parent_dne(self, gcs_hns, gcs_hns_mocks):
        """Test that HNS rename fails if the destination's parent does not exist."""
        gcsfs = gcs_hns
        path1 = f"{TEST_HNS_BUCKET}/dir_to_move"
        path2 = f"{TEST_HNS_BUCKET}/new_parent/new_name"

        with gcs_hns_mocks(BucketType.HIERARCHICAL, gcsfs) as mocks:
            # Mocked environment assertions
            mocks["info"].return_value = {"type": "directory"}
            mocks["control_client"].rename_folder.side_effect = (
                api_exceptions.FailedPrecondition("The parent folder does not exist.")
            )

            gcsfs.touch(f"{path1}/file.txt")

            # The underlying API error includes the status code (400) in its string representation.
            expected_msg = "HNS rename failed: 400 The parent folder does not exist."
            with pytest.raises(OSError, match=expected_msg):
                gcsfs.mv(path1, path2)

            expected_request = self._get_rename_folder_request(path1, path2)
            mocks["control_client"].rename_folder.assert_called_once_with(
                request=expected_request
            )
            mocks["info"].assert_awaited_with(path1)
            mocks["super_mv"].assert_not_called()

    def test_hns_rename_raises_file_not_found(self, gcs_hns, gcs_hns_mocks):
        """Test that NotFound from API raises FileNotFoundError."""
        gcsfs = gcs_hns
        path1 = f"{TEST_HNS_BUCKET}/dne"
        path2 = f"{TEST_HNS_BUCKET}/new_dir"
        with gcs_hns_mocks(BucketType.HIERARCHICAL, gcsfs) as mocks:
            mocks["info"].side_effect = FileNotFoundError(path1)

            with pytest.raises(FileNotFoundError):
                gcsfs.mv(path1, path2)

            mocks["info"].assert_awaited_with(path1)
            mocks["super_mv"].assert_not_called()
            mocks["control_client"].rename_folder.assert_not_called()

    def test_hns_rename_raises_file_not_found_on_race_condition(
        self, gcs_hns, gcs_hns_mocks
    ):
        """Test that api_exceptions.NotFound from rename call raises FileNotFoundError."""
        gcsfs = gcs_hns
        path1 = f"{TEST_HNS_BUCKET}/dir_disappears"
        path2 = f"{TEST_HNS_BUCKET}/new_dir"

        with gcs_hns_mocks(BucketType.HIERARCHICAL, gcsfs) as mocks:
            # Simulate _info finding the directory
            mocks["info"].return_value = {"type": "directory"}
            # Simulate the directory being gone when rename_folder is called
            mocks["control_client"].rename_folder.side_effect = api_exceptions.NotFound(
                "Folder not found during rename"
            )

            with pytest.raises(FileNotFoundError, match="Source .* not found"):
                gcsfs.mv(path1, path2)

            expected_request = self._get_rename_folder_request(path1, path2)
            mocks["control_client"].rename_folder.assert_called_once_with(
                request=expected_request
            )
            mocks["info"].assert_awaited_with(path1)
            mocks["super_mv"].assert_not_called()

    def test_hns_rename_raises_os_error_if_destination_exists(
        self, gcs_hns, gcs_hns_mocks
    ):
        """Test that FailedPrecondition from API raises OSError."""
        gcsfs = gcs_hns
        path1 = f"{TEST_HNS_BUCKET}/dir"
        path2 = f"{TEST_HNS_BUCKET}/existing_dir"

        gcsfs.touch(f"{path1}/file.txt")
        gcsfs.touch(f"{path2}/file.txt")

        with gcs_hns_mocks(BucketType.HIERARCHICAL, gcsfs) as mocks:
            mocks["info"].return_value = {"type": "directory"}
            mocks["control_client"].rename_folder.side_effect = api_exceptions.Conflict(
                "HNS rename failed due to conflict for"
            )

            expected_msg = (
                f"HNS rename failed due to conflict for '{path1}' to '{path2}'"
            )
            with pytest.raises(FileExistsError, match=expected_msg):
                gcsfs.mv(path1, path2)

            expected_request = self._get_rename_folder_request(path1, path2)
            mocks["control_client"].rename_folder.assert_called_once_with(
                request=expected_request
            )
            mocks["info"].assert_awaited_with(path1)
            mocks["super_mv"].assert_not_called()
            mocks["control_client"].rename_folder.assert_called()


class TestExtendedGcsFileSystemMkdir:
    """Tests for the mkdir method in ExtendedGcsFileSystem."""

    def _get_create_folder_request(self, dir_path, recursive=False):
        """Constructs a CreateFolderRequest for testing."""
        bucket, folder_path = dir_path.split("/", 1)
        return storage_control_v2.CreateFolderRequest(
            parent=f"projects/_/buckets/{bucket}",
            folder_id=folder_path.rstrip("/"),
            recursive=recursive,
        )

    def test_hns_mkdir_success(self, gcs_hns, gcs_hns_mocks):
        """Test successful HNS folder creation."""
        gcsfs = gcs_hns
        dir_path = f"{TEST_HNS_BUCKET}/new_mkdir_dir"

        with gcs_hns_mocks(BucketType.HIERARCHICAL, gcsfs) as mocks:
            mocks["info"].side_effect = [
                FileNotFoundError,  # exists check before mkdir
                {"type": "directory", "name": dir_path},  # exists check after
            ]

            assert not gcsfs.exists(dir_path)
            gcsfs.mkdir(dir_path)
            assert gcsfs.exists(dir_path)

            mocks["async_lookup_bucket_type"].assert_called_once_with(TEST_HNS_BUCKET)
            expected_request = self._get_create_folder_request(dir_path)
            mocks["control_client"].create_folder.assert_called_once_with(
                request=expected_request
            )
            mocks["super_mkdir"].assert_not_called()

    def test_hns_mkdir_nested_success_with_create_parents(self, gcs_hns, gcs_hns_mocks):
        """Test successful HNS folder creation for a nested path with create_parents=True."""
        gcsfs = gcs_hns
        parent_dir = f"{TEST_HNS_BUCKET}/nested_parent"
        dir_path = f"{parent_dir}/new_nested_dir"

        with gcs_hns_mocks(BucketType.HIERARCHICAL, gcsfs) as mocks:
            mocks["info"].side_effect = [
                FileNotFoundError,  # exists check before mkdir
                {"type": "directory", "name": parent_dir},  # parent exists check
                {"type": "directory", "name": dir_path},  # exists check after
            ]

            assert not gcsfs.exists(dir_path)
            gcsfs.mkdir(dir_path, create_parents=True)
            assert gcsfs.exists(parent_dir)
            assert gcsfs.exists(dir_path)

            mocks["async_lookup_bucket_type"].assert_called_once_with(TEST_HNS_BUCKET)
            expected_request = self._get_create_folder_request(dir_path, recursive=True)
            mocks["control_client"].create_folder.assert_called_once_with(
                request=expected_request
            )
            mocks["super_mkdir"].assert_not_called()

    def test_hns_mkdir_nested_fails_if_create_parents_false(
        self, gcs_hns, gcs_hns_mocks
    ):
        """Test HNS mkdir fails for nested path if create_parents=False and parent doesn't exist."""
        gcsfs = gcs_hns
        dir_path = f"{TEST_HNS_BUCKET}/non_existent_parent/new_dir"

        with gcs_hns_mocks(BucketType.HIERARCHICAL, gcsfs) as mocks:
            mocks["control_client"].create_folder.side_effect = (
                api_exceptions.FailedPrecondition("failed due to a precondition error")
            )

            with pytest.raises(
                FileNotFoundError, match="failed due to a precondition error"
            ):
                gcsfs.mkdir(dir_path, create_parents=False)

            mocks["async_lookup_bucket_type"].assert_called_once_with(TEST_HNS_BUCKET)
            expected_request = self._get_create_folder_request(dir_path)
            mocks["control_client"].create_folder.assert_called_once_with(
                request=expected_request
            )
            mocks["super_mkdir"].assert_not_called()

    @pytest.mark.skipif(
        os.environ.get("STORAGE_EMULATOR_HOST") == "https://storage.googleapis.com",
        reason="This test is only to check that create_folder is not called for non-HNS buckets.",
    )
    def test_mkdir_non_hns_bucket_falls_back(self, gcs_hns, gcs_hns_mocks):
        """Test that mkdir falls back to parent for non-HNS buckets."""
        gcsfs = gcs_hns
        dir_path = f"{TEST_HNS_BUCKET}/some_dir"

        with gcs_hns_mocks(BucketType.NON_HIERARCHICAL, gcsfs) as mocks:
            gcsfs.mkdir(dir_path)
            mocks["async_lookup_bucket_type"].assert_called_once_with(TEST_HNS_BUCKET)
            mocks["control_client"].create_folder.assert_not_called()
            mocks["super_mkdir"].assert_called_once_with(dir_path, create_parents=False)

    def test_mkdir_in_non_existent_bucket_fails(self, gcs_hns, gcs_hns_mocks):
        """Test that mkdir fails when the target bucket does not exist."""
        gcsfs = gcs_hns
        bucket_name = f"gcsfs-non-existent-bucket-{uuid.uuid4()}"
        dir_path = f"{bucket_name}/some_dir"

        with gcs_hns_mocks(BucketType.UNKNOWN, gcsfs) as mocks:
            # Simulate the parent mkdir raising an error because the bucket doesn't exist
            mocks["super_mkdir"].side_effect = FileNotFoundError(
                f"Bucket {bucket_name} does not exist."
            )

            with pytest.raises(FileNotFoundError, match=f"{bucket_name}"):
                gcsfs.mkdir(dir_path, create_parents=False)

            mocks["async_lookup_bucket_type"].assert_called_once_with(bucket_name)
            mocks["super_mkdir"].assert_called_once_with(dir_path, create_parents=False)
            mocks["control_client"].create_folder.assert_not_called()

    def test_mkdir_in_non_existent_bucket_with_create_parents_succeeds(
        self, gcs_hns, gcs_hns_mocks
    ):
        """Test that mkdir with create_parents=True creates the bucket."""
        gcsfs = gcs_hns
        bucket_name = f"gcsfs-bucket-mkdir-{uuid.uuid4()}"
        dir_path = f"{bucket_name}/some_dir"

        with gcs_hns_mocks(BucketType.UNKNOWN, gcsfs) as mocks:
            # Simulate the bucket not existing initially, then existing after creation.
            mocks["info"].side_effect = [
                FileNotFoundError,  # For the `exists` check on the bucket
                {"type": "directory", "name": bucket_name},  # For `exists` after
            ]

            assert not gcsfs.exists(bucket_name)
            # This should create the bucket `bucket_name` and then do nothing for `some_dir`
            gcsfs.mkdir(dir_path, create_parents=True)
            assert gcsfs.exists(bucket_name)

            mocks["async_lookup_bucket_type"].assert_called_once_with(bucket_name)
            # The call should fall back to the parent's mkdir to create the bucket.
            mocks["super_mkdir"].assert_called_once_with(dir_path, create_parents=True)
            mocks["control_client"].create_folder.assert_not_called()

    def test_mkdir_hns_bucket_with_create_parents_succeeds(
        self, gcs_hns, gcs_hns_mocks
    ):
        """Test mkdir with create_parents and enable_hierarchical_namespace creates an HNS bucket."""
        gcsfs = gcs_hns
        bucket_name = f"gcsfs-hns-bucket-mkdir-{uuid.uuid4()}"
        dir_path = f"{bucket_name}/some_dir"

        with gcs_hns_mocks(BucketType.UNKNOWN, gcsfs) as mocks:
            # Simulate the bucket not existing initially, then existing after creation.
            mocks["info"].side_effect = [
                FileNotFoundError,  # For the exists check on the bucket before mkdir call
                FileNotFoundError,  # For bucket exists check in mkdir method
                {"type": "directory", "name": bucket_name},  # For exists on bucket
                {"type": "directory", "name": dir_path},  # For exists on directory
            ]

            assert not gcsfs.exists(bucket_name)
            # This should create the HNS bucket `bucket_name` and then do nothing for `some_dir`
            gcsfs.mkdir(
                dir_path, create_parents=True, enable_hierarchical_namespace=True
            )
            assert gcsfs.exists(bucket_name)
            assert gcsfs.exists(dir_path)

            # The call should fall back to the parent's mkdir to create the bucket.
            mocks["super_mkdir"].assert_called_once_with(
                bucket_name,
                create_parents=True,
                hierarchicalNamespace={"enabled": True},
                iamConfiguration={"uniformBucketLevelAccess": {"enabled": True}},
                acl=None,
                default_acl=None,
            )
            # After the bucket is created, the HNS path is taken to create the folder.
            expected_request = self._get_create_folder_request(dir_path, recursive=True)
            mocks["control_client"].create_folder.assert_called_once_with(
                request=expected_request
            )
            mocks["async_lookup_bucket_type"].assert_not_called()

    def test_mkdir_create_non_hns_bucket(self, gcs_hns, gcs_hns_mocks):
        """Test creating a new non-HNS bucket by default."""
        gcsfs = gcs_hns
        bucket_path = f"new-non-hns-bucket-{uuid.uuid4()}"

        with gcs_hns_mocks(BucketType.NON_HIERARCHICAL, gcsfs) as mocks:
            mocks["info"].side_effect = [
                FileNotFoundError,  # For the `exists` check before mkdir
                {"type": "directory", "name": bucket_path},  # For `exists` after
            ]

            assert not gcsfs.exists(bucket_path)
            gcsfs.mkdir(bucket_path)
            assert gcsfs.exists(bucket_path)

            mocks["control_client"].create_folder.assert_not_called()
            mocks["super_mkdir"].assert_called_once_with(
                bucket_path, create_parents=False
            )
            mocks["async_lookup_bucket_type"].assert_not_called()

    def test_mkdir_create_bucket_with_parent_params(self, gcs_hns, gcs_hns_mocks):
        """Test creating a bucket passes parent-level parameters like enable_versioning."""
        gcsfs = gcs_hns
        bucket_path = f"new-versioned-bucket-{uuid.uuid4()}"

        with gcs_hns_mocks(BucketType.HIERARCHICAL, gcsfs) as mocks:
            mocks["info"].side_effect = [
                FileNotFoundError,  # For the `exists` check before mkdir
                {"type": "directory", "name": bucket_path},  # For `exists` after
            ]

            assert not gcsfs.exists(bucket_path)
            gcsfs.mkdir(
                bucket_path, enable_versioning=True, enable_object_retention=True
            )
            assert gcsfs.exists(bucket_path)

            mocks["super_mkdir"].assert_called_once_with(
                bucket_path,
                create_parents=False,
                enable_versioning=True,
                enable_object_retention=True,
            )
            mocks["async_lookup_bucket_type"].assert_not_called()
            mocks["control_client"].create_folder.assert_not_called()

    def test_mkdir_enable_hierarchical_namespace(self, gcs_hns, gcs_hns_mocks):
        """Test creating a new HNS-enabled bucket."""
        gcsfs = gcs_hns
        bucket_path = f"new-hns-bucket-{uuid.uuid4()}"

        with gcs_hns_mocks(BucketType.HIERARCHICAL, gcsfs) as mocks:
            mocks["info"].side_effect = [
                FileNotFoundError,  # For the `exists` check before mkdir
                {"type": "directory", "name": bucket_path},  # For `exists` after
            ]

            assert not gcsfs.exists(bucket_path)
            gcsfs.mkdir(bucket_path, enable_hierarchical_namespace=True)
            assert gcsfs.exists(bucket_path)

            mocks["control_client"].create_folder.assert_not_called()
            mocks["super_mkdir"].assert_called_once_with(
                bucket_path,
                create_parents=False,
                hierarchicalNamespace={"enabled": True},
                iamConfiguration={"uniformBucketLevelAccess": {"enabled": True}},
                acl=None,
                default_acl=None,
            )
            mocks["async_lookup_bucket_type"].assert_not_called()

    def test_mkdir_existing_hns_folder_is_noop(self, gcs_hns, gcs_hns_mocks):
        """Test that calling mkdir on an existing HNS folder is a no-op."""
        gcsfs = gcs_hns
        dir_path = f"{TEST_HNS_BUCKET}/existing_dir"
        gcsfs.touch(f"{dir_path}/file.txt")

        with gcs_hns_mocks(BucketType.HIERARCHICAL, gcsfs) as mocks:
            mocks["info"].return_value = {"type": "directory", "name": dir_path}
            mocks["control_client"].create_folder.side_effect = api_exceptions.Conflict(
                "Folder already exists"
            )

            assert gcsfs.exists(dir_path)
            gcsfs.mkdir(dir_path)
            assert gcsfs.exists(dir_path)

            expected_request = self._get_create_folder_request(dir_path)
            mocks["control_client"].create_folder.assert_called_once_with(
                request=expected_request
            )
            mocks["super_mkdir"].assert_not_called()
            mocks["async_lookup_bucket_type"].assert_called_once_with(TEST_HNS_BUCKET)


class TestExtendedGcsFileSystemFind:
    """Tests for the find method in ExtendedGcsFileSystem."""

    def test_hns_find_withdirs(
        self,
        gcs_hns,
        gcs_hns_mocks,
    ):
        """Test find with withdirs=True returns files and directories."""
        base_path = f"{TEST_HNS_BUCKET}/find_test"
        file1_path = f"{base_path}/file1.txt"
        nested_file_path = f"{base_path}/sub/file2.txt"
        empty_dir_path = f"{base_path}/empty"
        # Mock results from GCSFileSystem._find (files only)
        mock_files = {
            file1_path: {"name": file1_path, "type": "file", "size": 10},
            nested_file_path: {"name": nested_file_path, "type": "file", "size": 20},
        }
        # Mock results from storage_control.list_folders
        mock_folders = [
            get_mock_folder("find_test/sub"),
            get_mock_folder("find_test/empty"),
            get_mock_folder("find_test"),
        ]
        with gcs_hns_mocks(BucketType.HIERARCHICAL, gcs_hns) as mocks:
            mocks["super_find"].return_value = mock_files
            mocks["control_client"].list_folders.return_value = AsyncIter(mock_folders)

            result = gcs_hns.find(base_path, withdirs=True)

            assert len(result) == 5
            assert base_path in result
            assert file1_path in result
            assert f"{base_path}/sub" in result
            assert empty_dir_path in result
            assert nested_file_path in result

            # Verify that the parent find was called correctly to fetch files
            mocks["super_find"].assert_called_once()
            call_args, call_kwargs = mocks["super_find"].call_args
            assert call_args[0] == base_path
            assert call_kwargs.get("withdirs") is False
            assert call_kwargs.get("detail") is True

            # Assert that list_folders was called with the correct request
            expected_folder_id = "find_test/"
            expected_parent = f"projects/_/buckets/{TEST_HNS_BUCKET}"
            expected_request = storage_control_v2.ListFoldersRequest(
                parent=expected_parent, prefix=expected_folder_id
            )
            mocks["control_client"].list_folders.assert_called_once_with(
                request=expected_request
            )

    def test_hns_find_withdirs_detail(self, gcs_hns, gcs_hns_mocks):
        """Test find with withdirs=True and detail=True returns a dict."""
        base_path = f"{TEST_HNS_BUCKET}/find_test"
        file1_path = f"{base_path}/file1.txt"
        nested_file_path = f"{base_path}/sub/file2.txt"
        empty_dir_path = f"{base_path}/empty"
        # Mock results from GCSFileSystem._find (files only)
        mock_files = {
            file1_path: {"name": file1_path, "type": "file", "size": 10},
            nested_file_path: {"name": nested_file_path, "type": "file", "size": 20},
        }
        # Mock results from storage_control.list_folders
        mock_folders = [
            get_mock_folder("find_test/sub"),
            get_mock_folder("find_test/empty"),
            get_mock_folder("find_test"),
        ]
        with gcs_hns_mocks(BucketType.HIERARCHICAL, gcs_hns) as mocks:
            mocks["super_find"].return_value = mock_files
            mocks["control_client"].list_folders.return_value = AsyncIter(mock_folders)
            result = gcs_hns.find(base_path, withdirs=True, detail=True)

            assert isinstance(result, dict)
            assert len(result) == 5
            assert file1_path in result
            assert result[file1_path]["type"] == "file"
            assert empty_dir_path in result
            assert result[empty_dir_path]["type"] == "directory"

            # Verify that the parent find was called correctly to fetch files
            mocks["super_find"].assert_called_once()
            call_args, call_kwargs = mocks["super_find"].call_args
            assert call_args[0] == base_path
            assert call_kwargs.get("withdirs") is False
            assert call_kwargs.get("detail") is True

            # Assert that list_folders was called with the correct request
            expected_folder_id = "find_test/"
            expected_parent = f"projects/_/buckets/{TEST_HNS_BUCKET}"
            expected_request = storage_control_v2.ListFoldersRequest(
                parent=expected_parent, prefix=expected_folder_id
            )
            mocks["control_client"].list_folders.assert_called_once_with(
                request=expected_request
            )

    def test_hns_find_withdirs_maxdepth(self, gcs_hns, gcs_hns_mocks):
        """Test find with withdirs=True respects maxdepth."""
        base_path = f"{TEST_HNS_BUCKET}/find_test"
        file1_path = f"{base_path}/file1.txt"
        nested_file_path = f"{base_path}/sub/file2.txt"
        empty_dir_path = f"{base_path}/empty"
        # Mock results from GCSFileSystem._find (files only)
        mock_files = {
            file1_path: {"name": file1_path, "type": "file", "size": 10},
            nested_file_path: {"name": nested_file_path, "type": "file", "size": 20},
        }
        # Mock results from storage_control.list_folders
        mock_folders = [
            get_mock_folder("find_test/sub"),
            get_mock_folder("find_test/empty"),
            get_mock_folder("find_test"),
        ]
        with gcs_hns_mocks(BucketType.HIERARCHICAL, gcs_hns) as mocks:
            mocks["super_find"].return_value = mock_files
            mocks["control_client"].list_folders.return_value = AsyncIter(mock_folders)
            result = gcs_hns.find(base_path, withdirs=True, maxdepth=1)

            assert len(result) == 4
            assert base_path in result
            assert file1_path in result
            assert f"{base_path}/sub" in result
            assert empty_dir_path in result
            assert nested_file_path not in result

            # Verify that the parent find was called correctly to fetch files
            mocks["super_find"].assert_called_once()
            _, call_kwargs = mocks["super_find"].call_args
            assert call_kwargs.get("withdirs") is False
            assert call_kwargs.get("detail") is True
            assert call_kwargs.get("maxdepth") == 1

            # Assert that list_folders was called with the correct request
            expected_folder_id = "find_test/"
            expected_parent = f"projects/_/buckets/{TEST_HNS_BUCKET}"
            expected_request = storage_control_v2.ListFoldersRequest(
                parent=expected_parent, prefix=expected_folder_id
            )
            mocks["control_client"].list_folders.assert_called_once_with(
                request=expected_request
            )

    def test_hns_find_withdirs_versions(self, gcs_hns, gcs_hns_mocks):
        """Test find with withdirs=True and versions=True."""
        base_path = f"{TEST_HNS_BUCKET}/find_test"
        file1_path = f"{base_path}/file1.txt"
        nested_file_path = f"{base_path}/sub/file2.txt"
        # Mock results from GCSFileSystem._find (files only)
        mock_files = {
            file1_path: {"name": file1_path, "type": "file", "size": 10},
            nested_file_path: {"name": nested_file_path, "type": "file", "size": 20},
            f"{file1_path}#v2": {
                "name": file1_path,
                "type": "file",
                "generation": "v2",
            },
            f"{file1_path}#v1": {
                "name": file1_path,
                "type": "file",
                "generation": "v1",
            },
        }
        # Mock results from storage_control.list_folders
        mock_folders = [
            get_mock_folder("find_test/sub"),
            get_mock_folder("find_test/empty"),
            get_mock_folder("find_test"),
        ]

        with gcs_hns_mocks(BucketType.HIERARCHICAL, gcs_hns) as mocks:
            mocks["super_find"].return_value = mock_files
            mocks["control_client"].list_folders.return_value = AsyncIter(mock_folders)
            result = gcs_hns.find(base_path, withdirs=True, versions=True)

            assert len(result) == 7
            assert base_path in result
            assert file1_path in result  # The unversioned path
            assert f"{file1_path}#v1" in result
            assert f"{file1_path}#v2" in result
            assert nested_file_path in result

            # Verify super_find call
            mocks["super_find"].assert_called_once()
            _, call_kwargs = mocks["super_find"].call_args
            assert call_kwargs.get("versions") is True

            # Assert that list_folders was called with the correct request
            expected_folder_id = "find_test/"
            expected_parent = f"projects/_/buckets/{TEST_HNS_BUCKET}"
            expected_request = storage_control_v2.ListFoldersRequest(
                parent=expected_parent, prefix=expected_folder_id
            )
            mocks["control_client"].list_folders.assert_called_once_with(
                request=expected_request
            )

    def test_find_non_hns_falls_back(self, gcs_hns, gcs_hns_mocks):
        """Test that find falls back to parent implementation for non-HNS"""
        base_path = f"{TEST_HNS_BUCKET}/find_test"
        with gcs_hns_mocks(BucketType.NON_HIERARCHICAL, gcs_hns) as mocks:
            gcs_hns.find(base_path, withdirs=True)
            mocks["super_find"].assert_called_with(
                base_path,
                withdirs=True,
                detail=False,
                prefix="",
                versions=False,
                maxdepth=None,
            )
            mocks["control_client"].list_folders.assert_not_called()

    def test_find_on_non_existent_path_returns_empty(self, gcs_hns, gcs_hns_mocks):
        """Unit test for find on a non-existent path returning an empty list."""
        gcs = gcs_hns
        bucket = "test-bucket"
        path = f"{bucket}/does/not/exist"

        with gcs_hns_mocks(BucketType.HIERARCHICAL, gcs) as mocks:
            # Mock the underlying find/list calls to return empty results
            mocks["super_find"].return_value = {}
            mocks["control_client"].list_folders.return_value = AsyncIter([])

            result = gcs.find(path, withdirs=True)

            assert result == []
            mocks["super_find"].assert_called_once()
            mocks["control_client"].list_folders.assert_called_once()

    def test_find_on_non_existent_bucket_raises_error(self, gcs_hns, gcs_hns_mocks):
        """Unit test for find on a non-existent bucket raising FileNotFoundError."""
        gcs = gcs_hns
        bucket = "non-existent-bucket"
        path = f"{bucket}/some/path"

        with gcs_hns_mocks(BucketType.UNKNOWN, gcs) as mocks:
            # Mock the super `_find` to simulate the GCS API error.
            mocks["super_find"].side_effect = FileNotFoundError(bucket)

            with pytest.raises(FileNotFoundError):
                gcs.find(path)

            mocks["super_find"].assert_called_once()
            mocks["control_client"].list_folders.assert_not_called()

    def test_find_list_folders_api_fails(self, gcs_hns, gcs_hns_mocks):
        """Test that find propagates exceptions from the list_folders API call."""
        gcs = gcs_hns
        path = f"{TEST_HNS_BUCKET}/some/path"
        error_message = "API Internal Error"

        with gcs_hns_mocks(BucketType.HIERARCHICAL, gcs) as mocks:
            # Simulate the list_folders call failing
            mocks["control_client"].list_folders.side_effect = (
                api_exceptions.InternalServerError(error_message)
            )
            # super_find should still complete successfully
            mocks["super_find"].return_value = {}

            with pytest.raises(api_exceptions.InternalServerError, match=error_message):
                gcs.find(path, withdirs=True)


# This test class validates that info API retrieves folder data for HNS buckets.
class TestExtendedGcsFileSystemInternal:
    """Unit tests for internal methods and overrides in ExtendedGcsFileSystem."""

    @pytest.mark.asyncio
    async def test_is_bucket_hns_enabled_true_hierarchical(self):
        """Verifies HNS is enabled when bucket type is HIERARCHICAL."""
        fs = ExtendedGcsFileSystem(token="anon")
        with mock.patch.object(
            fs, "_lookup_bucket_type", return_value=BucketType.HIERARCHICAL
        ):
            assert await fs._is_bucket_hns_enabled("my-bucket") is True

    @pytest.mark.asyncio
    async def test_is_bucket_hns_enabled_true_zonal_hierarchical(self):
        """Verifies HNS is enabled when bucket type is ZONAL_HIERARCHICAL."""
        fs = ExtendedGcsFileSystem(token="anon")
        with mock.patch.object(
            fs, "_lookup_bucket_type", return_value=BucketType.ZONAL_HIERARCHICAL
        ):
            assert await fs._is_bucket_hns_enabled("my-bucket") is True

    @pytest.mark.asyncio
    async def test_is_bucket_hns_enabled_false(self):
        """Verifies HNS is disabled for STANDARD/non-hierarchical bucket types."""
        fs = ExtendedGcsFileSystem(token="anon")
        # Mocking a non-hierarchical return value (e.g., standard bucket)
        with mock.patch.object(fs, "_lookup_bucket_type", return_value="STANDARD"):
            assert await fs._is_bucket_hns_enabled("my-bucket") is False

    @pytest.mark.asyncio
    async def test_is_bucket_hns_enabled_exception_handling(self):
        """
        Verifies that if _lookup_bucket_type fails, we log a warning and return False
        (fail-open to non-HNS behavior).
        """
        fs = ExtendedGcsFileSystem(token="anon")

        with (
            mock.patch.object(
                fs, "_lookup_bucket_type", side_effect=Exception("API Error")
            ),
            mock.patch("gcsfs.extended_gcsfs.logger") as mock_logger,
        ):
            assert await fs._is_bucket_hns_enabled("error-bucket") is False

            # Verify warning was logged
            mock_logger.warning.assert_called_once()
            assert (
                "Could not determine if bucket" in mock_logger.warning.call_args[0][0]
            )

    @pytest.mark.asyncio
    async def test_get_directory_info_hns_success(self):
        """
        Verifies _get_directory_info uses storage_control_client.get_folder when HNS is enabled.
        """
        fs = ExtendedGcsFileSystem(token="anon")
        fs._storage_control_client = mock.AsyncMock()

        path = "bucket/folder"
        bucket = "bucket"
        key = "folder"

        # Mock the Storage Control API response
        mock_response = mock.Mock()
        mock_response.create_time = "2024-01-01T12:00:00Z"
        mock_response.update_time = "2024-01-02T12:00:00Z"
        mock_response.metageneration = "10"

        fs._storage_control_client.get_folder.return_value = mock_response

        # Force HNS enabled path
        with (
            mock.patch.object(fs, "_is_bucket_hns_enabled", return_value=True),
            mock.patch(
                "gcsfs.core.GCSFileSystem._get_directory_info"
            ) as mock_super_method,
        ):
            info = await fs._get_directory_info(path, bucket, key, generation=None)

            # Verify the returned structure matches expected directory metadata
            assert info["bucket"] == bucket
            assert info["name"] == path
            assert info["type"] == "directory"
            assert info["storageClass"] == "DIRECTORY"
            assert info["ctime"] == mock_response.create_time

            # Verify correct API call
            fs._storage_control_client.get_folder.assert_called_once()
            call_kwargs = fs._storage_control_client.get_folder.call_args[1]
            # Verify resource name format in the request
            assert (
                "projects/_/buckets/bucket/folders/folder"
                == call_kwargs["request"].name
            )
            mock_super_method.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_directory_info_hns_not_found(self):
        """
        Verifies _get_directory_info raises FileNotFoundError if the folder is missing in HNS bucket.
        """
        fs = ExtendedGcsFileSystem(token="anon")
        fs._storage_control_client = mock.AsyncMock()

        # Mock NotFound exception fromExtendedGCSFileSystem the API
        fs._storage_control_client.get_folder.side_effect = api_exceptions.NotFound(
            "Folder not found"
        )

        with mock.patch.object(fs, "_is_bucket_hns_enabled", return_value=True):
            with pytest.raises(FileNotFoundError):
                await fs._get_directory_info(
                    "bucket/missing", "bucket", "missing", None
                )

    @pytest.mark.asyncio
    async def test_get_directory_info_hns_generic_error(self):
        """
        Verifies that unexpected exceptions during HNS lookup are logged and re-raised.
        """
        fs = ExtendedGcsFileSystem(token="anon")
        fs._storage_control_client = mock.AsyncMock()

        test_exception = Exception("Unexpected API Failure")
        fs._storage_control_client.get_folder.side_effect = test_exception

        with (
            mock.patch.object(fs, "_is_bucket_hns_enabled", return_value=True),
            mock.patch("gcsfs.extended_gcsfs.logger") as mock_logger,
        ):
            with pytest.raises(Exception) as exc_info:
                await fs._get_directory_info("bucket/err", "bucket", "err", None)

            assert exc_info.value is test_exception
            mock_logger.error.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_directory_info_fallback_non_hns(self):
        """
        Verifies that we fall back to the superclass (standard GCS) implementation
        if the bucket is NOT HNS-enabled.
        """
        fs = ExtendedGcsFileSystem(token="anon")
        fs._storage_control_client = mock.AsyncMock()

        # We patch the superclass method on the class itself to verify delegation
        with (
            mock.patch.object(fs, "_is_bucket_hns_enabled", return_value=False),
            mock.patch(
                "gcsfs.core.GCSFileSystem._get_directory_info"
            ) as mock_super_method,
        ):
            expected_result = {"type": "directory", "mocked": True}
            mock_super_method.return_value = expected_result

            result = await fs._get_directory_info("bucket/std", "bucket", "std", None)

            assert result == expected_result
            mock_super_method.assert_called_once()
            fs._storage_control_client.get_folder.assert_not_called()


# This test class validates that list API retrieves all folders for HNS buckets.
class TestExtendedGcsFileSystemLs:
    """Unit tests for _do_list_objects in ExtendedGcsFileSystem."""

    @pytest.mark.asyncio
    async def test_ls_hns_enabled_delimiter(self):
        # Arrange: Mock the HNS check to return True
        with (
            mock.patch.object(
                ExtendedGcsFileSystem, "_is_bucket_hns_enabled", return_value=True
            ),
            mock.patch(
                "gcsfs.core.GCSFileSystem._do_list_objects", new_callable=mock.AsyncMock
            ) as mock_super_ls,
        ):
            fs = ExtendedGcsFileSystem(token="anon")

            # Act: Call _do_list_objects with delimiter="/"
            await fs._do_list_objects("gs://my-bucket/path", delimiter="/")

            # Assert: Verify includeFoldersAsPrefixes="true" is passed to the super method
            mock_super_ls.assert_called_once()
            _, kwargs = mock_super_ls.call_args
            assert kwargs.get("includeFoldersAsPrefixes") == "true"
            assert kwargs.get("delimiter") == "/"

    @pytest.mark.asyncio
    async def test_ls_hns_disabled(self):
        # Arrange: Mock the HNS check to return False
        with (
            mock.patch.object(
                ExtendedGcsFileSystem, "_is_bucket_hns_enabled", return_value=False
            ),
            mock.patch(
                "gcsfs.core.GCSFileSystem._do_list_objects", new_callable=mock.AsyncMock
            ) as mock_super_ls,
        ):
            fs = ExtendedGcsFileSystem(token="anon")

            # Act: Call _do_list_objects
            await fs._do_list_objects("gs://my-bucket/path", delimiter="/")

            # Assert: Verify includeFoldersAsPrefixes is NOT present
            mock_super_ls.assert_called_once()
            _, kwargs = mock_super_ls.call_args
            assert "includeFoldersAsPrefixes" not in kwargs

    @pytest.mark.asyncio
    async def test_ls_hns_enabled_non_slash_delimiter(self):
        # Arrange: HNS is enabled, but we use a different delimiter
        with (
            mock.patch.object(
                ExtendedGcsFileSystem, "_is_bucket_hns_enabled", return_value=True
            ),
            mock.patch(
                "gcsfs.core.GCSFileSystem._do_list_objects", new_callable=mock.AsyncMock
            ) as mock_super_ls,
        ):
            fs = ExtendedGcsFileSystem(token="anon")

            # Act: Call with an empty delimiter (or any non-slash)
            await fs._do_list_objects("gs://my-bucket/path", delimiter="")

            # Assert: Verify includeFoldersAsPrefixes is NOT present
            mock_super_ls.assert_called_once()
            _, kwargs = mock_super_ls.call_args
            assert "includeFoldersAsPrefixes" not in kwargs


# This test class validates that list API retrieves all folders for HNS buckets.
class TestExtendedGcsFileSystemLs:
    """Unit tests for _do_list_objects in ExtendedGcsFileSystem."""

    @pytest.mark.asyncio
    async def test_ls_hns_enabled_delimiter(self):
        # Arrange: Mock the HNS check to return True
        with (
            mock.patch.object(
                ExtendedGcsFileSystem, "_is_bucket_hns_enabled", return_value=True
            ),
            mock.patch(
                "gcsfs.core.GCSFileSystem._do_list_objects", new_callable=mock.AsyncMock
            ) as mock_super_ls,
        ):
            fs = ExtendedGcsFileSystem(token="anon")

            # Act: Call _do_list_objects with delimiter="/"
            await fs._do_list_objects("gs://my-bucket/path", delimiter="/")

            # Assert: Verify includeFoldersAsPrefixes="true" is passed to the super method
            mock_super_ls.assert_called_once()
            _, kwargs = mock_super_ls.call_args
            assert kwargs.get("includeFoldersAsPrefixes") == "true"
            assert kwargs.get("delimiter") == "/"

    @pytest.mark.asyncio
    async def test_ls_hns_disabled(self):
        # Arrange: Mock the HNS check to return False
        with (
            mock.patch.object(
                ExtendedGcsFileSystem, "_is_bucket_hns_enabled", return_value=False
            ),
            mock.patch(
                "gcsfs.core.GCSFileSystem._do_list_objects", new_callable=mock.AsyncMock
            ) as mock_super_ls,
        ):
            fs = ExtendedGcsFileSystem(token="anon")

            # Act: Call _do_list_objects
            await fs._do_list_objects("gs://my-bucket/path", delimiter="/")

            # Assert: Verify includeFoldersAsPrefixes is NOT present
            mock_super_ls.assert_called_once()
            _, kwargs = mock_super_ls.call_args
            assert "includeFoldersAsPrefixes" not in kwargs

    @pytest.mark.asyncio
    async def test_ls_hns_enabled_non_slash_delimiter(self):
        # Arrange: HNS is enabled, but we use a different delimiter
        with (
            mock.patch.object(
                ExtendedGcsFileSystem, "_is_bucket_hns_enabled", return_value=True
            ),
            mock.patch(
                "gcsfs.core.GCSFileSystem._do_list_objects", new_callable=mock.AsyncMock
            ) as mock_super_ls,
        ):
            fs = ExtendedGcsFileSystem(token="anon")

            # Act: Call with an empty delimiter (or any non-slash)
            await fs._do_list_objects("gs://my-bucket/path", delimiter="")

            # Assert: Verify includeFoldersAsPrefixes is NOT present
            mock_super_ls.assert_called_once()
            _, kwargs = mock_super_ls.call_args
            assert "includeFoldersAsPrefixes" not in kwargs


class TestExtendedGcsFileSystemRmdir:
    """Unit tests for the rmdir method in ExtendedGcsFileSystem."""

    def _get_delete_folder_request(self, dir_path):
        """Constructs a DeleteFolderRequest for testing."""
        bucket, folder_path = dir_path.split("/", 1)
        expected_folder_name = f"projects/_/buckets/{bucket}/folders/{folder_path}"
        return storage_control_v2.DeleteFolderRequest(name=expected_folder_name)

    def test_hns_rmdir_success(self, gcs_hns, gcs_hns_mocks):
        """Test successful HNS empty directory deletion."""
        gcsfs = gcs_hns
        dir_name = "empty_dir"
        dir_path = f"{TEST_HNS_BUCKET}/{dir_name}"

        gcsfs.mkdir(f"{dir_path}/placeholder", create_parents=True)

        with gcs_hns_mocks(BucketType.HIERARCHICAL, gcsfs) as mocks:
            # Configure mocks
            # 1. exists(dir_path) before rmdir should succeed.
            # 2. exists(dir_path) after rmdir should fail.
            mocks["info"].side_effect = [
                {"type": "directory", "name": dir_path},
                FileNotFoundError(dir_path),
            ]
            assert gcsfs.exists(dir_path)

            gcsfs.rmdir(dir_path)

            assert not gcsfs.exists(dir_path)
            mocks["async_lookup_bucket_type"].assert_called_once_with(TEST_HNS_BUCKET)
            expected_request = self._get_delete_folder_request(dir_path)
            mocks["control_client"].delete_folder.assert_called_once_with(
                request=expected_request
            )
            mocks["super_rmdir"].assert_not_called()

    @pytest.mark.skipif(
        os.environ.get("STORAGE_EMULATOR_HOST") == "https://storage.googleapis.com",
        reason="This test is only to check that delete_folder is not called in case of non-HNS buckets."
        "In real GCS on non-HNS bucket there would be no empty directories to delete.",
    )
    def test_rmdir_non_hns_bucket_falls_back(self, gcs_hns, gcs_hns_mocks):
        """Test that rmdir falls back to parent for non-HNS buckets."""
        gcsfs = gcs_hns
        dir_path = f"{TEST_HNS_BUCKET}/some_dir"

        with gcs_hns_mocks(BucketType.NON_HIERARCHICAL, gcsfs) as mocks:
            gcsfs.rmdir(dir_path)

            mocks["async_lookup_bucket_type"].assert_called_once_with(TEST_HNS_BUCKET)
            mocks["control_client"].delete_folder.assert_not_called()
            mocks["super_rmdir"].assert_called_once_with(dir_path)

    def test_hns_rmdir_non_empty_raises_os_error(self, gcs_hns, gcs_hns_mocks):
        """Test that HNS rmdir on a non-empty directory raises OSError."""
        gcsfs = gcs_hns
        dir_path = f"{TEST_HNS_BUCKET}/non_empty_dir"
        gcsfs.touch(f"{dir_path}/file.txt")

        with gcs_hns_mocks(BucketType.HIERARCHICAL, gcsfs) as mocks:
            mocks["control_client"].delete_folder.side_effect = (
                api_exceptions.FailedPrecondition("")
            )

            with pytest.raises(OSError, match="Pre condition failed for rmdir"):
                gcsfs.rmdir(dir_path)

            mocks["async_lookup_bucket_type"].assert_called_once_with(TEST_HNS_BUCKET)
            expected_request = self._get_delete_folder_request(dir_path)
            mocks["control_client"].delete_folder.assert_called_once_with(
                request=expected_request
            )
            mocks["super_rmdir"].assert_not_called()

    def test_hns_rmdir_dne_raises_not_found(self, gcs_hns, gcs_hns_mocks):
        """Test that HNS rmdir on a non-existent directory raises FileNotFoundError."""
        gcsfs = gcs_hns
        dir_name = "dne_dir"
        dir_path = f"{TEST_HNS_BUCKET}/{dir_name}"

        with gcs_hns_mocks(BucketType.HIERARCHICAL, gcsfs) as mocks:
            mocks["control_client"].delete_folder.side_effect = api_exceptions.NotFound(
                ""
            )

            with pytest.raises(FileNotFoundError, match="rmdir failed for path"):
                gcsfs.rmdir(dir_path)

            mocks["async_lookup_bucket_type"].assert_called_once_with(TEST_HNS_BUCKET)
            expected_request = self._get_delete_folder_request(dir_path)
            mocks["control_client"].delete_folder.assert_called_once_with(
                request=expected_request
            )
            mocks["super_rmdir"].assert_not_called()

    def test_rmdir_on_file_raises_file_not_found(self, gcs_hns, gcs_hns_mocks):
        """
        Test that HNS rmdir on a file path raises FileNotFoundError.
        The API returns NotFound in this case.
        """
        gcsfs = gcs_hns
        file_name = "a_file.txt"
        file_path = f"{TEST_HNS_BUCKET}/{file_name}"
        gcsfs.touch(file_path)

        with gcs_hns_mocks(BucketType.HIERARCHICAL, gcsfs) as mocks:
            mocks["control_client"].delete_folder.side_effect = api_exceptions.NotFound(
                ""
            )

            with pytest.raises(FileNotFoundError, match="rmdir failed for path"):
                gcsfs.rmdir(file_path)

            mocks["async_lookup_bucket_type"].assert_called_once_with(TEST_HNS_BUCKET)
            expected_request = self._get_delete_folder_request(file_path)
            mocks["control_client"].delete_folder.assert_called_once_with(
                request=expected_request
            )
            mocks["super_rmdir"].assert_not_called()

    def test_hns_rmdir_with_empty_subfolder_raises_os_error(
        self, gcs_hns, gcs_hns_mocks
    ):
        """Test that HNS rmdir on a directory with an empty subfolder raises OSError."""
        gcsfs = gcs_hns
        parent_dir = f"{TEST_HNS_BUCKET}/parent_dir"
        sub_dir = f"{parent_dir}/sub_dir"

        # Create an empty sub-directory
        gcsfs.touch(f"{sub_dir}/placeholder")
        gcsfs.rm(f"{sub_dir}/placeholder")

        with gcs_hns_mocks(BucketType.HIERARCHICAL, gcsfs) as mocks:
            mocks["control_client"].delete_folder.side_effect = (
                api_exceptions.FailedPrecondition("Directory not empty")
            )

            with pytest.raises(OSError, match="Pre condition failed for rmdir"):
                gcsfs.rmdir(parent_dir)

            mocks["async_lookup_bucket_type"].assert_called_once_with(TEST_HNS_BUCKET)
            expected_request = self._get_delete_folder_request(parent_dir)
            mocks["control_client"].delete_folder.assert_called_once_with(
                request=expected_request
            )
            mocks["super_rmdir"].assert_not_called()

    def test_hns_rmdir_nested_directories_from_leaf(self, gcs_hns, gcs_hns_mocks):
        """Test deleting nested directories starting from the leaf."""
        gcsfs = gcs_hns
        parent_dir = f"{TEST_HNS_BUCKET}/parent"
        child_dir = f"{parent_dir}/child"
        grandchild_dir = f"{child_dir}/grandchild"

        # Create nested empty directories
        gcsfs.touch(f"{grandchild_dir}/placeholder")
        gcsfs.rm(f"{grandchild_dir}/placeholder")

        with gcs_hns_mocks(BucketType.HIERARCHICAL, gcsfs) as mocks:
            mocks["info"].side_effect = [
                FileNotFoundError(grandchild_dir),
                FileNotFoundError(child_dir),
                FileNotFoundError(parent_dir),
            ]
            # Delete leaf first
            gcsfs.rmdir(grandchild_dir)
            assert not gcsfs.exists(grandchild_dir)

            # Delete child
            gcsfs.rmdir(child_dir)
            assert not gcsfs.exists(child_dir)

            # Delete parent
            gcsfs.rmdir(parent_dir)
            assert not gcsfs.exists(parent_dir)

            assert mocks["control_client"].delete_folder.call_count == 3
            mocks["super_rmdir"].assert_not_called()

    def test_rmdir_on_non_empty_hns_bucket_raises_http_error(
        self, gcs_hns, gcs_hns_mocks
    ):
        """Test that rmdir on a non-empty HNS bucket raises HttpError."""
        gcsfs = gcs_hns
        bucket_path = f"{TEST_HNS_BUCKET}"
        gcsfs.touch(f"{bucket_path}/file.txt")

        with gcs_hns_mocks(BucketType.HIERARCHICAL, gcsfs) as mocks:
            mocks["super_rmdir"].side_effect = HttpError(
                {"code": 409, "message": "The bucket you tried to delete is not empty"}
            )

            with pytest.raises(
                HttpError, match="The bucket you tried to delete is not empty"
            ):
                gcsfs.rmdir(bucket_path)

            mocks["async_lookup_bucket_type"].assert_not_called()
            mocks["control_client"].delete_folder.assert_not_called()
            mocks["super_rmdir"].assert_called_once_with(bucket_path)

    def test_hns_rmdir_cache_invalidation(self, gcs_hns, gcs_hns_mocks):
        """Test that HNS rmdir correctly invalidates and updates the cache."""
        gcsfs = gcs_hns
        base_dir = f"{TEST_HNS_BUCKET}/rmdir_cache_test"
        dir_to_delete = f"{base_dir}/dir_to_delete"
        sibling_dir = f"{base_dir}/sibling_dir"

        with gcs_hns_mocks(BucketType.HIERARCHICAL, gcsfs) as mocks:
            # --- Setup ---
            gcsfs.touch(f"{dir_to_delete}/file.txt")
            gcsfs.touch(f"{sibling_dir}/sibling_file.txt")
            # Configure mocks for the find() call to populate the cache.
            # 1. Mock the return value for the parent's _find() method, which is called
            #    to get all files. It should return a dictionary of file details.
            mocks["super_find"].return_value = {
                f"{dir_to_delete}/file.txt": {
                    "name": f"{dir_to_delete}/file.txt",
                    "type": "file",
                },
                f"{sibling_dir}/sibling_file.txt": {
                    "name": f"{sibling_dir}/sibling_file.txt",
                    "type": "file",
                },
            }

            # 2. Mock the return value for list_folders, which is called to get all
            #    explicit directory objects.
            mock_folders = [
                get_mock_folder("rmdir_cache_test/dir_to_delete"),
                get_mock_folder("rmdir_cache_test/sibling_dir"),
            ]
            mocks["control_client"].list_folders.return_value = AsyncIter(mock_folders)
            # --- Populate Cache ---
            # Use find() to deeply populate the cache for the entire base directory
            gcsfs.find(base_dir, withdirs=True)

            # --- Pre-Delete Assertions ---
            assert base_dir in gcsfs.dircache
            assert dir_to_delete in gcsfs.dircache
            assert sibling_dir in gcsfs.dircache

            # --- Empty the directory and perform rmdir ---
            # We must empty the directory on the backend for rmdir to succeed.
            # To avoid the broad cache invalidation from gcsfs.rm(), we use a
            # direct API call to delete the file. This keeps the parent caches
            # intact for the test assertions.
            file_to_delete = f"{dir_to_delete}/file.txt"
            bucket, key, _ = gcsfs.split_path(file_to_delete)
            gcsfs.call("DELETE", "b/{}/o/{}", bucket, key)

            gcsfs.rmdir(dir_to_delete)

            # --- Post-Delete Cache Assertions ---
            # 1. The deleted directory should be removed from the cache.
            assert dir_to_delete not in gcsfs.dircache

            # 2. The parent directory should still be in the cache.
            assert base_dir in gcsfs.dircache
            parent_listing = gcsfs.dircache[base_dir]

            # 3. The parent's listing should be updated, not cleared.
            # It should no longer contain the deleted directory.
            assert not any(e["name"] == dir_to_delete for e in parent_listing)

            # 4. The sibling directory should remain in the parent's listing.
            assert any(e["name"] == sibling_dir for e in parent_listing)

    def test_rmdir_on_non_empty_non_hns_bucket_raises_http_error(
        self, gcs_hns, gcs_hns_mocks
    ):
        """Test that rmdir on a non-empty non-HNS bucket raises HttpError."""
        gcsfs = gcs_hns
        bucket_path = f"{TEST_HNS_BUCKET}"
        gcsfs.touch(f"{bucket_path}/file.txt")

        with gcs_hns_mocks(BucketType.NON_HIERARCHICAL, gcsfs) as mocks:
            mocks["super_rmdir"].side_effect = HttpError(
                {"code": 409, "message": "The bucket you tried to delete is not empty"}
            )

            with pytest.raises(
                HttpError, match="The bucket you tried to delete is not empty"
            ):
                gcsfs.rmdir(bucket_path)

            mocks["async_lookup_bucket_type"].assert_not_called()
            mocks["control_client"].delete_folder.assert_not_called()
            mocks["super_rmdir"].assert_called_once_with(bucket_path)

    def test_rmdir_on_hns_bucket_falls_back(self, gcs_hns, gcs_hns_mocks):
        """Test that rmdir on a bucket falls back to parent method."""
        gcsfs = gcs_hns
        bucket_path = f"{TEST_HNS_BUCKET}"

        with gcs_hns_mocks(BucketType.HIERARCHICAL, gcsfs) as mocks:
            # exists(dir_path) after rmdir should fail.
            mocks["info"].side_effect = [
                FileNotFoundError(bucket_path),
            ]

            gcsfs.rmdir(bucket_path)

            assert not gcsfs.exists(bucket_path)
            mocks["async_lookup_bucket_type"].assert_not_called()
            mocks["control_client"].delete_folder.assert_not_called()
            mocks["super_rmdir"].assert_called_once_with(bucket_path)

    def test_rmdir_on_non_hns_bucket_falls_back(self, gcs_hns, gcs_hns_mocks):
        """Test that rmdir on a bucket falls back to parent method."""
        gcsfs = gcs_hns
        bucket_path = f"{TEST_HNS_BUCKET}"

        with gcs_hns_mocks(BucketType.NON_HIERARCHICAL, gcsfs) as mocks:
            # exists(dir_path) after rmdir should fail.
            mocks["info"].side_effect = [
                FileNotFoundError(bucket_path),
            ]

            gcsfs.rmdir(bucket_path)

            assert not gcsfs.exists(bucket_path)
            mocks["async_lookup_bucket_type"].assert_not_called()
            mocks["control_client"].delete_folder.assert_not_called()
            mocks["super_rmdir"].assert_called_once_with(bucket_path)

    def test_rmdir_on_folder_with_placeholder_object(self, gcs_hns, gcs_hns_mocks):
        """
        Tests that rmdir successfully deletes a folder that contains its own
        zero-byte placeholder object, a scenario common in HNS buckets when
        folders are created via tools that simulate directories with placeholder files.
        """
        gcsfs = gcs_hns
        folder_path = f"{TEST_HNS_BUCKET}/test-folder-with-placeholder"
        placeholder_path = f"{folder_path}/"
        gcsfs.touch(placeholder_path)

        with gcs_hns_mocks(BucketType.HIERARCHICAL, gcsfs) as mocks:
            mocks["info"].side_effect = [
                {"type": "directory", "name": folder_path},  # from isdir
                FileNotFoundError(folder_path),  # from exists after rm
            ]
            assert gcsfs.isdir(folder_path)

            gcsfs.rmdir(folder_path)

            assert not gcsfs.exists(folder_path)
            mocks["async_lookup_bucket_type"].assert_called_once_with(TEST_HNS_BUCKET)
            expected_request = self._get_delete_folder_request(folder_path)
            mocks["control_client"].delete_folder.assert_called_once_with(
                request=expected_request
            )
            mocks["super_rmdir"].assert_not_called()


class TestExtendedGcsFileSystemRm:
    """Unit tests for the rm method in ExtendedGcsFileSystem."""

    BATCH_SIZE = 20

    def _get_delete_folder_request(self, gcsfs, dir_path):
        """Constructs a DeleteFolderRequest for testing."""
        bucket, key, _ = gcsfs.split_path(dir_path)
        expected_folder_name = f"projects/_/buckets/{bucket}/folders/{key.rstrip('/')}"
        return storage_control_v2.DeleteFolderRequest(name=expected_folder_name)

    def test_rm_file_hns(self, gcs_hns, gcs_hns_mocks):
        """Test sync rm on a single file in an HNS bucket."""
        gcsfs = gcs_hns
        file_path = f"{TEST_HNS_BUCKET}/file_to_rm.txt"

        mock_expand = mock.AsyncMock()
        mock_delete_files = mock.AsyncMock()

        with (
            gcs_hns_mocks(BucketType.HIERARCHICAL, gcsfs) as mocks,
            mock.patch.object(gcsfs, "_expand_path", new=mock_expand),
            mock.patch.object(gcsfs, "_delete_files", new=mock_delete_files),
        ):
            mocks["info"].return_value = {"name": file_path, "type": "file"}
            mock_expand.return_value = [file_path]
            mock_delete_files.return_value = []

            gcsfs.rm(file_path)

            mock_expand.assert_called_once_with(
                file_path, recursive=False, maxdepth=None
            )
            mocks["info"].assert_called_once_with(file_path)
            mock_delete_files.assert_awaited_once_with([file_path], self.BATCH_SIZE)

    def test_rm_recursive_hns(self, gcs_hns, gcs_hns_mocks):
        """Test sync recursive rm on a directory in an HNS bucket."""
        gcsfs = gcs_hns
        base_dir = f"{TEST_HNS_BUCKET}/dir_to_rm"
        file_path = f"{base_dir}/file.txt"
        nested_dir1 = f"{base_dir}/nested1"
        nested_file1 = f"{nested_dir1}/file1.txt"
        nested_dir2 = f"{nested_dir1}/nested2"
        nested_file2 = f"{nested_dir2}/file2.txt"

        mock_expand = mock.AsyncMock()
        mock_delete_files = mock.AsyncMock()

        with (
            gcs_hns_mocks(BucketType.HIERARCHICAL, gcsfs) as mocks,
            mock.patch.object(gcsfs, "_expand_path", new=mock_expand),
            mock.patch.object(gcsfs, "_delete_files", new=mock_delete_files),
        ):
            # Simulate the directory structure found by _expand_path and _info
            expanded_paths = [
                base_dir,
                file_path,
                nested_dir1,
                nested_file1,
                nested_dir2,
                nested_file2,
            ]
            info_map = {
                base_dir: {"name": base_dir, "type": "directory"},
                file_path: {"name": file_path, "type": "file"},
                nested_dir1: {"name": nested_dir1, "type": "directory"},
                nested_file1: {"name": nested_file1, "type": "file"},
                nested_dir2: {"name": nested_dir2, "type": "directory"},
                nested_file2: {"name": nested_file2, "type": "file"},
            }
            mocks["info"].side_effect = lambda p: info_map[p]
            mock_expand.return_value = expanded_paths
            mock_delete_files.return_value = []

            gcsfs.rm(base_dir, recursive=True)

            # Verify correct calls
            mock_expand.assert_called_once_with(base_dir, recursive=True, maxdepth=None)
            assert mocks["info"].call_count == len(expanded_paths)
            files_to_delete = sorted([file_path, nested_file1, nested_file2])
            mock_delete_files.assert_awaited_once_with(mock.ANY, self.BATCH_SIZE)
            assert sorted(mock_delete_files.await_args[0][0]) == files_to_delete
            expected_delete_folder_requests = [
                mock.call(request=self._get_delete_folder_request(gcsfs, nested_dir2)),
                mock.call(request=self._get_delete_folder_request(gcsfs, nested_dir1)),
                mock.call(request=self._get_delete_folder_request(gcsfs, base_dir)),
            ]
            mocks["control_client"].delete_folder.assert_has_calls(
                expected_delete_folder_requests
            )

    def test_rm_non_hns_fallback(self, gcs_hns, gcs_hns_mocks):
        """Test that sync rm falls back to the parent implementation for non-HNS buckets."""
        gcsfs = gcs_hns
        path = f"{TEST_HNS_BUCKET}/some_path"

        with gcs_hns_mocks(BucketType.NON_HIERARCHICAL, gcsfs) as mocks:
            gcsfs.rm(path, recursive=True)

            # Verify it called the parent's _rm and not the HNS-specific logic
            mocks["super_rm"].assert_awaited_once_with(
                path, recursive=True, maxdepth=None, batchsize=self.BATCH_SIZE
            )

    def test_rm_non_existent_path_hns(self, gcs_hns, gcs_hns_mocks):
        """Test sync rm on a non-existent path in an HNS bucket raises FileNotFoundError."""
        gcsfs = gcs_hns
        path = f"{TEST_HNS_BUCKET}/dne"

        with (
            gcs_hns_mocks(BucketType.HIERARCHICAL, gcsfs) as mocks,
            mock.patch.object(
                gcsfs, "_expand_path", new_callable=mock.AsyncMock
            ) as mock_expand,
        ):
            mock_expand.return_value = []  # Nothing found

            with pytest.raises(FileNotFoundError):
                gcsfs.rm(path)

            mock_expand.assert_awaited_once_with(path, recursive=False, maxdepth=None)
            mocks["control_client"].delete_folder.assert_not_called()

    def test_rm_empty_dir_hns(self, gcs_hns, gcs_hns_mocks):
        """Test sync rm on an empty directory in an HNS bucket."""
        gcsfs = gcs_hns
        dir_path = f"{TEST_HNS_BUCKET}/empty_dir"

        mock_expand = mock.AsyncMock()
        mock_delete_files = mock.AsyncMock()

        with (
            gcs_hns_mocks(BucketType.HIERARCHICAL, gcsfs) as mocks,
            mock.patch.object(gcsfs, "_expand_path", new=mock_expand),
            mock.patch.object(gcsfs, "_delete_files", new=mock_delete_files),
        ):
            mocks["info"].return_value = {"name": dir_path, "type": "directory"}
            mock_expand.return_value = [dir_path]
            mock_delete_files.return_value = []

            gcsfs.rm(dir_path, recursive=True)

            mock_delete_files.assert_awaited_once_with([], self.BATCH_SIZE)
            expected_request = self._get_delete_folder_request(gcsfs, dir_path)
            mocks["control_client"].delete_folder.assert_called_once_with(
                request=expected_request
            )

    def test_rm_non_recursive_on_non_empty_dir_fails(self, gcs_hns, gcs_hns_mocks):
        """Test that rm without recursive=True on a non-empty directory fails."""
        gcsfs = gcs_hns
        dir_path = f"{TEST_HNS_BUCKET}/non_empty_dir"

        mock_expand = mock.AsyncMock()

        with (
            gcs_hns_mocks(BucketType.HIERARCHICAL, gcsfs) as mocks,
            mock.patch.object(gcsfs, "_expand_path", new=mock_expand),
        ):
            # Mock expand_path to return the directory itself
            mock_expand.return_value = [dir_path]
            mocks["info"].return_value = {"name": dir_path, "type": "directory"}

            # Mock delete_folder to raise FailedPrecondition (directory not empty)
            mocks["control_client"].delete_folder.side_effect = (
                api_exceptions.FailedPrecondition("Directory not empty")
            )

            with pytest.raises(OSError, match="Pre condition failed"):
                gcsfs.rm(dir_path, recursive=False)

            mock_expand.assert_called_once_with(
                dir_path, recursive=False, maxdepth=None
            )
            expected_request = self._get_delete_folder_request(gcsfs, dir_path)
            mocks["control_client"].delete_folder.assert_called_once_with(
                request=expected_request
            )

    def test_rm_multiple_paths(self, gcs_hns, gcs_hns_mocks):
        """Test rm with a list of paths containing both files and directories."""
        gcsfs = gcs_hns
        file_path1 = f"{TEST_HNS_BUCKET}/file.txt"
        file_path2 = f"{TEST_HNS_BUCKET}/another_file.txt"
        dir_path = f"{TEST_HNS_BUCKET}/dir"

        mock_expand = mock.AsyncMock()
        mock_delete_files = mock.AsyncMock()

        with (
            gcs_hns_mocks(BucketType.HIERARCHICAL, gcsfs) as mocks,
            mock.patch.object(gcsfs, "_expand_path", new=mock_expand),
            mock.patch.object(gcsfs, "_delete_files", new=mock_delete_files),
        ):
            mock_expand.return_value = [file_path1, file_path2, dir_path]

            def info_side_effect(path):
                if path == file_path1 or path == file_path2:
                    return {"name": path, "type": "file"}
                return {"name": path, "type": "directory"}

            mocks["info"].side_effect = info_side_effect

            mock_delete_files.return_value = []

            gcsfs.rm([file_path1, file_path2, dir_path], recursive=True)

            mock_expand.assert_called_once_with(
                [file_path1, file_path2, dir_path], recursive=True, maxdepth=None
            )
            mock_delete_files.assert_awaited_once()
            args, _ = mock_delete_files.await_args
            assert args[0] == [file_path1, file_path2]

            expected_request = self._get_delete_folder_request(gcsfs, dir_path)
            mocks["control_client"].delete_folder.assert_called_once_with(
                request=expected_request
            )
