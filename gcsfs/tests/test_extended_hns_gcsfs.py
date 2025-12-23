import contextlib
import os
from unittest import mock

import pytest
from google.api_core import exceptions as api_exceptions
from google.cloud import storage_control_v2

from gcsfs.extended_gcsfs import BucketType
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


@pytest.fixture
def gcs_hns_mocks():
    """A factory fixture for mocking bucket functionality for HNS mv tests."""

    @contextlib.contextmanager
    def _gcs_hns_mocks_factory(bucket_type_val, gcsfs):
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
        patch_target_super_mv = "gcsfs.core.GCSFileSystem.mv"
        patch_target_super_rmdir = "gcsfs.core.GCSFileSystem._rmdir"

        # Mock the async rename_folder method on the storage_control_client
        mock_rename_folder = mock.AsyncMock()
        mock_control_client_instance = mock.AsyncMock()
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
                patch_target_super_rmdir, new_callable=mock.AsyncMock
            ) as mock_super_rmdir,
        ):
            mock_async_lookup_bucket_type.return_value = bucket_type_val
            mock_sync_lookup_bucket_type.return_value = bucket_type_val
            mocks = {
                "async_lookup_bucket_type": mock_async_lookup_bucket_type,
                "sync_lookup_bucket_type": mock_sync_lookup_bucket_type,
                "info": mock_info,
                "control_client": mock_control_client_instance,
                "super_mv": mock_super_mv,
                "super_rmdir": mock_super_rmdir,
            }
            yield mocks

    return _gcs_hns_mocks_factory


class TestExtendedGcsFileSystemMv:
    """Unit tests for the _mv method in ExtendedGcsFileSystem."""

    def _assert_rename_folder_called_with(self, mocks, path1, path2):
        """Asserts that the rename_folder method was called with the correct request."""
        path1_key = path1.split("/", 1)[1]
        path2_key = path2.split("/", 1)[1]
        expected_request = storage_control_v2.RenameFolderRequest(
            name=f"projects/_/buckets/{TEST_HNS_BUCKET}/folders/{path1_key}",
            destination_folder_id=path2_key,
        )
        mocks["control_client"].rename_folder.assert_called_once_with(
            request=expected_request
        )

    def _assert_hns_rename_called_using_logs(self, mocks, caplog, path1, path2):
        """Asserts that HNS rename was called by checking logs and mocks."""
        # Verify log message for both mocked and real GCS.
        hns_log_message = f"Using HNS-aware folder rename for '{path1}' to '{path2}'."
        assert any(
            hns_log_message in record.message for record in caplog.records
        ), "HNS rename log message not found."

    rename_success_params = [
        pytest.param("old_dir", "new_dir", id="simple_rename_at_root"),
        pytest.param(
            "nested/old_dir",
            "nested/new_dir",
            id="rename_within_nested_dir",
        ),
    ]

    @pytest.mark.parametrize("path1, path2", rename_success_params)
    def test_hns_folder_rename_success(
        self, gcs_hns, gcs_hns_mocks, path1, path2, caplog
    ):
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
            if mocks:
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

            self._assert_hns_rename_called_using_logs(mocks, caplog, path1, path2)

            if mocks:
                mocks["async_lookup_bucket_type"].assert_called_once_with(
                    TEST_HNS_BUCKET
                )
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

                self._assert_rename_folder_called_with(mocks, path1, path2)
                mocks["super_mv"].assert_not_called()

    def test_hns_folder_rename_with_protocol(self, gcs_hns, gcs_hns_mocks, caplog):
        """Test successful HNS folder rename when paths include the protocol."""
        gcsfs = gcs_hns
        path1_no_proto = f"{TEST_HNS_BUCKET}/old_dir_proto"
        path2_no_proto = f"{TEST_HNS_BUCKET}/new_dir_proto"
        path1 = f"gs://{path1_no_proto}"
        path2 = f"gs://{path2_no_proto}"

        file_in_root = f"{path1}/file1.txt"
        gcsfs.touch(file_in_root)

        with gcs_hns_mocks(BucketType.HIERARCHICAL, gcsfs) as mocks:
            if mocks:
                # The paths passed to info will be stripped of protocol
                mocks["info"].side_effect = [
                    {"type": "directory", "name": path1},
                    FileNotFoundError(path1),
                    {"type": "directory", "name": path2},
                ]

            gcsfs.mv(path1, path2)

            assert not gcsfs.exists(path1)
            assert gcsfs.exists(path2)

            self._assert_hns_rename_called_using_logs(mocks, caplog, path1, path2)

            if mocks:
                mocks["async_lookup_bucket_type"].assert_called_once_with(
                    TEST_HNS_BUCKET
                )
                expected_info_calls = [
                    mock.call(path1),  # from _mv
                    mock.call(path1),  # from exists(path1)
                    mock.call(path2),  # from exists(path2)
                ]
                mocks["info"].assert_has_awaits(expected_info_calls)
                self._assert_rename_folder_called_with(
                    mocks, path1_no_proto, path2_no_proto
                )
                mocks["super_mv"].assert_not_called()

    @pytest.mark.skipif(
        os.environ.get("STORAGE_EMULATOR_HOST") == "https://storage.googleapis.com",
        reason=(
            "Skipping on real GCS, info method throws FileNotFoundError for empty directories on HNS buckets. "
            "TODO: This can be unskipped once info supports HNS buckets."
        ),
    )
    def test_hns_empty_folder_rename_success(self, gcs_hns, gcs_hns_mocks, caplog):
        """Test successful HNS rename of an empty folder."""
        gcsfs = gcs_hns
        path1 = f"{TEST_HNS_BUCKET}/empty_old_dir"
        path2 = f"{TEST_HNS_BUCKET}/empty_new_dir"

        # Simulate creating an empty directory by creating and then deleting a file inside a
        # folder as mkdir is still not supported on HNS buckets.
        placeholder_file = f"{path1}/placeholder.txt"
        gcsfs.touch(placeholder_file)
        gcsfs.rm(placeholder_file)

        with gcs_hns_mocks(BucketType.HIERARCHICAL, gcsfs) as mocks:
            if mocks:
                # Configure mocks for the sequence of calls
                mocks["info"].side_effect = [
                    {"type": "directory", "name": path1},  # _mv check
                    FileNotFoundError(path1),  # exists(path1) after move
                    {"type": "directory", "name": path2},  # exists(path2) after move
                ]

            gcsfs.mv(path1, path2)

            assert not gcsfs.exists(path1)
            assert gcsfs.exists(path2)

            self._assert_hns_rename_called_using_logs(mocks, caplog, path1, path2)

            if mocks:
                mocks["async_lookup_bucket_type"].assert_called_once_with(
                    TEST_HNS_BUCKET
                )
                expected_info_calls = [
                    mock.call(path1),  # from _mv
                    mock.call(path1),  # from exists(path1)
                    mock.call(path2),  # from exists(path2)
                ]
                mocks["info"].assert_has_awaits(expected_info_calls)
                self._assert_rename_folder_called_with(mocks, path1, path2)
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
            if mocks:
                mocks["info"].side_effect = [
                    {"type": "file"},
                    FileNotFoundError(path1),
                    {"type": "file", "name": path2},
                ]

            gcsfs.touch(path1)
            gcsfs.mv(path1, path2)

            assert not gcsfs.exists(path1)
            assert gcsfs.exists(path2)

            if mocks:
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
            if mocks:
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

            if mocks:
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
            if mocks:
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

            if mocks:
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

            if mocks:
                mocks["async_lookup_bucket_type"].assert_not_called()
                mocks["info"].assert_not_called()
                mocks["control_client"].rename_folder.assert_not_called()
                mocks["super_mv"].assert_not_called()

    def test_hns_rename_fails_if_parent_dne(self, gcs_hns, gcs_hns_mocks, caplog):
        """Test that HNS rename fails if the destination's parent does not exist."""
        gcsfs = gcs_hns
        path1 = f"{TEST_HNS_BUCKET}/dir_to_move"
        path2 = f"{TEST_HNS_BUCKET}/new_parent/new_name"

        with gcs_hns_mocks(BucketType.HIERARCHICAL, gcsfs) as mocks:
            if mocks:
                # Mocked environment assertions
                mocks["info"].return_value = {"type": "directory"}
                mocks["control_client"].rename_folder.side_effect = (
                    api_exceptions.FailedPrecondition(
                        "The parent folder does not exist."
                    )
                )

            gcsfs.touch(f"{path1}/file.txt")

            # The underlying API error includes the status code (400) in its string representation.
            expected_msg = "HNS rename failed: 400 The parent folder does not exist."
            with pytest.raises(OSError, match=expected_msg):
                gcsfs.mv(path1, path2)

            self._assert_hns_rename_called_using_logs(mocks, caplog, path1, path2)

            if mocks:
                self._assert_rename_folder_called_with(mocks, path1, path2)
                mocks["info"].assert_awaited_with(path1)
                mocks["super_mv"].assert_not_called()

    def test_hns_folder_rename_cache_invalidation(self, gcs_hns, gcs_hns_mocks):
        """Test that HNS folder rename correctly invalidates and updates the cache."""
        gcsfs = gcs_hns
        base_dir = f"{TEST_HNS_BUCKET}/cache_test"
        path1 = f"{base_dir}/old_dir"
        destination_parent = f"{base_dir}/destination_parent"
        path2 = f"{destination_parent}/new_nested_dir"
        sibling_dir = f"{base_dir}/sibling_dir"

        with gcs_hns_mocks(BucketType.HIERARCHICAL, gcsfs) as mocks:
            # --- Setup ---
            gcsfs.touch(f"{path1}/sub/file.txt")
            gcsfs.touch(f"{sibling_dir}/sibling_file.txt")
            gcsfs.touch(f"{destination_parent}/file.txt")

            # --- Populate Cache ---
            # Use find() to deeply populate the cache for the entire base directory
            gcsfs.find(base_dir, withdirs=True)

            # --- Pre-Rename Assertions ---
            # Ensure all relevant paths are in the cache before the rename
            assert base_dir in gcsfs.dircache
            assert path1 in gcsfs.dircache
            assert destination_parent in gcsfs.dircache
            assert f"{path1}/sub" in gcsfs.dircache
            assert sibling_dir in gcsfs.dircache

            if mocks:
                # Mock the info call for the mv operation itself
                mocks["info"].return_value = {"type": "directory", "name": path1}

            # --- Perform Rename ---
            gcsfs.mv(path1, path2)

            # --- Post-Rename Cache Assertions ---
            # 1. Source directory and its descendants should be removed from the cache
            assert path1 not in gcsfs.dircache
            assert f"{path1}/sub" not in gcsfs.dircache

            # 2. The destination path should be removed from cache
            assert (
                path2 not in gcsfs.dircache
            ), "Destination path should not be in dircache"

            # 3. The parent directory of the source should have been updated, not cleared.
            # It should now just contain the original sibling.
            assert base_dir in gcsfs.dircache
            source_parent_listing = gcsfs.dircache[base_dir]

            # Check that the old directory is gone from the parent's listing
            assert not any(
                entry["name"] == path1 for entry in source_parent_listing
            ), "Old directory should be removed from parent cache"

            # Check that the sibling directory is still there
            assert any(
                entry["name"] == sibling_dir for entry in source_parent_listing
            ), "Sibling directory should remain in parent cache"

            # 4. The destination parent's cache should be updated with the new path
            assert destination_parent in gcsfs.dircache
            assert any(
                entry["name"] == path2 for entry in gcsfs.dircache[destination_parent]
            ), "New path should be added to destination parent cache"

            # Cache for sibling folder should be untouched
            assert sibling_dir in gcsfs.dircache

    def test_hns_rename_raises_file_not_found(self, gcs_hns, gcs_hns_mocks):
        """Test that NotFound from API raises FileNotFoundError."""
        gcsfs = gcs_hns
        path1 = f"{TEST_HNS_BUCKET}/dne"
        path2 = f"{TEST_HNS_BUCKET}/new_dir"
        with gcs_hns_mocks(BucketType.HIERARCHICAL, gcsfs) as mocks:
            if mocks:
                mocks["info"].side_effect = FileNotFoundError(path1)

            with pytest.raises(FileNotFoundError):
                gcsfs.mv(path1, path2)

            if mocks:
                mocks["info"].assert_awaited_with(path1)
                mocks["super_mv"].assert_not_called()
                mocks["control_client"].rename_folder.assert_not_called()

    def test_hns_rename_raises_file_not_found_on_race_condition(
        self, gcs_hns, gcs_hns_mocks, caplog
    ):
        """Test that api_exceptions.NotFound from rename call raises FileNotFoundError."""
        is_real_gcs = (
            os.environ.get("STORAGE_EMULATOR_HOST") == "https://storage.googleapis.com"
        )
        if is_real_gcs:
            pytest.skip(
                "Cannot simulate race condition for rename against real GCS endpoint."
            )
        gcsfs = gcs_hns
        path1 = f"{TEST_HNS_BUCKET}/dir_disappears"
        path2 = f"{TEST_HNS_BUCKET}/new_dir"

        with gcs_hns_mocks(BucketType.HIERARCHICAL, gcsfs) as mocks:
            if mocks:
                # Simulate _info finding the directory
                mocks["info"].return_value = {"type": "directory"}
                # Simulate the directory being gone when rename_folder is called
                mocks["control_client"].rename_folder.side_effect = (
                    api_exceptions.NotFound("Folder not found during rename")
                )

            with pytest.raises(FileNotFoundError, match="Source .* not found"):
                gcsfs.mv(path1, path2)

            self._assert_hns_rename_called_using_logs(mocks, caplog, path1, path2)

            if mocks:
                self._assert_rename_folder_called_with(mocks, path1, path2)
                mocks["info"].assert_awaited_with(path1)
                mocks["super_mv"].assert_not_called()

    def test_hns_rename_raises_os_error_if_destination_exists(
        self, gcs_hns, gcs_hns_mocks, caplog
    ):
        """Test that FailedPrecondition from API raises OSError."""
        gcsfs = gcs_hns
        path1 = f"{TEST_HNS_BUCKET}/dir"
        path2 = f"{TEST_HNS_BUCKET}/existing_dir"

        with gcs_hns_mocks(BucketType.HIERARCHICAL, gcsfs) as mocks:
            if mocks:
                mocks["info"].return_value = {"type": "directory"}
                mocks["control_client"].rename_folder.side_effect = (
                    api_exceptions.Conflict("HNS rename failed due to conflict for")
                )

            gcsfs.touch(f"{path1}/file.txt")
            gcsfs.touch(f"{path2}/file.txt")

            expected_msg = (
                f"HNS rename failed due to conflict for '{path1}' to '{path2}'"
            )
            with pytest.raises(FileExistsError, match=expected_msg):
                gcsfs.mv(path1, path2)

            self._assert_hns_rename_called_using_logs(mocks, caplog, path1, path2)

            if mocks:
                self._assert_rename_folder_called_with(mocks, path1, path2)
                mocks["info"].assert_awaited_with(path1)
                mocks["super_mv"].assert_not_called()
                mocks["control_client"].rename_folder.assert_called()


class TestExtendedGcsFileSystemRmdir:
    """Unit tests for the rmdir method in ExtendedGcsFileSystem."""

    def _assert_delete_folder_called_with(self, mocks, dir_path):
        """Asserts that delete_folder was called with the correct request."""
        bucket, folder_path = dir_path.split("/", 1)
        expected_folder_name = f"projects/_/buckets/{bucket}/folders/{folder_path}"
        mocks["control_client"].delete_folder.assert_called_once_with(
            request=storage_control_v2.DeleteFolderRequest(name=expected_folder_name)
        )

    def _assert_hns_rmdir_called_using_logs(self, caplog, path):
        """Asserts that HNS rmdir was called by checking logs."""
        hns_log_message = f"Using HNS-aware rmdir for '{path}'."
        assert any(
            hns_log_message in record.message for record in caplog.records
        ), "HNS rmdir log message not found."

    @pytest.mark.skipif(
        os.environ.get("STORAGE_EMULATOR_HOST") == "https://storage.googleapis.com",
        reason=(
            "Skipping on real GCS, info method throws FileNotFoundError for empty directories on HNS buckets. "
            "TODO: This can be unskipped once info supports HNS buckets."
        ),
    )
    def test_hns_rmdir_success(self, gcs_hns, gcs_hns_mocks, caplog):
        """Test successful HNS empty directory deletion."""
        gcsfs = gcs_hns
        dir_name = "empty_dir"
        dir_path = f"{TEST_HNS_BUCKET}/{dir_name}"

        with gcs_hns_mocks(BucketType.HIERARCHICAL, gcsfs) as mocks:
            if mocks:
                # Configure mocks
                # 1. exists(dir_path) before rmdir should succeed.
                # 2. exists(dir_path) after rmdir should fail.
                mocks["info"].side_effect = [
                    {"type": "directory", "name": dir_path},
                    FileNotFoundError(dir_path),
                ]

            # Create an empty directory, can be replaced with mkdir once it is implemented for HNS buckets.
            gcsfs.touch(f"{dir_path}/placeholder")
            gcsfs.rm(f"{dir_path}/placeholder")

            assert gcsfs.exists(dir_path)

            gcsfs.rmdir(dir_path)

            assert not gcsfs.exists(dir_path)

            self._assert_hns_rmdir_called_using_logs(caplog, dir_path)

            if mocks:
                mocks["async_lookup_bucket_type"].assert_called_once_with(
                    TEST_HNS_BUCKET
                )
                self._assert_delete_folder_called_with(mocks, dir_path)
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
            if mocks:
                mocks["async_lookup_bucket_type"].assert_called_once_with(
                    TEST_HNS_BUCKET
                )
                mocks["control_client"].delete_folder.assert_not_called()
                mocks["super_rmdir"].assert_called_once_with(dir_path)

    def test_hns_rmdir_non_empty_raises_os_error(self, gcs_hns, gcs_hns_mocks, caplog):
        """Test that HNS rmdir on a non-empty directory raises OSError."""
        gcsfs = gcs_hns
        dir_path = f"{TEST_HNS_BUCKET}/non_empty_dir"
        gcsfs.touch(f"{dir_path}/file.txt")

        with gcs_hns_mocks(BucketType.HIERARCHICAL, gcsfs) as mocks:
            if mocks:
                mocks["control_client"].delete_folder.side_effect = (
                    api_exceptions.FailedPrecondition("")
                )

            with pytest.raises(OSError, match="Pre condition failed for rmdir"):
                gcsfs.rmdir(dir_path)

            self._assert_hns_rmdir_called_using_logs(caplog, dir_path)

            if mocks:
                mocks["async_lookup_bucket_type"].assert_called_once_with(
                    TEST_HNS_BUCKET
                )
                self._assert_delete_folder_called_with(mocks, dir_path)
                mocks["super_rmdir"].assert_not_called()

    def test_hns_rmdir_dne_raises_not_found(self, gcs_hns, gcs_hns_mocks, caplog):
        """Test that HNS rmdir on a non-existent directory raises FileNotFoundError."""
        gcsfs = gcs_hns
        dir_name = "dne_dir"
        dir_path = f"{TEST_HNS_BUCKET}/{dir_name}"

        with gcs_hns_mocks(BucketType.HIERARCHICAL, gcsfs) as mocks:
            if mocks:
                mocks["control_client"].delete_folder.side_effect = (
                    api_exceptions.NotFound("")
                )

            with pytest.raises(FileNotFoundError, match="rmdir failed for path"):
                gcsfs.rmdir(dir_path)

            self._assert_hns_rmdir_called_using_logs(caplog, dir_path)

            if mocks:
                mocks["async_lookup_bucket_type"].assert_called_once_with(
                    TEST_HNS_BUCKET
                )
                self._assert_delete_folder_called_with(mocks, dir_path)
                mocks["super_rmdir"].assert_not_called()

    def test_rmdir_on_file_raises_file_not_found(self, gcs_hns, gcs_hns_mocks, caplog):
        """
        Test that HNS rmdir on a file path raises FileNotFoundError.
        The API returns NotFound in this case.
        """
        gcsfs = gcs_hns
        file_name = "a_file.txt"
        file_path = f"{TEST_HNS_BUCKET}/{file_name}"
        gcsfs.touch(file_path)

        with gcs_hns_mocks(BucketType.HIERARCHICAL, gcsfs) as mocks:
            if mocks:
                mocks["control_client"].delete_folder.side_effect = (
                    api_exceptions.NotFound("")
                )

            with pytest.raises(FileNotFoundError, match="rmdir failed for path"):
                gcsfs.rmdir(file_path)

            self._assert_hns_rmdir_called_using_logs(caplog, file_path)

            if mocks:
                mocks["async_lookup_bucket_type"].assert_called_once_with(
                    TEST_HNS_BUCKET
                )
                self._assert_delete_folder_called_with(mocks, file_path)
                mocks["super_rmdir"].assert_not_called()

    def test_hns_rmdir_with_empty_subfolder_raises_os_error(
        self, gcs_hns, gcs_hns_mocks, caplog
    ):
        """Test that HNS rmdir on a directory with an empty subfolder raises OSError."""
        gcsfs = gcs_hns
        parent_dir = f"{TEST_HNS_BUCKET}/parent_dir"
        sub_dir = f"{parent_dir}/sub_dir"

        # Create an empty sub-directory
        gcsfs.touch(f"{sub_dir}/placeholder")
        gcsfs.rm(f"{sub_dir}/placeholder")

        with gcs_hns_mocks(BucketType.HIERARCHICAL, gcsfs) as mocks:
            if mocks:
                mocks["control_client"].delete_folder.side_effect = (
                    api_exceptions.FailedPrecondition("Directory not empty")
                )

            with pytest.raises(OSError, match="Pre condition failed for rmdir"):
                gcsfs.rmdir(parent_dir)

            self._assert_hns_rmdir_called_using_logs(caplog, parent_dir)

            if mocks:
                mocks["async_lookup_bucket_type"].assert_called_once_with(
                    TEST_HNS_BUCKET
                )
                self._assert_delete_folder_called_with(mocks, parent_dir)
                mocks["super_rmdir"].assert_not_called()

    def test_hns_rmdir_nested_directories_from_leaf(
        self, gcs_hns, gcs_hns_mocks, caplog
    ):
        """Test deleting nested directories starting from the leaf."""
        gcsfs = gcs_hns
        parent_dir = f"{TEST_HNS_BUCKET}/parent"
        child_dir = f"{parent_dir}/child"
        grandchild_dir = f"{child_dir}/grandchild"

        # Create nested empty directories
        gcsfs.touch(f"{grandchild_dir}/placeholder")
        gcsfs.rm(f"{grandchild_dir}/placeholder")

        with gcs_hns_mocks(BucketType.HIERARCHICAL, gcsfs) as mocks:
            if mocks:
                mocks["info"].side_effect = [
                    FileNotFoundError(grandchild_dir),
                    FileNotFoundError(child_dir),
                    FileNotFoundError(parent_dir),
                ]
            # Delete leaf first
            gcsfs.rmdir(grandchild_dir)
            assert not gcsfs.exists(grandchild_dir)
            self._assert_hns_rmdir_called_using_logs(caplog, grandchild_dir)

            # Delete child
            gcsfs.rmdir(child_dir)
            assert not gcsfs.exists(child_dir)
            self._assert_hns_rmdir_called_using_logs(caplog, child_dir)

            # Delete parent
            gcsfs.rmdir(parent_dir)
            assert not gcsfs.exists(parent_dir)
            self._assert_hns_rmdir_called_using_logs(caplog, parent_dir)

            if mocks:
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
            if mocks:
                mocks["super_rmdir"].side_effect = HttpError(
                    {
                        "code": 409,
                        "message": "The bucket you tried to delete is not empty",
                    }
                )

            with pytest.raises(
                HttpError, match="The bucket you tried to delete is not empty"
            ):
                gcsfs.rmdir(bucket_path)

            if mocks:
                mocks["async_lookup_bucket_type"].assert_not_called()
                mocks["control_client"].delete_folder.assert_not_called()
                mocks["super_rmdir"].assert_called_once_with(bucket_path)

    def test_hns_rmdir_cache_invalidation(self, gcs_hns, gcs_hns_mocks):
        """Test that HNS rmdir correctly invalidates and updates the cache."""
        gcsfs = gcs_hns
        base_dir = f"{TEST_HNS_BUCKET}/rmdir_cache_test"
        dir_to_delete = f"{base_dir}/dir_to_delete"
        sibling_dir = f"{base_dir}/sibling_dir"

        with gcs_hns_mocks(BucketType.HIERARCHICAL, gcsfs):
            # --- Setup ---
            gcsfs.touch(f"{dir_to_delete}/file.txt")
            gcsfs.touch(f"{sibling_dir}/sibling_file.txt")

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
            if mocks:
                mocks["super_rmdir"].side_effect = HttpError(
                    {
                        "code": 409,
                        "message": "The bucket you tried to delete is not empty",
                    }
                )

            with pytest.raises(
                HttpError, match="The bucket you tried to delete is not empty"
            ):
                gcsfs.rmdir(bucket_path)

            if mocks:
                mocks["async_lookup_bucket_type"].assert_not_called()
                mocks["control_client"].delete_folder.assert_not_called()
                mocks["super_rmdir"].assert_called_once_with(bucket_path)

    @pytest.mark.skipif(
        os.environ.get("STORAGE_EMULATOR_HOST") == "https://storage.googleapis.com",
        reason=(
            "Deletes bucket. TODO: re-enable on real GCS once mkdir for HNS buckets"
            " is implemented to recreate bucket in fixture."
        ),
    )
    def test_rmdir_on_hns_bucket_falls_back(self, gcs_hns, gcs_hns_mocks, caplog):
        """Test that rmdir on a bucket falls back to parent method."""
        gcsfs = gcs_hns
        bucket_path = f"{TEST_HNS_BUCKET}"

        with gcs_hns_mocks(BucketType.HIERARCHICAL, gcsfs) as mocks:
            if mocks:
                # exists(dir_path) after rmdir should fail.
                mocks["info"].side_effect = [
                    FileNotFoundError(bucket_path),
                ]

            gcsfs.rmdir(bucket_path)
            assert not gcsfs.exists(bucket_path)
            if mocks:
                mocks["async_lookup_bucket_type"].assert_not_called()
                mocks["control_client"].delete_folder.assert_not_called()
                mocks["super_rmdir"].assert_called_once_with(bucket_path)

    @pytest.mark.skipif(
        os.environ.get("STORAGE_EMULATOR_HOST") == "https://storage.googleapis.com",
        reason=(
            "Deletes bucket. TODO: re-enable on real GCS once mkdir for HNS buckets"
            " is implemented to recreate bucket in fixture."
        ),
    )
    def test_rmdir_on_non_hns_bucket_falls_back(self, gcs_hns, gcs_hns_mocks, caplog):
        """Test that rmdir on a bucket falls back to parent method."""
        gcsfs = gcs_hns
        bucket_path = f"{TEST_HNS_BUCKET}"

        with gcs_hns_mocks(BucketType.NON_HIERARCHICAL, gcsfs) as mocks:
            if mocks:
                # exists(dir_path) after rmdir should fail.
                mocks["info"].side_effect = [
                    FileNotFoundError(bucket_path),
                ]

            gcsfs.rmdir(bucket_path)
            assert not gcsfs.exists(bucket_path)
            if mocks:
                mocks["async_lookup_bucket_type"].assert_not_called()
                mocks["control_client"].delete_folder.assert_not_called()
                mocks["super_rmdir"].assert_called_once_with(bucket_path)
