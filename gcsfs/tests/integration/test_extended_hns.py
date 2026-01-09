import os

import pytest

from gcsfs.tests.settings import TEST_HNS_BUCKET

should_run_hns = os.getenv("GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT", "false").lower() in (
    "true",
    "1",
)

# Skip these tests if not running against a real GCS backend or if experimentation flag is not set.
pytestmark = pytest.mark.skipif(
    os.environ.get("STORAGE_EMULATOR_HOST") != "https://storage.googleapis.com"
    or not should_run_hns,
    reason="This test class is for real GCS HNS buckets only and requires experimental flag.",
)


class TestExtendedGcsFileSystemMv:
    """Integration tests for the _mv method in ExtendedGcsFileSystem."""

    rename_success_params = [
        pytest.param("old_dir", "new_dir", id="simple_rename_at_root"),
        pytest.param(
            "nested/old_dir",
            "nested/new_dir",
            id="rename_within_nested_dir",
        ),
    ]

    @pytest.mark.parametrize("path1, path2", rename_success_params)
    def test_hns_folder_rename_success(self, gcs_hns, path1, path2):
        """Test successful HNS folder rename."""
        gcsfs = gcs_hns
        path1 = f"{TEST_HNS_BUCKET}/{path1}"
        path2 = f"{TEST_HNS_BUCKET}/{path2}"

        file_in_root = f"{path1}/file1.txt"
        nested_file = f"{path1}/sub_dir/file2.txt"

        gcsfs.touch(file_in_root)
        gcsfs.touch(nested_file)

        gcsfs.mv(path1, path2)

        # Verify that the old path no longer exist
        assert not gcsfs.exists(path1)

        # Verify that the new paths exist
        assert gcsfs.exists(path2)
        assert gcsfs.exists(f"{path2}/file1.txt")
        assert gcsfs.exists(f"{path2}/sub_dir/file2.txt")

    def test_hns_folder_rename_with_protocol(self, gcs_hns):
        """Test successful HNS folder rename when paths include the protocol."""
        gcsfs = gcs_hns
        path1_no_proto = f"{TEST_HNS_BUCKET}/old_dir_proto"
        path2_no_proto = f"{TEST_HNS_BUCKET}/new_dir_proto"
        path1 = f"gs://{path1_no_proto}"
        path2 = f"gs://{path2_no_proto}"

        file_in_root = f"{path1}/file1.txt"
        gcsfs.touch(file_in_root)

        gcsfs.mv(path1, path2)

        assert not gcsfs.exists(path1)
        assert gcsfs.exists(path2)

    def test_hns_empty_folder_rename_success(self, gcs_hns):
        """Test successful HNS rename of an empty folder."""
        gcsfs = gcs_hns
        path1 = f"{TEST_HNS_BUCKET}/empty_old_dir"
        path2 = f"{TEST_HNS_BUCKET}/empty_new_dir"

        gcsfs.mkdir(path1)

        gcsfs.mv(path1, path2)

        assert not gcsfs.exists(path1)
        assert gcsfs.exists(path2)

    def test_file_rename_fallback_to_super_mv(
        self,
        gcs_hns,
    ):
        """Test scenarios that should fall back to the parent's mv method."""
        gcsfs = gcs_hns
        path1 = f"{TEST_HNS_BUCKET}/file.txt"
        path2 = f"{TEST_HNS_BUCKET}/new_file.txt"

        gcsfs.touch(path1)
        gcsfs.mv(path1, path2)

        assert not gcsfs.exists(path1)
        assert gcsfs.exists(path2)

    @pytest.mark.skip(reason="Skipping until rm is implemented for HNS buckets")
    def test_folder_rename_to_root_directory(
        self,
        gcs_hns,
    ):
        # TODO: Un-skip the integration test once rm is implemented for HNS buckets.
        """Test scenarios that should fall back to the parent's mv method."""
        gcsfs = gcs_hns
        dir_name = "root_dir"
        path1 = f"{TEST_HNS_BUCKET}/test/{dir_name}"
        path2 = f"{TEST_HNS_BUCKET}/"

        gcsfs.touch(f"{path1}/file.txt")
        gcsfs.mv(path1, path2, recursive=True)

        assert not gcsfs.exists(path1)
        assert gcsfs.exists(path2)
        assert gcsfs.exists(f"{path2.rstrip('/')}/{dir_name}")
        assert gcsfs.exists(f"{path2.rstrip('/')}/{dir_name}/file.txt")

    def test_hns_rename_fails_if_parent_dne(self, gcs_hns):
        """Test that HNS rename fails if the destination's parent does not exist."""
        gcsfs = gcs_hns
        path1 = f"{TEST_HNS_BUCKET}/dir_to_move"
        path2 = f"{TEST_HNS_BUCKET}/new_parent/new_name"
        gcsfs.touch(f"{path1}/file.txt")

        expected_msg = "HNS rename failed: 400 The parent folder does not exist."
        with pytest.raises(OSError, match=expected_msg):
            gcsfs.mv(path1, path2)

    def test_hns_folder_rename_cache_invalidation(self, gcs_hns):
        """Test that HNS folder rename correctly invalidates and updates the cache."""
        gcsfs = gcs_hns
        base_dir = f"{TEST_HNS_BUCKET}/cache_test"
        path1 = f"{base_dir}/old_dir"
        destination_parent = f"{base_dir}/destination_parent"
        path2 = f"{destination_parent}/new_nested_dir"
        sibling_dir = f"{base_dir}/sibling_dir"

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

        # --- Perform Rename ---
        gcsfs.mv(path1, path2)

        # --- Post-Rename Cache Assertions ---
        # 1. Source directory and its descendants should be removed from the cache
        assert path1 not in gcsfs.dircache
        assert f"{path1}/sub" not in gcsfs.dircache

        # 2. The destination path should be removed from cache
        assert path2 not in gcsfs.dircache, "Destination path should not be in dircache"

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

    def test_hns_rename_raises_file_not_found(self, gcs_hns):
        """Test that NotFound from API raises FileNotFoundError."""
        gcsfs = gcs_hns
        path1 = f"{TEST_HNS_BUCKET}/dne"
        path2 = f"{TEST_HNS_BUCKET}/new_dir"
        with pytest.raises(FileNotFoundError):
            gcsfs.mv(path1, path2)

    def test_hns_rename_raises_os_error_if_destination_exists(self, gcs_hns):
        """Test that FailedPrecondition from API raises OSError."""
        gcsfs = gcs_hns
        path1 = f"{TEST_HNS_BUCKET}/dir"
        path2 = f"{TEST_HNS_BUCKET}/existing_dir"

        gcsfs.touch(f"{path1}/file.txt")
        gcsfs.touch(f"{path2}/file.txt")

        expected_msg = f"HNS rename failed due to conflict for '{path1}' to '{path2}'"
        with pytest.raises(FileExistsError, match=expected_msg):
            gcsfs.mv(path1, path2)
