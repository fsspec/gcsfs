"""
This module contains integration tests for Hierarchical Namespace (HNS) enabled buckets.

These tests are designed to run against a real GCS backend and require specific
configuration:
- A GCS bucket with HNS enabled must be specified in the environment variable mentioned in `gcsfs/tests/settings.py`.
- `STORAGE_EMULATOR_HOST` must be set to "https://storage.googleapis.com" to use the real GCS endpoint.
- The `GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT` environment variable must be set to 'true'.

Each test class within this module should focus on a specific filesystem operation
that has been extended or modified to support HNS features, such as `mv` (rename)
and `mkdir`.
"""

import os
import uuid

import pytest

from gcsfs.extended_gcsfs import BucketType
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


class TestExtendedGcsFileSystemMkdir:
    """Integration tests for the mkdir method in ExtendedGcsFileSystem."""

    def test_hns_mkdir_success(self, gcs_hns):
        """Test successful HNS folder creation."""
        gcsfs = gcs_hns
        dir_path = f"{TEST_HNS_BUCKET}/new_dir_integration"
        gcsfs.mkdir(dir_path)
        assert gcsfs.isdir(dir_path)

    def test_hns_mkdir_nested_success_with_create_parents(self, gcs_hns):
        """Test successful HNS folder creation for a nested path with create_parents=True."""
        gcsfs = gcs_hns
        parent_dir = f"{TEST_HNS_BUCKET}/nested_parent"
        dir_path = f"{parent_dir}/new_nested_dir"
        gcsfs.mkdir(dir_path, create_parents=True)
        assert gcsfs.exists(parent_dir) and gcsfs.isdir(parent_dir)
        assert gcsfs.exists(dir_path) and gcsfs.isdir(dir_path)

    def test_hns_mkdir_nested_fails_if_create_parents_false(self, gcs_hns):
        """Test HNS mkdir fails for nested path if create_parents=False and parent doesn't exist."""
        gcsfs = gcs_hns
        dir_path = f"{TEST_HNS_BUCKET}/non_existent_parent/new_dir"
        with pytest.raises(FileNotFoundError):
            gcsfs.mkdir(dir_path, create_parents=False)

    def test_mkdir_in_non_existent_bucket_fails(self, gcs_hns):
        """Test that mkdir fails when the target bucket does not exist."""
        gcsfs = gcs_hns
        bucket_name = f"gcsfs-non-existent-bucket-{uuid.uuid4()}"
        dir_path = f"{bucket_name}/some_dir"

        with pytest.raises(FileNotFoundError, match=f"{bucket_name}"):
            gcsfs.mkdir(dir_path, create_parents=False)

    def test_mkdir_in_non_existent_bucket_with_create_parents_succeeds(
        self, gcs_hns, buckets_to_delete
    ):
        """Test that mkdir with create_parents=True creates the bucket."""
        gcsfs = gcs_hns
        bucket_name = f"gcsfs-bucket-mkdir-{uuid.uuid4()}"
        dir_path = f"{bucket_name}/some_dir"
        buckets_to_delete.add(bucket_name)

        assert not gcsfs.exists(bucket_name)
        gcsfs.mkdir(dir_path, create_parents=True)
        assert gcsfs.exists(bucket_name)

    def test_mkdir_hns_bucket_with_create_parents_succeeds(
        self, gcs_hns, buckets_to_delete
    ):
        """Test mkdir with create_parents and enable_hierarchical_namespace creates an HNS bucket."""
        gcsfs = gcs_hns
        bucket_name = f"gcsfs-hns-bucket-mkdir-{uuid.uuid4()}"
        dir_path = f"{bucket_name}/some_dir"
        buckets_to_delete.add(bucket_name)

        assert not gcsfs.exists(bucket_name)
        gcsfs.mkdir(dir_path, create_parents=True, enable_hierarchical_namespace=True)
        assert gcsfs.exists(bucket_name)
        assert gcsfs.exists(dir_path)

        assert gcsfs._sync_lookup_bucket_type(bucket_name) is BucketType.HIERARCHICAL

    def test_mkdir_create_non_hns_bucket(self, gcs_hns, buckets_to_delete):
        """Test creating a new non-HNS bucket by default."""
        gcsfs = gcs_hns
        bucket_path = f"new-non-hns-bucket-{uuid.uuid4()}"
        buckets_to_delete.add(bucket_path)

        assert not gcsfs.exists(bucket_path)
        gcsfs.mkdir(bucket_path)
        assert gcsfs.exists(bucket_path)
        assert (
            gcsfs._sync_lookup_bucket_type(bucket_path) is BucketType.NON_HIERARCHICAL
        )

    def test_mkdir_create_bucket_with_parent_params(self, gcs_hns, buckets_to_delete):
        """Test creating a bucket passes parent-level parameters like enable_versioning."""
        gcsfs = gcs_hns
        bucket_path = f"new-versioned-bucket-{uuid.uuid4()}"
        buckets_to_delete.add(bucket_path)

        assert not gcsfs.exists(bucket_path)
        gcsfs.mkdir(bucket_path, enable_versioning=True, enable_object_retention=True)
        assert gcsfs.exists(bucket_path)

    def test_mkdir_enable_hierarchical_namespace(self, gcs_hns, buckets_to_delete):
        """Test creating a new HNS-enabled bucket."""
        gcsfs = gcs_hns
        bucket_path = f"new-hns-bucket-{uuid.uuid4()}"
        buckets_to_delete.add(bucket_path)

        assert not gcsfs.exists(bucket_path)
        gcsfs.mkdir(bucket_path, enable_hierarchical_namespace=True)
        assert gcsfs.exists(bucket_path)
        assert gcsfs._sync_lookup_bucket_type(bucket_path) is BucketType.HIERARCHICAL


class TestExtendedGcsFileSystemRmdir:
    """Integration tests for the rmdir method in ExtendedGcsFileSystem."""

    def test_hns_rmdir_success(self, gcs_hns):
        """Test successful HNS empty directory deletion."""
        gcsfs = gcs_hns
        dir_path = f"{TEST_HNS_BUCKET}/empty_dir_to_delete"

        gcsfs.mkdir(dir_path)
        assert gcsfs.isdir(dir_path)

        gcsfs.rmdir(dir_path)
        assert not gcsfs.exists(dir_path)

    def test_hns_rmdir_non_empty_raises_os_error(self, gcs_hns):
        """Test that HNS rmdir on a non-empty directory raises OSError."""
        gcsfs = gcs_hns
        dir_path = f"{TEST_HNS_BUCKET}/non_empty_dir_rmdir"
        gcsfs.touch(f"{dir_path}/file.txt")

        with pytest.raises(OSError, match="Pre condition failed for rmdir"):
            gcsfs.rmdir(dir_path)

    def test_hns_rmdir_dne_raises_not_found(self, gcs_hns):
        """Test that HNS rmdir on a non-existent directory raises FileNotFoundError."""
        gcsfs = gcs_hns
        dir_path = f"{TEST_HNS_BUCKET}/dne_dir_rmdir"

        with pytest.raises(FileNotFoundError, match="rmdir failed for path"):
            gcsfs.rmdir(dir_path)

    def test_rmdir_on_file_raises_file_not_found(self, gcs_hns):
        """
        Test that HNS rmdir on a file path raises FileNotFoundError.
        The API returns NotFound in this case.
        """
        gcsfs = gcs_hns
        file_path = f"{TEST_HNS_BUCKET}/a_file_for_rmdir.txt"
        gcsfs.touch(file_path)

        with pytest.raises(FileNotFoundError, match="rmdir failed for path"):
            gcsfs.rmdir(file_path)

    def test_hns_rmdir_with_empty_subfolder_raises_os_error(self, gcs_hns):
        """Test that HNS rmdir on a directory with an empty subfolder raises OSError."""
        gcsfs = gcs_hns
        parent_dir = f"{TEST_HNS_BUCKET}/parent_dir_rmdir"
        sub_dir = f"{parent_dir}/sub_dir"

        gcsfs.mkdir(sub_dir, create_parents=True)

        with pytest.raises(OSError, match="Pre condition failed for rmdir"):
            gcsfs.rmdir(parent_dir)

    def test_hns_rmdir_nested_directories_from_leaf(self, gcs_hns):
        """Test deleting nested directories starting from the leaf."""
        gcsfs = gcs_hns
        parent_dir = f"{TEST_HNS_BUCKET}/parent_rmdir"
        child_dir = f"{parent_dir}/child"
        grandchild_dir = f"{child_dir}/grandchild"

        gcsfs.mkdir(grandchild_dir, create_parents=True)

        # Delete leaf first
        gcsfs.rmdir(grandchild_dir)
        assert not gcsfs.exists(grandchild_dir)

        # Delete child
        gcsfs.rmdir(child_dir)
        assert not gcsfs.exists(child_dir)

        # Delete parent
        gcsfs.rmdir(parent_dir)
        assert not gcsfs.exists(parent_dir)

    def test_rmdir_on_folder_with_placeholder_object(self, gcs_hns):
        """
        Tests that rmdir successfully deletes a folder that contains its own
        zero-byte placeholder object.
        """
        gcsfs = gcs_hns
        folder_path = f"{TEST_HNS_BUCKET}/test-folder-with-placeholder"
        placeholder_path = f"{folder_path}/"
        gcsfs.touch(placeholder_path)

        assert gcsfs.isdir(folder_path)
        gcsfs.rmdir(folder_path)
        assert not gcsfs.exists(folder_path)


class TestExtendedGcsFileSystemLsIntegration:
    """Integration tests for ls method on HNS buckets."""

    @pytest.fixture
    def base_test_dir(self, gcs_hns):
        """
        Fixture to create a unique temporary directory for each test.
        Handles automatic cleanup after the test finishes.
        """
        unique_id = uuid.uuid4().hex
        base_dir = f"{TEST_HNS_BUCKET}/integration_ls_{unique_id}"

        # Explicitly create the base directory
        # This prevents "Parent folder does not exist" errors in tests
        gcs_hns.mkdir(base_dir)

        # Yield the path to the test
        yield base_dir

        # Cleanup: Remove the directory and all contents
        if gcs_hns.exists(base_dir):
            try:
                gcs_hns.rm(base_dir, recursive=True)
            except Exception:
                pass

    def test_ls_empty_folder(self, gcs_hns, base_test_dir):
        """Test that ls correctly lists an explicitly created empty folder."""
        path_empty_folder = f"{base_test_dir}/empty_folder"

        gcs_hns.mkdir(path_empty_folder)

        # List the root directory
        items_root = gcs_hns.ls(base_test_dir, detail=False)
        items_root_cleaned = [item.rstrip("/") for item in items_root]

        assert (
            path_empty_folder in items_root_cleaned
        ), f"Empty folder '{path_empty_folder}' missing from root listing"

    def test_ls_nested_empty_folder(self, gcs_hns, base_test_dir):
        """
        Test that ls works for nested empty folders (e.g., parent/child).
        Verifies both parent visibility in root and child visibility in parent.
        """
        path_parent = f"{base_test_dir}/parent_empty"
        path_child = f"{path_parent}/sub_empty"

        # Use create_parents=True to create 'parent_empty' automatically
        gcs_hns.mkdir(path_child, create_parents=True)

        # 1. Verify parent appears in root listing
        items_root = gcs_hns.ls(base_test_dir, detail=False)
        items_root_cleaned = [item.rstrip("/") for item in items_root]

        assert (
            path_parent in items_root_cleaned
        ), f"Parent folder '{path_parent}' missing from root listing"

        # 2. Verify child appears in parent listing
        items_parent = gcs_hns.ls(path_parent, detail=False)
        items_parent_cleaned = [item.rstrip("/") for item in items_parent]

        assert (
            path_child in items_parent_cleaned
        ), f"Nested child folder '{path_child}' missing from parent listing"

    def test_ls_folder_with_file(self, gcs_hns, base_test_dir):
        """
        Test that ls correctly lists a folder that exists implicitly because it contains a file.
        """
        path_parent = f"{base_test_dir}/parent_file"
        path_file = f"{path_parent}/sub_file/file.txt"
        path_sub_folder = f"{path_parent}/sub_file"

        # Uploading a file automatically creates the directory structure on HNS
        gcs_hns.touch(path_file)

        # 1. Verify parent appears in root listing
        items_root = gcs_hns.ls(base_test_dir, detail=False)
        items_root_cleaned = [item.rstrip("/") for item in items_root]

        assert (
            path_parent in items_root_cleaned
        ), f"Parent folder '{path_parent}' missing from root listing"

        # 2. Verify sub-folder appears in parent listing
        items_parent = gcs_hns.ls(path_parent, detail=False)
        items_parent_cleaned = [item.rstrip("/") for item in items_parent]

        assert (
            path_sub_folder in items_parent_cleaned
        ), f"Subfolder '{path_sub_folder}' missing from parent listing"

        # 3. Verify file appears in sub-folder listing
        items_sub = gcs_hns.ls(path_sub_folder, detail=False)
        items_sub_cleaned = [item.rstrip("/") for item in items_sub]

        assert (
            path_file in items_sub_cleaned
        ), f"File '{path_file}' missing from sub-folder listing"
