import os
import uuid

import pytest

from gcsfs.tests.settings import TEST_HNS_BUCKET

# Ensure tests run only when the experimental flag is set and we are on real GCS
should_run_hns = os.getenv("GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT", "false").lower() in (
    "true",
    "1",
)

pytestmark = pytest.mark.skipif(
    os.environ.get("STORAGE_EMULATOR_HOST") != "https://storage.googleapis.com"
    or not should_run_hns,
    reason="This test class is for real GCS HNS buckets only and requires experimental flag.",
)


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
