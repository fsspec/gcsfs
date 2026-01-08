import os
import uuid
import pytest
from gcsfs.tests.settings import TEST_HNS_BUCKET

# Ensure tests run only when the experimental flag is set and we are on real GCS
should_run_hns = os.getenv("GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT",
                           "false").lower() in (
                     "true",
                     "1",
                 )

pytestmark = pytest.mark.skipif(
    os.environ.get("STORAGE_EMULATOR_HOST") != "https://storage.googleapis.com"
    or not should_run_hns,
    reason="This test class is for real GCS HNS buckets only and requires experimental flag.",
)


class TestExtendedGcsFileSystemLsIntegration:
  """Integration tests for ls method edge cases on HNS buckets."""

  def test_hns_ls_edge_cases(self, gcs_hns):
    """
    Test ls behavior with empty folders, nested empty folders, and populated subfolders.
    """
    # Unique prefix to isolate this test run
    unique_id = uuid.uuid4().hex
    base_dir = f"{TEST_HNS_BUCKET}/integration_ls_{unique_id}"

    # Define edge case paths
    path_empty_folder = f"{base_dir}/empty_folder"
    path_parent_empty = f"{base_dir}/parent_empty"
    path_nested_empty = f"{path_parent_empty}/sub_empty"
    path_parent_file = f"{base_dir}/parent_file"
    path_file_inside = f"{path_parent_file}/sub_file/file.txt"

    try:
      # 1. Create an empty folder explicitly
      gcs_hns.mkdir(path_empty_folder)

      # 2. Create a nested empty folder (creates parent implicitly or explicitly)
      gcs_hns.mkdir(path_nested_empty)

      # 3. Create a subfolder containing a file (implicit folder creation)
      gcs_hns.touch(path_file_inside)

      # --- Verification ---

      # A. List the root directory
      # Expected: empty_folder, parent_empty, parent_file
      items_root = gcs_hns.ls(base_dir, detail=False)

      # Normalize items (gcsfs usually returns paths without trailing slash)
      items_root_cleaned = [item.rstrip('/') for item in items_root]

      assert path_empty_folder in items_root_cleaned, \
        f"Empty folder '{path_empty_folder}' missing from root listing"
      assert path_parent_empty in items_root_cleaned, \
        f"Parent of empty subfolder '{path_parent_empty}' missing from root listing"
      assert path_parent_file in items_root_cleaned, \
        f"Parent of populated subfolder '{path_parent_file}' missing from root listing"

      # B. List the parent of the nested empty folder
      # Expected: sub_empty
      items_nested = gcs_hns.ls(path_parent_empty, detail=False)
      items_nested_cleaned = [item.rstrip('/') for item in items_nested]
      assert path_nested_empty in items_nested_cleaned, \
        f"Nested empty folder '{path_nested_empty}' missing from ls"

      # C. List the parent of the file structure
      # Expected: sub_file
      items_file_parent = gcs_hns.ls(path_parent_file, detail=False)
      items_file_parent_cleaned = [item.rstrip('/') for item in
                                   items_file_parent]
      expected_sub_folder = f"{path_parent_file}/sub_file"
      assert expected_sub_folder in items_file_parent_cleaned, \
        f"Subfolder containing file '{expected_sub_folder}' missing from ls"

    finally:
      # Cleanup
      if gcs_hns.exists(base_dir):
        try:
          gcs_hns.rm(base_dir, recursive=True)
        except Exception:
          pass
