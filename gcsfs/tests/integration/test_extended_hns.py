import os
import uuid

import pytest

from gcsfs.tests.settings import TEST_HNS_BUCKET

# Skip these tests if not running against a real GCS backend
should_run_hns = os.getenv("GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT", "false").lower() in (
    "true",
    "1",
)

pytestmark = pytest.mark.skipif(
    os.environ.get("STORAGE_EMULATOR_HOST") != "https://storage.googleapis.com"
    or not should_run_hns,
    reason="This test class is for real GCS HNS buckets only and requires experimental flag.",
)


@pytest.fixture()
def test_structure(gcs_hns):
    """Sets up a standard directory structure for find tests and cleans it up afterward."""
    base_dir = f"{TEST_HNS_BUCKET}/integration-find-{uuid.uuid4()}"

    structure = {
        "base_dir": base_dir,
        "root_file": f"{base_dir}/root_file.txt",
        "empty_dir": f"{base_dir}/empty_dir",
        "dir_with_files": f"{base_dir}/dir_with_files",
        "file1": f"{base_dir}/dir_with_files/file1.txt",
        "file2": f"{base_dir}/dir_with_files/file2.txt",
        "nested_dir": f"{base_dir}/dir_with_files/nested_dir",
        "nested_file": f"{base_dir}/dir_with_files/nested_dir/nested_file.txt",
    }

    print(f"--- Setting up test structure in: {base_dir} ---")
    gcs_hns.touch(structure["root_file"])
    gcs_hns.mkdir(structure["empty_dir"])
    gcs_hns.touch(structure["file1"])
    gcs_hns.touch(structure["file2"])
    gcs_hns.touch(structure["nested_file"])
    print("--- Test structure created. ---")

    yield structure

    print(f"--- Cleaning up test structure in: {base_dir} ---")
    if gcs_hns.exists(base_dir):
        gcs_hns.rm(base_dir, recursive=True)
    print("--- Cleanup complete. ---")


class TestExtendedGcsFileSystemFindIntegration:
    """Integration tests for the find method on HNS buckets."""

    def test_find_files_only(self, gcs_hns, test_structure):
        """Test find without withdirs, which should only return files."""
        base_dir = test_structure["base_dir"]
        result = sorted(gcs_hns.find(base_dir))
        expected = sorted(
            [
                test_structure["root_file"],
                test_structure["file1"],
                test_structure["file2"],
                test_structure["nested_file"],
            ]
        )
        assert result == expected

    def test_find_withdirs(self, gcs_hns, test_structure):
        """Test find with withdirs=True, which should return files and directories."""
        base_dir = test_structure["base_dir"]
        result = sorted(gcs_hns.find(base_dir, withdirs=True))
        expected = sorted(
            [
                base_dir,
                test_structure["root_file"],
                test_structure["empty_dir"],
                test_structure["dir_with_files"],
                test_structure["file1"],
                test_structure["file2"],
                test_structure["nested_dir"],
                test_structure["nested_file"],
            ]
        )
        assert result == expected

    def test_find_with_maxdepth(self, gcs_hns, test_structure):
        """Test that find respects the maxdepth parameter."""
        base_dir = test_structure["base_dir"]
        # maxdepth=1 from base_dir should not include nested_dir or nested_file
        result = sorted(gcs_hns.find(base_dir, maxdepth=1, withdirs=True))
        expected = sorted(
            [
                base_dir,
                test_structure["root_file"],
                test_structure["empty_dir"],
                test_structure["dir_with_files"],
            ]
        )
        assert result == expected

    def test_find_with_detail(self, gcs_hns, test_structure):
        """Test that find with detail=True returns a dictionary of details."""
        base_dir = test_structure["base_dir"]
        result = gcs_hns.find(base_dir, withdirs=True, detail=True)

        assert isinstance(result, dict)
        assert test_structure["root_file"] in result
        assert test_structure["empty_dir"] in result
        assert result[test_structure["empty_dir"]]["type"] == "directory"
        assert result[test_structure["root_file"]]["type"] == "file"

    def test_find_with_prefix(self, gcs_hns, test_structure):
        """Test that find correctly filters by prefix within a directory."""
        dir_with_files = test_structure["dir_with_files"]
        # This directory contains 'file1.txt' and 'file2.txt'.
        # The prefix should only match 'file1.txt'.
        result = sorted(gcs_hns.find(dir_with_files, prefix="file1"))
        expected = [test_structure["file1"]]
        assert result == expected, "find with prefix should only return matching files."

    def test_find_on_file(self, gcs_hns, test_structure):
        """Test that calling find on a single file returns only that file."""
        file_path = test_structure["root_file"]
        result = gcs_hns.find(file_path)
        assert result == [
            file_path
        ], "find on a file path should return a list containing only that file."

    @pytest.mark.parametrize("withdirs_param", [True, False])
    def test_find_updates_dircache_without_prefix(
        self, gcs_hns, test_structure, withdirs_param
    ):
        """Test that find() populates the dircache when no prefix is given."""
        base_dir = test_structure["base_dir"]
        gcs_hns.invalidate_cache()
        assert not gcs_hns.dircache

        # Run find to populate the cache
        gcs_hns.find(base_dir, withdirs=withdirs_param)

        # Verify that the cache is now populated for the found directories
        assert base_dir in gcs_hns.dircache
        assert test_structure["dir_with_files"] in gcs_hns.dircache
        assert test_structure["nested_dir"] in gcs_hns.dircache

        # Check content of the base directory's cache
        base_dir_listing = {d["name"] for d in gcs_hns.dircache[base_dir]}
        assert test_structure["root_file"] in base_dir_listing
        assert test_structure["empty_dir"] in base_dir_listing
        assert test_structure["dir_with_files"] in base_dir_listing

        # Check content of the 'dir_with_files' cache
        dir_with_files_listing = {
            d["name"] for d in gcs_hns.dircache[test_structure["dir_with_files"]]
        }
        assert test_structure["file1"] in dir_with_files_listing
        assert test_structure["file2"] in dir_with_files_listing
        assert test_structure["nested_dir"] in dir_with_files_listing

        # Check content of the 'nested_dir' cache
        nested_dir_listing = {
            d["name"] for d in gcs_hns.dircache[test_structure["nested_dir"]]
        }
        assert test_structure["nested_file"] in nested_dir_listing

    def test_find_does_not_update_dircache_with_prefix(self, gcs_hns, test_structure):
        """Test that find() does NOT populate the dircache when a prefix is given."""
        base_dir = test_structure["base_dir"]
        gcs_hns.invalidate_cache()
        assert not gcs_hns.dircache

        # find with a prefix should not update the cache, as it's a partial listing
        gcs_hns.find(base_dir, prefix="root_")

        assert (
            not gcs_hns.dircache
        ), "dircache should not be updated when using a prefix"
