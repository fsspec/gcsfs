import os
import uuid

import pytest

from gcsfs.extended_gcsfs import ExtendedGcsFileSystem
from gcsfs.tests.settings import TEST_BUCKET, TEST_ZONAL_BUCKET

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
        os.environ.get("STORAGE_EMULATOR_HOST") != "https://storage.googleapis.com",
        reason="Skipping tests on emulator, requires real GCS.",
    ),
]


@pytest.mark.asyncio
async def test_async_pipe_and_cat(async_gcs, file_path):
    """Test async _pipe_file and _cat_file."""
    data = b"async data content"
    assert isinstance(async_gcs, ExtendedGcsFileSystem)

    # Write data
    await async_gcs._pipe_file(file_path, data, finalize_on_close=True)

    # Read data
    result = await async_gcs._cat_file(file_path)
    assert result == data

    # Verify info
    info = await async_gcs._info(file_path)
    assert info["type"] == "file"
    assert info["size"] == len(data)


@pytest.mark.asyncio
async def test_async_put(async_gcs, tmp_path, file_path):
    """Test async _put_file."""
    local_file_in = tmp_path / "input.txt"

    data = b"file data for put"
    local_file_in.write_bytes(data)

    # Upload
    await async_gcs._put_file(str(local_file_in), file_path, finalize_on_close=True)

    # Verify
    assert await async_gcs._cat_file(file_path) == data


@pytest.mark.asyncio
async def test_async_get(async_gcs, tmp_path, file_path):
    """Test async _get_file."""
    local_file_out = tmp_path / "output.txt"
    data = b"file data for get"
    await async_gcs._pipe_file(file_path, data)

    # Download
    await async_gcs._get_file(file_path, str(local_file_out))

    # Verify
    assert local_file_out.read_bytes() == data


@pytest.mark.asyncio
async def test_get_file_to_directory(async_gcs, tmp_path, file_path):
    """
    Tests that _get_file does nothing if the local path is a directory.
    """
    ldir = tmp_path / "output_dir"
    ldir.mkdir()
    await async_gcs._pipe_file(file_path, b"some data", finalize_on_close=True)

    # Call _get_file with a directory as the local path
    await async_gcs._get_file(file_path, str(ldir))

    # Check that no file was created inside the directory
    assert not list(ldir.iterdir())


@pytest.mark.asyncio
async def test_async_ls(async_gcs, file_path):
    """Test async _ls."""
    prefix = file_path
    file1 = f"{prefix}/f1"
    file2 = f"{prefix}/f2"

    await async_gcs._pipe_file(file1, b"1")
    await async_gcs._pipe_file(file2, b"2")

    files = await async_gcs._ls(prefix)

    expected_file1 = f"{prefix}/f1"
    expected_file2 = f"{prefix}/f2"

    assert expected_file1 in files
    assert expected_file2 in files


@pytest.mark.asyncio
async def test_async_rm(async_gcs, file_path):
    """Test async _rm_file."""
    data = b"delete me"

    await async_gcs._pipe_file(file_path, data)

    # Remove
    await async_gcs._rm_file(file_path)

    # Verify removal
    with pytest.raises(FileNotFoundError):
        await async_gcs._info(file_path)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "file_contents, range_requests, expected_results, batch_size",
    [
        # Case 1: Single file, single range
        ([b"0123456789"], [(0, 0, 4)], [b"0123"], None),
        # Case 2: Single file, multiple ranges (Standard + Zero-length boundary)
        (
            [b"0123456789"],
            [(0, 0, 4), (0, 5, 5), (0, 8, 10)],
            [b"0123", b"", b"89"],
            None,
        ),
        # Case 3: Multiple files (f0, f1), single range from each
        ([b"AAAAA", b"BBBBB"], [(0, 0, 2), (1, 0, 2)], [b"AA", b"BB"], None),
        # Case 4: Batch size limits (forces multiple internal calls)
        ([b"test data"], [(0, 0, 4), (0, 5, 9)], [b"test", b"data"], 1),
    ],
    ids=["single_read", "multi_read_with_boundary", "multi_file", "batched_read"],
)
async def test_cat_ranges_unified(
    async_gcs, file_path, file_contents, range_requests, expected_results, batch_size
):
    """
    Unified happy-path test for _cat_ranges.
    range_requests is a list of tuples: (file_index_in_contents_list, start, end).
    """
    # Setup: Create all necessary files
    filenames = []
    for i, content in enumerate(file_contents):
        fname = f"{file_path}/f{i}"
        await async_gcs._pipe_file(fname, content, finalize_on_close=True)
        filenames.append(fname)

    # Build inputs: Map indices (0, 1) to actual filenames
    paths = [filenames[idx] for idx, _, _ in range_requests]
    starts = [s for _, s, _ in range_requests]
    ends = [e for _, _, e in range_requests]

    # Execute and assert
    results = await async_gcs._cat_ranges(paths, starts, ends, batch_size=batch_size)

    assert results == expected_results


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "path_input, starts, ends, max_gap, error_type, error_match",
    [
        ("NOT_A_LIST", [0], [1], None, TypeError, "paths must be a list of file paths"),
        (
            ["f1", "f2"],
            [0],
            [1],
            None,
            ValueError,
            "starts, ends, and paths must have the same length",
        ),
        (
            ["f1"],
            [0, 1],
            [1],
            None,
            ValueError,
            "starts, ends, and paths must have the same length",
        ),
        (["f1"], [0], [1], 10, NotImplementedError, "max_gap is not supported"),
    ],
    ids=[
        "type_error",
        "length_mismatch_paths",
        "length_mismatch_starts",
        "max_gap_error",
    ],
)
async def test_cat_ranges_validation(
    async_gcs, path_input, starts, ends, max_gap, error_type, error_match
):
    """Test input validation errors."""
    with pytest.raises(error_type, match=error_match):
        await async_gcs._cat_ranges(path_input, starts, ends, max_gap=max_gap)


@pytest.mark.asyncio
async def test_cat_ranges_negative_batch_size(async_gcs, file_path):
    """Test that negative batch_size raises ValueError."""
    await async_gcs._pipe_file(file_path, b"data", finalize_on_close=True)

    with pytest.raises(
        ValueError, match="batch_size must be a positive integer or None."
    ):
        await async_gcs._cat_ranges([file_path], [0], [4], batch_size=-1)


@pytest.mark.asyncio
async def test_cat_ranges_mixed_zonal_and_regional(async_gcs):
    """Integration test: ensuring Zonal and Regional buckets work in the same call."""
    # Setup
    z_file = f"{TEST_ZONAL_BUCKET}/z-{uuid.uuid4()}"
    r_file = f"{TEST_BUCKET}/r-{uuid.uuid4()}"

    await async_gcs._pipe_file(z_file, b"ZonalData", finalize_on_close=True)
    await async_gcs._pipe_file(r_file, b"RegionalData", finalize_on_close=True)

    # Execute: Request "Zonal" (0-5) and "Regional" (0-8)
    results = await async_gcs._cat_ranges([z_file, r_file], [0, 0], [5, 8])

    # Assert
    assert results == [b"Zonal", b"Regional"]


@pytest.mark.asyncio
@pytest.mark.parametrize("on_error", ["return", "raise"])
async def test_cat_ranges_runtime_errors(async_gcs, file_path, on_error):
    """Test handling of runtime errors (e.g., File Not Found) during execution."""
    # Setup: Create one valid file
    valid_file = f"{file_path}/valid"
    await async_gcs._pipe_file(valid_file, b"valid_data", finalize_on_close=True)

    # Define paths: Valid, Invalid (Missing), Valid
    paths = [valid_file, f"{file_path}/missing_file", valid_file]
    starts = [0, 0, 0]
    ends = [5, 5, 5]

    if on_error == "raise":
        # Expect the entire operation to fail
        with pytest.raises(
            Exception
        ):  # usually FileNotFoundError or Google Cloud Error
            await async_gcs._cat_ranges(paths, starts, ends, on_error=on_error)
    else:
        # Expect results mixed with exceptions
        results = await async_gcs._cat_ranges(paths, starts, ends, on_error=on_error)

        assert len(results) == 3
        assert results[0] == b"valid"
        assert isinstance(
            results[1], Exception
        )  # The missing file should be an Exception object
        assert results[2] == b"valid"
