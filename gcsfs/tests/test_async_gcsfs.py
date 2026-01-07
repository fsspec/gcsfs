import os

import pytest

from gcsfs.extended_gcsfs import ExtendedGcsFileSystem

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
