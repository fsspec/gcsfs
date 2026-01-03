import os

import pytest
import pytest_asyncio

from gcsfs import GCSFileSystem
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


@pytest_asyncio.fixture
async def gcs():
    """Fixture to provide an asynchronous GCSFileSystem instance."""
    gcs = GCSFileSystem(asynchronous=True, skip_instance_cache=True)
    yield gcs
    if gcs.session and not gcs.session.closed:
        await gcs.session.close()


@pytest.mark.asyncio
async def test_async_pipe_and_cat(gcs, file_path):
    """Test async _pipe_file and _cat_file."""
    data = b"async data content"
    assert isinstance(gcs, ExtendedGcsFileSystem)

    # Write data
    await gcs._pipe_file(file_path, data, finalize_on_close=True)

    # Read data
    result = await gcs._cat_file(file_path)
    assert result == data

    # Verify info
    info = await gcs._info(file_path)
    assert info["type"] == "file"
    assert info["size"] == len(data)


@pytest.mark.asyncio
async def test_async_put(gcs, tmp_path, file_path):
    """Test async _put_file."""
    local_file_in = tmp_path / "input.txt"

    data = b"file data for put"
    local_file_in.write_bytes(data)

    # Upload
    await gcs._put_file(str(local_file_in), file_path, finalize_on_close=True)

    # Verify
    assert await gcs._cat_file(file_path) == data


@pytest.mark.asyncio
async def test_async_ls(gcs, file_path):
    """Test async _ls."""
    prefix = file_path
    file1 = f"{prefix}/f1"
    file2 = f"{prefix}/f2"

    await gcs._pipe_file(file1, b"1")
    await gcs._pipe_file(file2, b"2")

    files = await gcs._ls(prefix)

    expected_file1 = f"{prefix}/f1"
    expected_file2 = f"{prefix}/f2"

    assert expected_file1 in files
    assert expected_file2 in files


@pytest.mark.asyncio
async def test_async_rm(gcs, file_path):
    """Test async _rm_file."""
    data = b"delete me"

    await gcs._pipe_file(file_path, data)

    # Remove
    await gcs._rm_file(file_path)

    # Verify removal
    with pytest.raises(FileNotFoundError):
        await gcs._info(file_path)
