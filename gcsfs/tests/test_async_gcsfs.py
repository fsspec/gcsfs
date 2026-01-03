import pytest
import pytest_asyncio
import os
import uuid
from gcsfs import GCSFileSystem
from gcsfs.tests.settings import TEST_ZONAL_BUCKET
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
async def test_async_pipe_and_cat(gcs):
    """Test async _pipe_file and _cat_file."""
    filename = f"test_async_pipe_cat_{uuid.uuid4().hex}"
    path = f"{TEST_ZONAL_BUCKET}/{filename}"
    data = b"async data content"
    assert isinstance(gcs, ExtendedGcsFileSystem)

    try:
        # Write data
        await gcs._pipe_file(path, data, finalize_on_close=True)

        # Read data
        result = await gcs._cat_file(path)
        assert result == data

        # Verify info
        info = await gcs._info(path)
        assert info["type"] == "file"
        assert info["size"] == len(data)

    finally:
        try:
            await gcs._rm_file(path)
        except Exception:
            pass

@pytest.mark.asyncio
async def test_async_put(gcs, tmp_path):
    """Test async _put_file."""
    filename = f"test_async_put_{uuid.uuid4().hex}"
    remote_path = f"{TEST_ZONAL_BUCKET}/{filename}"
    local_file_in = tmp_path / "input.txt"
    
    data = b"file data for put"
    local_file_in.write_bytes(data)

    try:
        # Upload
        await gcs._put_file(str(local_file_in), remote_path, finalize_on_close=True)

        # Verify
        assert await gcs._cat_file(remote_path) == data

    finally:
        try:
            await gcs._rm_file(remote_path)
        except Exception:
            pass

@pytest.mark.asyncio
async def test_async_ls(gcs):
    """Test async _ls."""
    dirname = f"test_async_ls_{uuid.uuid4().hex}"
    prefix = f"{TEST_ZONAL_BUCKET}/{dirname}"
    file1 = f"{prefix}/f1"
    file2 = f"{prefix}/f2"

    try:
        await gcs._pipe_file(file1, b"1")
        await gcs._pipe_file(file2, b"2")

        files = await gcs._ls(prefix)
        
        expected_file1 = f"{TEST_ZONAL_BUCKET}/{dirname}/f1"
        expected_file2 = f"{TEST_ZONAL_BUCKET}/{dirname}/f2"
        
        assert expected_file1 in files
        assert expected_file2 in files

    finally:
        try:
            await gcs._rm(prefix, recursive=True)
        except Exception:
            pass

@pytest.mark.asyncio
async def test_async_rm(gcs):
    """Test async _rm_file."""
    filename = f"test_async_rm_{uuid.uuid4().hex}"
    path1 = f"{TEST_ZONAL_BUCKET}/{filename}"
    data = b"delete me"

    try:
        await gcs._pipe_file(path1, data)
        
        # Remove
        await gcs._rm_file(path1)
        
        # Verify removal
        with pytest.raises(FileNotFoundError):
            await gcs._info(path1)

    finally:
        try:
            await gcs._rm_file(path1)
        except Exception:
            pass
