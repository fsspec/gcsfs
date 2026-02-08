import os
import uuid

import pytest
import pytest_asyncio

from gcsfs.extended_gcsfs import ExtendedGcsFileSystem
from gcsfs.tests.settings import TEST_HNS_BUCKET

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
async def hns_file_path(async_gcs):
    """Generates a unique test file path in HNS bucket for every test and cleans up."""
    path = f"{TEST_HNS_BUCKET}/async-hns-test-{uuid.uuid4()}"
    yield path
    try:
        await async_gcs._rm(path, recursive=True)
    except FileNotFoundError:
        pass


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
async def test_async_mkdir_and_rmdir(async_gcs, hns_file_path):
    """Test async _mkdir and _rmdir."""
    dir_path = f"{hns_file_path}/dir"

    # mkdir
    await async_gcs._mkdir(dir_path, create_parents=True)

    # check exists
    assert await async_gcs._exists(dir_path)
    info = await async_gcs._info(dir_path)
    assert info["type"] == "directory"

    # rmdir
    await async_gcs._rmdir(dir_path)

    # verify gone
    assert not await async_gcs._exists(dir_path)


@pytest.mark.asyncio
async def test_async_mv(async_gcs, hns_file_path):
    """Test async _mv on a file and a directory."""
    # 1. Move file
    src = f"{hns_file_path}/src"
    dst = f"{hns_file_path}/dst"

    await async_gcs._pipe_file(src, b"data")
    await async_gcs._mv(src, dst)

    assert not await async_gcs._exists(src)
    assert await async_gcs._exists(dst)
    assert await async_gcs._cat_file(dst) == b"data"

    # 2. Move directory (HNS specific)
    src_dir = f"{hns_file_path}/src_dir"
    dst_dir = f"{hns_file_path}/dst_dir"
    src_file = f"{src_dir}/file"
    dst_file = f"{dst_dir}/file"

    await async_gcs._mkdir(src_dir)
    await async_gcs._pipe_file(src_file, b"data")
    await async_gcs._mv(src_dir, dst_dir, recursive=True)

    assert not await async_gcs._exists(src_dir)
    assert await async_gcs._exists(dst_dir)
    assert await async_gcs._exists(dst_file)
    assert await async_gcs._cat_file(dst_file) == b"data"


@pytest.mark.asyncio
async def test_async_find(async_gcs, hns_file_path):
    """Test async _find."""
    base = f"{hns_file_path}/find"
    f1 = f"{base}/file1"
    f2 = f"{base}/sub/file2"

    await async_gcs._mkdir(base, create_parents=True)
    await async_gcs._pipe_file(f1, b"1")
    await async_gcs._pipe_file(f2, b"2")

    out = await async_gcs._find(base)
    assert f1 in out
    assert f2 in out


@pytest.mark.asyncio
async def test_async_info(async_gcs, hns_file_path):
    """Test async _info."""
    file_path = f"{hns_file_path}/file"
    await async_gcs._pipe_file(file_path, b"data")

    info = await async_gcs._info(file_path)
    assert info["name"] == file_path

    bucket, _, _ = async_gcs.split_path(file_path)
    if not await async_gcs._is_zonal_bucket(bucket):
        assert info["size"] == 4
    assert info["type"] == "file"


@pytest.mark.asyncio
async def test_async_rm_recursive(async_gcs, hns_file_path):
    """Test async _rm recursive."""
    base = f"{hns_file_path}/rm"
    f1 = f"{base}/file1"
    await async_gcs._pipe_file(f1, b"1")

    await async_gcs._rm(base, recursive=True)

    assert not await async_gcs._exists(base)
