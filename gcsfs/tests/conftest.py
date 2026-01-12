import logging
import os
import shlex
import subprocess
import time
import uuid
from unittest import mock

import fsspec
import pytest
import pytest_asyncio
import requests
from google.cloud import storage
from google.cloud.storage._experimental.asyncio.async_appendable_object_writer import (
    AsyncAppendableObjectWriter,
)

from gcsfs import GCSFileSystem
from gcsfs.extended_gcsfs import BucketType
from gcsfs.tests.settings import (
    TEST_BUCKET,
    TEST_HNS_BUCKET,
    TEST_VERSIONED_BUCKET,
    TEST_ZONAL_BUCKET,
)

files = {
    "test/accounts.1.json": (
        b'{"amount": 100, "name": "Alice"}\n'
        b'{"amount": 200, "name": "Bob"}\n'
        b'{"amount": 300, "name": "Charlie"}\n'
        b'{"amount": 400, "name": "Dennis"}\n'
    ),
    "test/accounts.2.json": (
        b'{"amount": 500, "name": "Alice"}\n'
        b'{"amount": 600, "name": "Bob"}\n'
        b'{"amount": 700, "name": "Charlie"}\n'
        b'{"amount": 800, "name": "Dennis"}\n'
    ),
}

csv_files = {
    "2014-01-01.csv": (
        b"name,amount,id\n" b"Alice,100,1\n" b"Bob,200,2\n" b"Charlie,300,3\n"
    ),
    "2014-01-02.csv": b"name,amount,id\n",
    "2014-01-03.csv": (
        b"name,amount,id\n" b"Dennis,400,4\n" b"Edith,500,5\n" b"Frank,600,6\n"
    ),
}
text_files = {
    "nested/file1": b"hello\n",
    "nested/file2": b"world",
    "nested/nested2/file1": b"hello\n",
    "nested/nested2/file2": b"world",
    "zonal/test/a": b"a,b\n11,22\n3,4",
    "zonal/test/b": b"",
    "zonal/test/c": b"ab\n" + b"a" * (2**18) + b"\nab",
}

_MULTI_THREADED_TEST_DATA_SIZE = 5 * 1024 * 1024  # 5MB
pattern = b"0123456789abcdef"
text_files["multi_threaded_test_file"] = (
    pattern * (_MULTI_THREADED_TEST_DATA_SIZE // len(pattern))
    + pattern[: _MULTI_THREADED_TEST_DATA_SIZE % len(pattern)]
)

allfiles = dict(**files, **csv_files, **text_files)
a = TEST_BUCKET + "/tmp/test/a"
b = TEST_BUCKET + "/tmp/test/b"
c = TEST_BUCKET + "/tmp/test/c"
d = TEST_BUCKET + "/tmp/test/d"

params = dict()

BUCKET_NAME_MAP = {
    "regional": TEST_BUCKET,
    "zonal": TEST_ZONAL_BUCKET,
    "hns": TEST_HNS_BUCKET,
}


def stop_docker(container):
    cmd = shlex.split('docker ps -a -q --filter "name=%s"' % container)
    cid = subprocess.check_output(cmd).strip().decode()
    if cid:
        subprocess.call(["docker", "rm", "-f", "-v", cid])


@pytest.fixture(scope="session")
def docker_gcs():
    if "STORAGE_EMULATOR_HOST" in os.environ:
        # assume using real API or otherwise have a server already set up
        yield os.getenv("STORAGE_EMULATOR_HOST")
        return
    params["token"] = "anon"
    container = "gcsfs_test"
    cmd = (
        "docker run -d -p 4443:4443 --name gcsfs_test fsouza/fake-gcs-server:latest -scheme "
        "http -public-host 0.0.0.0:4443 -external-url http://localhost:4443 "
        "-backend memory"
    )
    stop_docker(container)
    subprocess.check_output(shlex.split(cmd))
    url = "http://0.0.0.0:4443"
    timeout = 10
    while True:
        try:
            r = requests.get(url + "/storage/v1/b")
            if r.ok:
                yield url
                break
        except Exception as e:  # noqa: E722
            timeout -= 1
            if timeout < 0:
                raise SystemError from e
            time.sleep(1)
    stop_docker(container)


@pytest.fixture(scope="session")
def gcs_factory(docker_gcs):
    params["endpoint_url"] = docker_gcs

    def factory(**kwargs):
        GCSFileSystem.clear_instance_cache()
        return fsspec.filesystem("gcs", **params, **kwargs)

    return factory


@pytest.fixture(scope="session")
def buckets_to_delete():
    """
    Provides a session-scoped set to track the names of GCS buckets that are
    created by the test suite.

    When tests run, they may create new GCS buckets. If these buckets are not
    deleted, they will persist after the test run, leading to resource leakage.
    This set acts as a registry of buckets that the `final_cleanup` fixture
    should remove at the end of the entire test session.
    """
    return set()


@pytest.fixture
def gcs(gcs_factory, buckets_to_delete, populate=True):
    gcs = gcs_factory()
    try:  # ensure we're empty.
        # Create the bucket if it doesn't exist, otherwise clean it.
        if not gcs.exists(TEST_BUCKET):
            gcs.mkdir(TEST_BUCKET)
            # By adding the bucket name to this set, we are marking it for
            # deletion at the end of the test session. This ensures that if
            # the test suite creates the bucket, it will also be responsible
            # for deleting it. If the bucket already existed, we assume it's
            # managed externally and should not be deleted by the tests.
            buckets_to_delete.add(TEST_BUCKET)
        else:
            _cleanup_gcs(gcs, bucket=TEST_BUCKET)

        if populate:
            gcs.pipe({TEST_BUCKET + "/" + k: v for k, v in allfiles.items()})
        gcs.invalidate_cache()
        yield gcs
    finally:
        _cleanup_gcs(gcs)


@pytest.fixture
def extended_gcs_factory(gcs_factory, buckets_to_delete, populate=True):
    created_instances = []

    def factory(**kwargs):
        fs = _create_extended_gcsfs(gcs_factory, buckets_to_delete, populate, **kwargs)
        created_instances.append(fs)
        return fs

    yield factory

    for fs in created_instances:
        _cleanup_gcs(fs)


@pytest.fixture
def extended_gcsfs(gcs_factory, buckets_to_delete, populate=True):
    extended_gcsfs = _create_extended_gcsfs(gcs_factory, buckets_to_delete, populate)
    try:
        yield extended_gcsfs
    finally:
        _cleanup_gcs(extended_gcsfs)


def _cleanup_gcs(gcs, bucket=TEST_BUCKET):
    """Clean the bucket contents, logging a warning on failure."""
    try:
        if gcs.exists(bucket):
            files_to_delete = gcs.find(bucket, withdirs=True)
            if files_to_delete:
                gcs.rm(files_to_delete)
    except Exception as e:
        logging.warning(f"Failed to clean up GCS bucket {bucket}: {e}")


@pytest.fixture(scope="session", autouse=True)
def final_cleanup(gcs_factory, buckets_to_delete):
    """
    A session-scoped, auto-use fixture that deletes all buckets registered
    in the `buckets_to_delete` set after the entire test session is complete.
    """
    yield
    # This code runs after the entire test session finishes

    gcs = gcs_factory()
    for bucket in buckets_to_delete:
        # The cleanup logic attempts to delete every bucket that was
        # added to the set during the session. For real GCS, only delete if
        # created by the test suite.
        try:
            if gcs.exists(bucket):
                gcs.rm(bucket, recursive=True)
                logging.info(f"Cleaned up bucket: {bucket}")
        except Exception as e:
            logging.warning(f"Failed to perform final cleanup for bucket {bucket}: {e}")


@pytest.fixture
def gcs_versioned(gcs_factory, buckets_to_delete):
    gcs = gcs_factory()
    gcs.version_aware = True
    is_real_gcs = (
        os.environ.get("STORAGE_EMULATOR_HOST") == "https://storage.googleapis.com"
    )
    try:  # ensure we're empty.
        # The versioned bucket might be created by `is_versioning_enabled`
        # in test_core_versioned.py. We must register it for cleanup only if
        # it was created by this test run.
        try:
            from gcsfs.tests.test_core_versioned import (
                _VERSIONED_BUCKET_CREATED_BY_TESTS,
            )

            if _VERSIONED_BUCKET_CREATED_BY_TESTS:
                buckets_to_delete.add(TEST_VERSIONED_BUCKET)
        except ImportError:
            pass  # test_core_versioned is not being run
        if is_real_gcs:
            cleanup_versioned_bucket(gcs, TEST_VERSIONED_BUCKET)
        else:
            # For emulators, we delete and recreate the bucket for a clean state
            try:
                gcs.rm(TEST_VERSIONED_BUCKET, recursive=True)
            except FileNotFoundError:
                pass
            gcs.mkdir(TEST_VERSIONED_BUCKET, enable_versioning=True)
            buckets_to_delete.add(TEST_VERSIONED_BUCKET)
        gcs.invalidate_cache()
        yield gcs
    finally:
        # Ensure the bucket is empty after the test.
        try:
            if is_real_gcs:
                cleanup_versioned_bucket(gcs, TEST_VERSIONED_BUCKET)
        except Exception as e:
            logging.warning(
                f"Failed to clean up versioned bucket {TEST_VERSIONED_BUCKET} after test: {e}"
            )


def cleanup_versioned_bucket(gcs, bucket_name, prefix=None):
    """
    Deletes all object versions in a bucket using the google-cloud-storage client,
    ensuring it uses the same credentials as the gcsfs instance.
    """
    # Define a retry policy for API calls to handle rate limiting.
    # This can retry on 429 Too Many Requests errors, which can happen
    # when deleting many object versions quickly.
    from google.api_core.retry import Retry

    retry_policy = Retry(
        initial=1.0,  # Initial delay in seconds
        maximum=30.0,  # Maximum delay in seconds
        multiplier=1.2,  # Backoff factor
    )

    client = storage.Client(
        credentials=gcs.credentials.credentials, project=gcs.project
    )

    # List all blobs, including old versions
    blobs_to_delete = list(client.list_blobs(bucket_name, versions=True, prefix=prefix))

    if not blobs_to_delete:
        logging.info("No object versions to delete in %s.", bucket_name)
        return

    logging.info(
        "Deleting %d object versions from %s.", len(blobs_to_delete), bucket_name
    )
    time.sleep(2)
    for blob in blobs_to_delete:
        blob.delete(retry=retry_policy)

    logging.info("Successfully deleted %d object versions.", len(blobs_to_delete))


def _create_extended_gcsfs(gcs_factory, buckets_to_delete, populate=True, **kwargs):
    is_real_gcs = (
        os.environ.get("STORAGE_EMULATOR_HOST") == "https://storage.googleapis.com"
    )

    extended_gcsfs = gcs_factory(**kwargs)
    # Only create/delete/populate the bucket if we are NOT using the real GCS endpoint.
    if not is_real_gcs:
        try:
            extended_gcsfs.rm(TEST_ZONAL_BUCKET, recursive=True)
        except FileNotFoundError:
            pass
        extended_gcsfs.mkdir(TEST_ZONAL_BUCKET)
        buckets_to_delete.add(TEST_ZONAL_BUCKET)
    try:
        if populate:
            # To avoid hitting object mutation limits, only pipe files if they
            # don't exist or if their size has changed.
            existing_files = extended_gcsfs.find(TEST_ZONAL_BUCKET, detail=True)
            files_to_pipe = {}
            for k, v in allfiles.items():
                remote_path = f"{TEST_ZONAL_BUCKET}/{k}"
                if remote_path not in existing_files or existing_files[remote_path][
                    "size"
                ] != len(v):
                    files_to_pipe[remote_path] = v

            if files_to_pipe:
                extended_gcsfs.pipe(files_to_pipe, finalize_on_close=True)
    except Exception as e:
        logging.warning(f"Failed to populate Zonal bucket: {e}")

    extended_gcsfs.invalidate_cache()
    return extended_gcsfs


@pytest.fixture
def gcs_hns(gcs_factory, buckets_to_delete):
    """
    Provides a GCSFileSystem instance pointed at a HNS-enabled bucket.

    - Creates the bucket if it doesn't exist.
    - Cleans the bucket before the test.
    - Yields the filesystem instance.
    - Cleans the bucket after the test.
    """
    # TODO: Re-use _create_extended_gcsfs once cleanup for real_gcs is added to it
    gcs = gcs_factory()

    try:
        if not gcs.exists(TEST_HNS_BUCKET):
            # Note: Emulators may not fully support HNS features like real GCS.
            gcs.mkdir(TEST_HNS_BUCKET, enable_hierarchial_namespace=True)
            buckets_to_delete.add(TEST_HNS_BUCKET)
        else:
            _cleanup_gcs(gcs, bucket=TEST_HNS_BUCKET)
        gcs.invalidate_cache()
        yield gcs
    finally:
        _cleanup_gcs(gcs, bucket=TEST_HNS_BUCKET)


@pytest.fixture
def zonal_write_mocks():
    """A fixture for mocking Zonal bucket write functionality."""

    if os.environ.get("STORAGE_EMULATOR_HOST") == "https://storage.googleapis.com":
        yield None
        return

    patch_target_get_bucket_type = (
        "gcsfs.extended_gcsfs.ExtendedGcsFileSystem._get_bucket_type"
    )
    patch_target_init_aaow = "gcsfs.zb_hns_utils.init_aaow"
    patch_target_gcsfs_info = "gcsfs.core.GCSFileSystem._info"

    mock_aaow = mock.AsyncMock(spec=AsyncAppendableObjectWriter)
    mock_aaow.offset = 0
    mock_aaow._is_stream_open = True
    mock_init_aaow = mock.AsyncMock(return_value=mock_aaow)
    mock_gcsfs_info = mock.AsyncMock(
        return_value={"generation": "12345", "type": "file", "name": "mock_file"}
    )

    async def append_side_effect(data):
        mock_aaow.offset += len(data)

    mock_aaow.append.side_effect = append_side_effect

    async def close_side_effect(finalize_on_close=False):
        mock_aaow._is_stream_open = False

    mock_aaow.close.side_effect = close_side_effect

    # Finalize closes the stream as well
    async def finalize_side_effect():
        mock_aaow._is_stream_open = False

    mock_aaow.finalize.side_effect = finalize_side_effect

    with (
        mock.patch(
            patch_target_get_bucket_type,
            return_value=BucketType.ZONAL_HIERARCHICAL,
        ),
        mock.patch(patch_target_gcsfs_info, mock_gcsfs_info),
        mock.patch(patch_target_init_aaow, mock_init_aaow),
    ):
        mocks = {
            "aaow": mock_aaow,
            "init_aaow": mock_init_aaow,
            "_gcsfs_info": mock_gcsfs_info,
        }
        yield mocks


@pytest.fixture
def file_path():
    """Generates a unique test file path for every test."""
    path = f"{TEST_ZONAL_BUCKET}/zonal-test-{uuid.uuid4()}"
    yield path


@pytest_asyncio.fixture
async def async_gcs():
    """Fixture to provide an asynchronous GCSFileSystem instance."""
    token = "anon" if not os.getenv("STORAGE_EMULATOR_HOST") else None
    GCSFileSystem.clear_instance_cache()
    gcs = GCSFileSystem(asynchronous=True, token=token)
    yield gcs
