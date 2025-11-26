import logging
import os
import shlex
import subprocess
import time
from contextlib import nullcontext
from unittest.mock import patch

import fsspec
import pytest
import requests
from google.cloud import storage

from gcsfs import GCSFileSystem
from gcsfs.tests.settings import TEST_BUCKET, TEST_VERSIONED_BUCKET

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
}
allfiles = dict(**files, **csv_files, **text_files)
a = TEST_BUCKET + "/tmp/test/a"
b = TEST_BUCKET + "/tmp/test/b"
c = TEST_BUCKET + "/tmp/test/c"
d = TEST_BUCKET + "/tmp/test/d"

params = dict()


def stop_docker(container):
    cmd = shlex.split('docker ps -a -q --filter "name=%s"' % container)
    cid = subprocess.check_output(cmd).strip().decode()
    if cid:
        subprocess.call(["docker", "rm", "-f", "-v", cid])


@pytest.fixture(scope="module")
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


@pytest.fixture
def gcs_factory(docker_gcs):
    params["endpoint_url"] = docker_gcs

    def factory(**kwargs):
        GCSFileSystem.clear_instance_cache()
        return fsspec.filesystem("gcs", **params, **kwargs)

    return factory


@pytest.fixture
def gcs(gcs_factory, populate=True):
    gcs = gcs_factory()
    try:
        # ensure we're empty.
        try:
            gcs.rm(TEST_BUCKET, recursive=True)
        except FileNotFoundError:
            pass
        try:
            gcs.mkdir(TEST_BUCKET)
        except Exception:
            pass

        if populate:
            gcs.pipe({TEST_BUCKET + "/" + k: v for k, v in allfiles.items()})
        gcs.invalidate_cache()
        yield gcs
    finally:
        try:
            gcs.rm(gcs.find(TEST_BUCKET))
            gcs.rm(TEST_BUCKET)
        except:  # noqa: E722
            pass


def _cleanup_gcs(gcs, is_real_gcs):
    """Only remove the bucket/contents if we are NOT using the real GCS, logging a warning on failure."""
    if is_real_gcs:
        return
    try:
        gcs.rm(TEST_BUCKET, recursive=True)
    except Exception as e:
        logging.warning(f"Failed to clean up GCS bucket {TEST_BUCKET}: {e}")


@pytest.fixture
def extended_gcsfs(gcs_factory, populate=True):
    # Check if we are running against a real GCS endpoint
    is_real_gcs = (
        os.environ.get("STORAGE_EMULATOR_HOST") == "https://storage.googleapis.com"
    )

    # Mock authentication if not using a real GCS endpoint,
    # since grpc client in extended_gcsfs does not work with anon access
    mock_authentication_manager = (
        patch("google.auth.default", return_value=(None, "fake-project"))
        if not is_real_gcs
        else nullcontext()
    )

    with mock_authentication_manager:
        extended_gcsfs = gcs_factory()
        try:
            # Only create/delete/populate the bucket if we are NOT using the real GCS endpoint
            if not is_real_gcs:
                try:
                    extended_gcsfs.rm(TEST_BUCKET, recursive=True)
                except FileNotFoundError:
                    pass
                extended_gcsfs.mkdir(TEST_BUCKET)
                if populate:
                    extended_gcsfs.pipe(
                        {TEST_BUCKET + "/" + k: v for k, v in allfiles.items()}
                    )
            extended_gcsfs.invalidate_cache()
            yield extended_gcsfs
        finally:
            _cleanup_gcs(extended_gcsfs, is_real_gcs)


@pytest.fixture
def gcs_versioned(gcs_factory):
    gcs = gcs_factory()
    gcs.version_aware = True
    is_real_gcs = (
        os.environ.get("STORAGE_EMULATOR_HOST") == "https://storage.googleapis.com"
    )
    try:  # ensure we're empty.
        if is_real_gcs:
            # For real GCS, we assume the bucket exists and only clean its contents.
            try:
                cleanup_versioned_bucket(gcs, TEST_VERSIONED_BUCKET)
            except Exception as e:
                logging.warning(
                    f"Failed to empty versioned bucket {TEST_VERSIONED_BUCKET}: {e}"
                )
        else:
            # For emulators, we delete and recreate the bucket for a clean state.
            try:
                gcs.rm(TEST_VERSIONED_BUCKET, recursive=True)
            except FileNotFoundError:
                pass
            gcs.mkdir(TEST_VERSIONED_BUCKET, enable_versioning=True)
        gcs.invalidate_cache()
        yield gcs
    finally:
        try:
            if not is_real_gcs:
                gcs.rm(gcs.find(TEST_VERSIONED_BUCKET, versions=True))
                gcs.rm(TEST_VERSIONED_BUCKET)
        except:  # noqa: E722
            pass


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
