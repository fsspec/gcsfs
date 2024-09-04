import os
import shlex
import subprocess
import time

import fsspec
import pytest
import requests

from gcsfs import GCSFileSystem
from gcsfs.tests.settings import TEST_BUCKET

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

    def factory(default_location=None):
        GCSFileSystem.clear_instance_cache()
        params["default_location"] = default_location
        return fsspec.filesystem("gcs", **params)

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


@pytest.fixture
def gcs_versioned(gcs_factory):
    gcs = gcs_factory()
    gcs.version_aware = True
    try:
        try:
            gcs.rm(gcs.find(TEST_BUCKET, versions=True))
        except FileNotFoundError:
            pass

        try:
            gcs.mkdir(TEST_BUCKET, enable_versioning=True)
        except Exception:
            pass
        gcs.invalidate_cache()
        yield gcs
    finally:
        try:
            gcs.rm(gcs.find(TEST_BUCKET, versions=True))
            gcs.rm(TEST_BUCKET)
        except:  # noqa: E722
            pass
