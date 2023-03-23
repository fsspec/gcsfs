import os
import shlex
import subprocess
import time

import pytest
import requests
from fsspec import filesystem
from fsspec.tests.abstract import AbstractFixtures

import gcsfs.tests.settings
from gcsfs.core import GCSFileSystem

TEST_BUCKET = gcsfs.tests.settings.TEST_BUCKET


def _container():
    return "gcsfs_test"


def _stop_docker(container):
    cmd = shlex.split('docker ps -a -q --filter "name=%s"' % container)
    cid = subprocess.check_output(cmd).strip().decode()
    if cid:
        subprocess.call(["docker", "rm", "-f", "-v", cid])


class GcsfsFixtures(AbstractFixtures):
    @staticmethod
    @pytest.fixture
    def fs(_gcs):
        return _gcs

    @staticmethod
    @pytest.fixture
    def fs_path():
        return TEST_BUCKET

    @staticmethod
    @pytest.fixture
    def _docker_gcs(scope="module"):
        if "STORAGE_EMULATOR_HOST" in os.environ:
            # assume using real API or otherwise have a server already set up
            yield os.getenv("STORAGE_EMULATOR_HOST")
            return
        cmd = (
            "docker run -d -p 4443:4443 --name gcsfs_test fsouza/fake-gcs-server:latest -scheme "
            "http -public-host http://localhost:4443 -external-url http://localhost:4443 "
            "-backend memory"
        )
        container = _container()
        _stop_docker(container)
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
        _stop_docker(container)

    @staticmethod
    @pytest.fixture
    def _gcs_factory(_docker_gcs):
        def factory(default_location=None):
            GCSFileSystem.clear_instance_cache()
            return filesystem(
                "gcs", endpoint_url=_docker_gcs, default_location=default_location
            )

        return factory

    @staticmethod
    @pytest.fixture
    def _gcs(_gcs_factory, populate=True):
        gcs = _gcs_factory()
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

            gcs.invalidate_cache()
            yield gcs
        finally:
            try:
                gcs.rm(gcs.find(TEST_BUCKET))
                gcs.rm(TEST_BUCKET)
            except:  # noqa: E722
                pass
