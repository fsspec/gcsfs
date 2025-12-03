import logging

import fsspec
import pytest
from fsspec.tests.abstract import AbstractFixtures

from gcsfs.core import GCSFileSystem
from gcsfs.tests.conftest import _cleanup_gcs, allfiles
from gcsfs.tests.settings import TEST_BUCKET


class GcsfsFixtures(AbstractFixtures):
    @pytest.fixture(scope="class")
    def fs(self, docker_gcs, buckets_to_delete):
        GCSFileSystem.clear_instance_cache()
        gcs = fsspec.filesystem("gcs", endpoint_url=docker_gcs)
        try:  # ensure we're empty.
            # Create the bucket if it doesn't exist, otherwise clean it.
            if not gcs.exists(TEST_BUCKET):
                buckets_to_delete.add(TEST_BUCKET)
                gcs.mkdir(TEST_BUCKET)
            else:
                try:
                    gcs.rm(gcs.find(TEST_BUCKET))
                except Exception as e:
                    logging.warning(f"Failed to empty bucket {TEST_BUCKET}: {e}")

            gcs.pipe({TEST_BUCKET + "/" + k: v for k, v in allfiles.items()})
            gcs.invalidate_cache()
            yield gcs
        finally:
            _cleanup_gcs(gcs)

    @pytest.fixture
    def fs_path(self):
        return TEST_BUCKET

    @pytest.fixture
    def supports_empty_directories(self):
        return False
