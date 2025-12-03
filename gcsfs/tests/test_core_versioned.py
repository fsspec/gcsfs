import logging
import os
import posixpath

import pytest
from google.cloud import storage

from gcsfs import GCSFileSystem
from gcsfs.tests.settings import TEST_VERSIONED_BUCKET

a = TEST_VERSIONED_BUCKET + "/tmp/test/a"
b = TEST_VERSIONED_BUCKET + "/tmp/test/b"

# Flag to track if the bucket was created by this test run.
_VERSIONED_BUCKET_CREATED_BY_TESTS = False


def is_versioning_enabled():
    """
    Helper function to check if the test bucket has versioning enabled.
    Returns a tuple of (bool, reason_string).
    """
    # Don't skip when using an emulator, as we create the versioned bucket ourselves.
    global _VERSIONED_BUCKET_CREATED_BY_TESTS
    if os.environ.get("STORAGE_EMULATOR_HOST") != "https://storage.googleapis.com":
        return True, ""
    try:
        gcs = GCSFileSystem(project=os.getenv("GCSFS_TEST_PROJECT", "project"))
        if not gcs.exists(TEST_VERSIONED_BUCKET):
            logging.info(
                f"Creating versioned bucket for tests: {TEST_VERSIONED_BUCKET}"
            )
            gcs.mkdir(TEST_VERSIONED_BUCKET, enable_versioning=True)
            _VERSIONED_BUCKET_CREATED_BY_TESTS = True

        client = storage.Client(
            credentials=gcs.credentials.credentials, project=gcs.project
        )
        bucket = client.get_bucket(TEST_VERSIONED_BUCKET)
        if bucket.versioning_enabled:
            return True, ""
        return (
            False,
            f"Bucket '{TEST_VERSIONED_BUCKET}' does not have versioning enabled.",
        )
    except Exception as e:
        return (
            False,
            f"Could not verify versioning status for bucket '{TEST_VERSIONED_BUCKET}': {e}",
        )


pytestmark = pytest.mark.skipif(
    not is_versioning_enabled()[0], reason=is_versioning_enabled()[1]
)


def test_info_versioned(gcs_versioned):
    with gcs_versioned.open(a, "wb") as wo:
        wo.write(b"v1")
    v1 = gcs_versioned.info(a)["generation"]
    assert v1 is not None
    with gcs_versioned.open(a, "wb") as wo:
        wo.write(b"v2")
    v2 = gcs_versioned.info(a)["generation"]
    assert v2 is not None and v1 != v2
    assert gcs_versioned.info(f"{a}#{v1}")["generation"] == v1
    assert gcs_versioned.info(f"{a}?generation={v2}")["generation"] == v2


def test_cat_versioned(gcs_versioned):
    with gcs_versioned.open(b, "wb") as wo:
        wo.write(b"v1")
    v1 = gcs_versioned.info(b)["generation"]
    assert v1 is not None
    with gcs_versioned.open(b, "wb") as wo:
        wo.write(b"v2")
    assert gcs_versioned.cat(f"{b}#{v1}") == b"v1"


def test_cp_versioned(gcs_versioned):
    with gcs_versioned.open(a, "wb") as wo:
        wo.write(b"v1")
    v1 = gcs_versioned.info(a)["generation"]
    assert v1 is not None
    with gcs_versioned.open(a, "wb") as wo:
        wo.write(b"v2")
    gcs_versioned.cp_file(f"{a}#{v1}", b)
    assert gcs_versioned.cat(b) == b"v1"


def test_ls_versioned(gcs_versioned):
    with gcs_versioned.open(b, "wb") as wo:
        wo.write(b"v1")
    v1 = gcs_versioned.info(b)["generation"]
    with gcs_versioned.open(b, "wb") as wo:
        wo.write(b"v2")
    v2 = gcs_versioned.info(b)["generation"]
    dpath = posixpath.dirname(b)
    versions = {f"{b}#{v1}", f"{b}#{v2}"}
    assert versions == set(gcs_versioned.ls(dpath, versions=True))
    assert versions == {
        entry["name"] for entry in gcs_versioned.ls(dpath, detail=True, versions=True)
    }
    assert gcs_versioned.ls(TEST_VERSIONED_BUCKET, versions=True) == [
        f"{TEST_VERSIONED_BUCKET}/tmp"
    ]


def test_find_versioned(gcs_versioned):
    with gcs_versioned.open(a, "wb") as wo:
        wo.write(b"v1")
    v1 = gcs_versioned.info(a)["generation"]
    with gcs_versioned.open(a, "wb") as wo:
        wo.write(b"v2")
    v2 = gcs_versioned.info(a)["generation"]
    versions = {f"{a}#{v1}", f"{a}#{v2}"}
    assert versions == set(gcs_versioned.find(a, versions=True))
    assert versions == set(gcs_versioned.find(a, detail=True, versions=True))
