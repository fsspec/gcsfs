"""Live integration tests for Rapid Cache Ultra (RCU) bucket read/write paths.

Exercises RCU-specific object lifecycle and placement scenarios against real
GCS RCU buckets (both flat and HNS):
- MRD reads of ``STANDARD`` storage-class objects uploaded over HTTP (cache miss)
- Re-reading ``STANDARD`` objects from a fresh filesystem handle (post-miss / cache hit)
- Default ``ExtendedGcsFileSystem`` (``rcu_rapid_write=False``) HTTP writes
  producing ``STANDARD`` objects (preserving metadata/content_type and warning
  on ``"ab"`` overwrite), read back over MRD
- Explicit ``rcu_rapid_write=True`` writes overwriting ``STANDARD`` objects
  with ``RAPID`` objects via ``ZonalFile``
- Cross-zone MRD reads and both default HTTP and ``rcu_rapid_write=True``
  writes when ``GCSFS_CROSS_ZONE_RCU_TEST_BUCKET`` points to an RCU bucket whose
  rapid cache is provisioned in a different zone
"""

import uuid

import pytest

from gcsfs.core import GCSFile as CoreGCSFile
from gcsfs.core import GCSFileSystem as CoreGCSFileSystem
from gcsfs.extended_gcsfs import ExtendedGcsFileSystem
from gcsfs.tests.conftest import _close_gcs, requires_rapid, requires_real_gcs
from gcsfs.tests.settings import (
    TEST_CROSS_ZONE_RCU_BUCKET,
    TEST_PROJECT,
    TEST_REGION,
    TEST_ZONAL_BUCKET,
)
from gcsfs.tests.utils import tmpfile
from gcsfs.zonal_file import ZonalFile

pytestmark = [requires_real_gcs, requires_rapid]


def _yield_rcu_fs(*, rcu_rapid_write=None):
    kwargs = {
        "project": TEST_PROJECT,
        "default_location": TEST_REGION,
        "skip_instance_cache": True,
    }
    if rcu_rapid_write is not None:
        kwargs["rcu_rapid_write"] = rcu_rapid_write
    fs = ExtendedGcsFileSystem(**kwargs)
    try:
        layout = fs._sync_lookup_bucket_layout(TEST_ZONAL_BUCKET)
        if not layout.is_rcu:
            pytest.skip(
                f"RCU integration tests require an RCU bucket, got {layout} "
                f"for {TEST_ZONAL_BUCKET}"
            )
        yield fs
    finally:
        _close_gcs(fs)


@pytest.fixture
def rcu_fs():
    """Yield a default ``ExtendedGcsFileSystem`` verified against a live RCU bucket."""
    yield from _yield_rcu_fs()


@pytest.fixture
def rcu_rapid_fs():
    """Yield an ``ExtendedGcsFileSystem`` with ``rcu_rapid_write=True`` on a live RCU bucket."""
    yield from _yield_rcu_fs(rcu_rapid_write=True)


def _assert_mrd_reads(fs, path, expected):
    assert fs.cat_file(path) == expected
    assert fs.cat_ranges([path, path], [0, 24], [24, 88]) == [
        expected[:24],
        expected[24:88],
    ]
    with fs.open(path, "rb") as f:
        assert isinstance(f, ZonalFile)
        assert f.read() == expected


@pytest.fixture
def http_fs():
    """Yield a base ``GCSFileSystem`` that uploads ``STANDARD`` objects over HTTP."""
    fs = CoreGCSFileSystem(
        project=TEST_PROJECT,
        default_location=TEST_REGION,
        skip_instance_cache=True,
    )
    try:
        yield fs
    finally:
        _close_gcs(fs)


@pytest.fixture
def rcu_object_factory(rcu_fs):
    """Allocate unique object paths and remove only those objects on teardown."""
    created_paths = []

    def make_path(bucket: str = TEST_ZONAL_BUCKET, prefix: str = "rcu-e2e") -> str:
        path = f"{bucket}/{prefix}-{uuid.uuid4().hex}"
        created_paths.append(path)
        return path

    yield make_path

    for path in created_paths:
        try:
            rcu_fs.rm_file(path)
        except FileNotFoundError:
            pass


def test_rcu_read_standard_object_cache_miss(rcu_fs, http_fs, rcu_object_factory):
    """Reading a freshly HTTP-uploaded STANDARD object on RCU succeeds over MRD."""
    path = rcu_object_factory(prefix="rcu-std-miss")
    data = b"rcu-standard-cache-miss-" * 1024

    http_fs.pipe_file(path, data)
    assert rcu_fs.info(path)["storageClass"] == "STANDARD"

    assert rcu_fs.cat_file(path) == data
    assert rcu_fs.cat_ranges([path, path], [0, 24], [24, 88]) == [
        data[:24],
        data[24:88],
    ]
    with rcu_fs.open(path, "rb") as f:
        assert isinstance(f, ZonalFile)
        f.seek(24)
        assert f.read(64) == data[24:88]


def test_rcu_read_standard_object_reread(rcu_fs, http_fs, rcu_object_factory):
    """Re-reading a STANDARD object with a fresh handle succeeds after initial read."""
    path = rcu_object_factory(prefix="rcu-std-reread")
    data = b"rcu-standard-reread-" * 1024

    http_fs.pipe_file(path, data)
    assert rcu_fs.info(path)["storageClass"] == "STANDARD"
    assert rcu_fs.cat_file(path) == data

    fresh_fs = ExtendedGcsFileSystem(
        project=TEST_PROJECT,
        default_location=TEST_REGION,
        skip_instance_cache=True,
    )
    try:
        assert fresh_fs.cat_file(path) == data
        with fresh_fs.open(path, "rb") as f:
            assert isinstance(f, ZonalFile)
            assert f.read() == data
    finally:
        _close_gcs(fresh_fs)


def test_rcu_default_http_writes_and_mrd_reads(rcu_fs, rcu_object_factory):
    """Default ExtendedGcsFileSystem writes STANDARD objects over HTTP and reads via MRD."""
    assert rcu_fs.rcu_rapid_write is False

    pipe_path = rcu_object_factory(prefix="rcu-http-pipe")
    pipe_data = b'{"hello": "rcu-http-pipe"}' * 4
    rcu_fs.pipe_file(
        pipe_path,
        pipe_data,
        content_type="application/json",
        metadata={"x-test-meta": "rcu-http"},
    )
    rcu_fs.invalidate_cache(pipe_path)
    pipe_info = rcu_fs.info(pipe_path)
    assert pipe_info["storageClass"] == "STANDARD"
    assert pipe_info.get("timeFinalized") is not None
    assert pipe_info["contentType"] == "application/json"
    assert pipe_info["metadata"] == {"x-test-meta": "rcu-http"}
    _assert_mrd_reads(rcu_fs, pipe_path, pipe_data)

    put_path = rcu_object_factory(prefix="rcu-http-put")
    put_data = b"rcu-http-put-payload-" * 8
    with tmpfile() as local_path:
        with open(local_path, "wb") as f:
            f.write(put_data)
        rcu_fs.put_file(local_path, put_path)
    rcu_fs.invalidate_cache(put_path)
    assert rcu_fs.info(put_path)["storageClass"] == "STANDARD"
    _assert_mrd_reads(rcu_fs, put_path, put_data)

    open_path = rcu_object_factory(prefix="rcu-http-open")
    wb_data = b"rcu-http-open-wb-payload-" * 4
    with rcu_fs.open(
        open_path,
        "wb",
        content_type="text/plain",
        metadata={"open-mode": "wb"},
    ) as f:
        assert isinstance(f, CoreGCSFile)
        assert not isinstance(f, ZonalFile)
        f.write(wb_data)
    rcu_fs.invalidate_cache(open_path)
    wb_info = rcu_fs.info(open_path)
    assert wb_info["storageClass"] == "STANDARD"
    assert wb_info["contentType"] == "text/plain"
    assert wb_info["metadata"] == {"open-mode": "wb"}
    assert rcu_fs.cat_file(open_path) == wb_data

    ab_data = b"rcu-http-open-ab-overwrite-" * 4
    with pytest.warns(UserWarning, match="Append mode 'a' is not supported"):
        with rcu_fs.open(open_path, "ab") as f:
            assert isinstance(f, CoreGCSFile)
            assert not isinstance(f, ZonalFile)
            f.write(ab_data)
    rcu_fs.invalidate_cache(open_path)
    assert rcu_fs.info(open_path)["storageClass"] == "STANDARD"
    assert rcu_fs.cat_file(open_path) == ab_data


def test_rcu_overwrite_standard_with_rapid(rcu_fs, rcu_rapid_fs, rcu_object_factory):
    """Overwriting a STANDARD object with rcu_rapid_write=True writes RAPID."""
    path = rcu_object_factory(prefix="rcu-std-overwrite")
    standard_data = b"initial-standard-object-payload"
    rapid_data = b"overwritten-rapid-object-payload"

    rcu_fs.pipe_file(path, standard_data)
    std_info = rcu_fs.info(path)
    assert std_info["storageClass"] == "STANDARD"
    std_gen = std_info["generation"]
    assert rcu_fs.cat_file(path) == standard_data

    rcu_rapid_fs.pipe_file(path, rapid_data)
    rcu_fs.invalidate_cache(path)

    info = rcu_fs.info(path)
    assert info["storageClass"] == "RAPID"
    assert info.get("timeFinalized") is not None
    assert info["generation"] != std_gen
    assert rcu_fs.cat_file(path) == rapid_data

    stream_path = rcu_object_factory(prefix="rcu-std-overwrite-open")
    rcu_fs.pipe_file(stream_path, standard_data)
    stream_std_info = rcu_fs.info(stream_path)
    assert stream_std_info["storageClass"] == "STANDARD"
    stream_std_gen = stream_std_info["generation"]

    with rcu_rapid_fs.open(stream_path, "wb") as f:
        assert isinstance(f, ZonalFile)
        f.write(rapid_data)
    rcu_fs.invalidate_cache(stream_path)

    stream_info = rcu_fs.info(stream_path)
    assert stream_info["storageClass"] == "RAPID"
    assert stream_info.get("timeFinalized") is not None
    assert stream_info["generation"] != stream_std_gen
    assert rcu_fs.cat_file(stream_path) == rapid_data


def test_rcu_cross_zone_read_and_write(rcu_fs, rcu_rapid_fs, rcu_object_factory):
    """Reads and writes succeed when the RCU cache is in a different zone."""
    if not TEST_CROSS_ZONE_RCU_BUCKET:
        pytest.skip("GCSFS_CROSS_ZONE_RCU_TEST_BUCKET is not configured")

    cross_layout = rcu_fs._sync_lookup_bucket_layout(TEST_CROSS_ZONE_RCU_BUCKET)
    assert (
        cross_layout.is_rcu
    ), f"Expected RCU layout for {TEST_CROSS_ZONE_RCU_BUCKET}, got {cross_layout}"

    rapid_path = rcu_object_factory(
        bucket=TEST_CROSS_ZONE_RCU_BUCKET, prefix="rcu-xzone-rapid"
    )
    rapid_data = b"cross-zone-rapid-payload-" * 512
    rcu_rapid_fs.pipe_file(rapid_path, rapid_data)
    assert rcu_fs.info(rapid_path)["storageClass"] == "RAPID"
    _assert_mrd_reads(rcu_fs, rapid_path, rapid_data)

    std_path = rcu_object_factory(
        bucket=TEST_CROSS_ZONE_RCU_BUCKET, prefix="rcu-xzone-std"
    )
    std_data = b"cross-zone-standard-payload-" * 512
    rcu_fs.pipe_file(std_path, std_data)
    assert rcu_fs.info(std_path)["storageClass"] == "STANDARD"
    _assert_mrd_reads(rcu_fs, std_path, std_data)
