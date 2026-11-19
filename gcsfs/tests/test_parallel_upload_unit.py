"""Unit tests for parallel uploads. The SDK call and the bucket layout lookup are mocked."""

import fsspec
import pytest

from gcsfs.extended_gcsfs import ExtendedGcsFileSystem
from gcsfs.tests.utils import is_real_gcs

pytestmark = pytest.mark.skipif(
    is_real_gcs(), reason="Unit tests with mocks; no real GCS needed."
)


def _fs(**kwargs):
    return ExtendedGcsFileSystem(token="anon", skip_instance_cache=True, **kwargs)


def _flags(fs):
    return (
        fs.parallel_upload,
        fs.parallel_upload_threshold,
        fs.parallel_upload_segment_size,
    )


def test_flags_default_off():
    assert _flags(_fs()) == (False, 128 * 2**20, None)


@pytest.mark.parametrize(
    "kwargs, expected",
    [
        (
            dict(
                parallel_upload=True,
                parallel_upload_threshold=1024,
                parallel_upload_segment_size=4096,
            ),
            (True, 1024, 4096),
        ),
        # Config files give strings.
        (dict(parallel_upload="true"), (True, 128 * 2**20, None)),
        (dict(parallel_upload=""), (False, 128 * 2**20, None)),
    ],
)
def test_flags_from_constructor(kwargs, expected):
    assert _flags(_fs(**kwargs)) == expected


def test_flags_from_env(monkeypatch):
    monkeypatch.setenv("FSSPEC_GCS_PARALLEL_UPLOAD", "true")
    monkeypatch.setenv("FSSPEC_GCS_PARALLEL_UPLOAD_THRESHOLD", "1024")
    monkeypatch.setenv("FSSPEC_GCS_PARALLEL_UPLOAD_SEGMENT_SIZE", "4096")
    # fsspec reads FSSPEC_* variables when it is imported, so read them again.
    conf = {}
    fsspec.config.set_conf_env(conf)
    monkeypatch.setattr(fsspec.config, "conf", conf)
    assert _flags(_fs()) == (True, 1024, 4096)


@pytest.mark.parametrize(
    "flag, value",
    [
        ("parallel_upload", "maybe"),
    ],
)
def test_unparsable_flag_names_the_flag(flag, value):
    with pytest.raises(ValueError, match=flag):
        _fs(**{flag: value})
