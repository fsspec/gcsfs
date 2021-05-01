import os
import requests
from requests.exceptions import ProxyError
import pytest

from gcsfs.tests.settings import TEST_BUCKET
from gcsfs.retry import HttpError, is_retriable, validate_response
from gcsfs.tests.utils import tmpfile, my_vcr, gcs_maker


def test_tempfile():
    with tmpfile() as fn:
        with open(fn, "w"):
            pass
        assert os.path.exists(fn)
    assert not os.path.exists(fn)


def test_retriable_exception():
    e = requests.exceptions.Timeout()
    assert is_retriable(e)
    e = ValueError
    assert not is_retriable(e)

    e = HttpError({"message": "", "code": 500})
    assert is_retriable(e)

    e = HttpError({"message": "", "code": "500"})
    assert is_retriable(e)

    e = HttpError({"message": "", "code": 400})
    assert not is_retriable(e)

    e = HttpError({"code": "429"})
    assert is_retriable(e)

    e = ProxyError()
    assert is_retriable(e)


def test_validate_response():
    validate_response(200, None, "/path")

    # HttpError with no JSON body
    with pytest.raises(HttpError) as e:
        validate_response(503, b"", "/path")
    assert e.value.code == 503
    assert e.value.message == ", 503"

    # HttpError with JSON body
    j = '{"error": {"code": 503, "message": "Service Unavailable"}}'
    with pytest.raises(HttpError) as e:
        validate_response(503, j, "/path")
    assert e.value.code == 503
    assert e.value.message == "Service Unavailable, 503"

    # 403
    j = '{"error": {"message": "Not ok"}}'
    with pytest.raises(IOError, match="Forbidden: /path\nNot ok"):
        validate_response(403, j, "/path")

    # 404
    with pytest.raises(FileNotFoundError):
        validate_response(404, b"", None, "/path")

    # 502
    with pytest.raises(ProxyError):
        validate_response(502, b"", None, "/path")


@my_vcr.use_cassette(match=["all"])
@pytest.mark.parametrize(
    ["file_path", "validate_get_error", "validate_list_error", "expected_error"],
    [
        (
            "/missing",
            FileNotFoundError,
            None,
            FileNotFoundError,
        ),  # Not called
        (
            "/missing",
            OSError("Forbidden"),
            FileNotFoundError,
            FileNotFoundError,
        ),
        (
            "/2014-01-01.csv",
            None,
            None,
            None,
        ),
        (
            "/2014-01-01.csv",
            OSError("Forbidden"),
            None,
            None,
        ),
    ],
    ids=[
        "missing_with_get_perms",
        "missing_with_list_perms",
        "existing_with_get_perms",
        "existing_with_list_perms",
    ],
)
def test_metadata_read_permissions(
    file_path, validate_get_error, validate_list_error, expected_error
):
    with gcs_maker(True) as gcs:

        def _validate_response(self, status, content, path, headers=None):
            if path.endswith(f"/o{file_path}") and validate_get_error is not None:
                raise validate_get_error
            if path.endswith("/o/") and validate_list_error is not None:
                raise validate_list_error
            validate_response(status, content, path, headers=None)

        if expected_error is None:
            gcs.ls(TEST_BUCKET + file_path)
            gcs.info(TEST_BUCKET + file_path)
            assert gcs.exists(TEST_BUCKET + file_path)
        else:
            with pytest.raises(expected_error):
                gcs.ls(TEST_BUCKET + file_path)
            with pytest.raises(expected_error):
                gcs.info(TEST_BUCKET + file_path)
            assert gcs.exists(TEST_BUCKET + file_path) is False
