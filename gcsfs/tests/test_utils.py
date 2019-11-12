import os
import requests
from gcsfs.utils import HttpError, RateLimitException, is_retriable
from gcsfs.tests.utils import tmpfile


def test_tempfile():
    with tmpfile() as fn:
        with open(fn, "w"):
            pass
        assert os.path.exists(fn)
    assert not os.path.exists(fn)


def retriable_exception():
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
    e = HttpError()
    assert not is_retriable(e)
    e = RateLimitException()
    assert not is_retriable(e)
    e = RateLimitException({"message": "", "code": 501})
    assert is_retriable(e)
    e = RateLimitException({"message": "", "code": "501"})
    assert is_retriable(e)
    e = RateLimitException({"message": "", "code": 400})
    assert not is_retriable(e)
