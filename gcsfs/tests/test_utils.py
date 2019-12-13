import os
import requests
from requests.exceptions import ProxyError

from gcsfs.utils import HttpError, is_retriable
from gcsfs.tests.utils import tmpfile


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
