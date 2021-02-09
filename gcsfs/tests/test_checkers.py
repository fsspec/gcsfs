from gcsfs.utils import ChecksumError
from gcsfs.checkers import MD5Checker, SizeChecker
from hashlib import md5
import base64

import pytest


def google_response_from_data(data: bytes, checksum=None):

    if checksum is None:
        checksum = md5(data)
        checksum_b64 = base64.b64encode(checksum.digest()).decode("UTF-8")
    else:
        checksum_b64 = checksum

    class response:
        content_length = len(data)
        headers = {"X-Goog-Hash": f"md5={checksum_b64}"}

    return response


def test_size_checker_json():
    checker = SizeChecker()
    data = b"hello world"
    checker.update(data)
    checker.validate_json_response({"size": len(data)})


def test_size_checker_json_raises_error():
    checker = SizeChecker()
    data = b"hello world"
    checker.update(data)
    with pytest.raises(AssertionError):
        checker.validate_json_response({"size": 1})


def test_size_checker_http():
    checker = SizeChecker()
    data = b"hello world"

    class response:
        content_length = len(data)

    checker.update(data)
    checker.validate_http_response(response)


def test_size_checker_http_raises_error():
    checker = SizeChecker()
    data = b"hello world"
    response = google_response_from_data(data)

    # set incorrect length
    response.content_length = 1

    checker.update(data)
    with pytest.raises(AssertionError):
        checker.validate_http_response(response)


def test_md5_checker_http():
    checker = MD5Checker()
    data = b"hello world"
    response = google_response_from_data(data)

    checker.update(data)
    checker.validate_http_response(response)
    checker.validate_headers(response.headers)


def test_md5_checker_http_raisers_checksum_error():
    checker = MD5Checker()
    data = b"hello world"
    response = google_response_from_data(data, checksum=b"not the real hash")

    checker.update(data)

    with pytest.raises(ChecksumError):
        checker.validate_http_response(response)

    with pytest.raises(ChecksumError):
        checker.validate_headers(response.headers)


def test_md5_checker_json():
    checker = MD5Checker()
    data = b"hello world"
    checksum = md5(data)
    checksum_b64 = base64.b64encode(checksum.digest()).decode("UTF-8")

    checker.update(data)
    checker.validate_json_response({"md5Hash": checksum_b64})
