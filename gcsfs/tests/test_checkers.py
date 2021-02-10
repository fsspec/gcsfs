from gcsfs.utils import ChecksumError
from gcsfs.checkers import Crc32cChecker, MD5Checker, SizeChecker, crcmod
from hashlib import md5
import base64

import pytest


def google_response_from_data(expected_data: bytes, actual_data=None):

    actual_data = actual_data or expected_data
    checksum = md5(actual_data)
    checksum_b64 = base64.b64encode(checksum.digest()).decode("UTF-8")

    class response:
        content_length = len(actual_data)
        headers = {"X-Goog-Hash": f"md5={checksum_b64}"}

    return response


def google_json_response_from_data(expected_data: bytes, actual_data=None):
    actual_data = actual_data or expected_data
    checksum = md5(actual_data)
    checksum_b64 = base64.b64encode(checksum.digest()).decode("UTF-8")

    response = {"md5Hash": checksum_b64, "size": len(actual_data)}

    # some manual checksums verified using gsutil ls -L
    # also can add using https://crccalc.com/
    # be careful about newlines
    crc32c_points = {
        b"hello world\n": "8P9ykg==",
        b"different checksum": "DoesntMatter==",
    }

    try:
        response["crc32c"] = crc32c_points[actual_data]
    except KeyError:
        pass

    return response


@pytest.mark.parametrize(
    "data, actual_data, raises",
    [
        (b"hello world", b"different checksum", (ChecksumError,)),
        (b"hello world", b"hello world", ()),
    ],
)
def test_md5_checker_validate_headers(data, actual_data, raises):
    checker = MD5Checker()
    response = google_response_from_data(actual_data)
    checker.update(data)

    if raises:
        with pytest.raises(raises):
            checker.validate_headers(response.headers)
    else:
        checker.validate_headers(response.headers)


params = [
    (MD5Checker(), b"hello world", b"different checksum", (ChecksumError,)),
    (MD5Checker(), b"hello world", b"hello world", ()),
    (SizeChecker(), b"hello world", b"hello world", ()),
    (SizeChecker(), b"hello world", b"different size", (AssertionError,)),
]

if crcmod is not None:
    params.append(
        (Crc32cChecker(), b"hello world", b"different size", (NotImplementedError,))
    )


@pytest.mark.parametrize("checker, data, actual_data, raises", params)
def test_checker_validate_http_response(checker, data, actual_data, raises):
    response = google_response_from_data(data, actual_data=actual_data)
    checker.update(data)
    if raises:
        with pytest.raises(raises):
            checker.validate_http_response(response)
    else:
        checker.validate_http_response(response)


params = [
    (MD5Checker(), b"hello world", b"different checksum", (ChecksumError,)),
    (MD5Checker(), b"hello world", b"hello world", ()),
    (SizeChecker(), b"hello world", b"hello world", ()),
    (SizeChecker(), b"hello world", b"different size", (AssertionError,)),
]
if crcmod is not None:
    params.extend(
        [
            (Crc32cChecker(), b"hello world", b"different checksum", (ChecksumError,)),
            (Crc32cChecker(), b"hello world\n", b"hello world\n", ()),
        ]
    )


@pytest.mark.parametrize("checker, data, actual_data, raises", params)
def test_checker_validate_json_response(checker, data, actual_data, raises):
    response = google_json_response_from_data(data, actual_data=actual_data)
    checker.update(data)
    if raises:
        with pytest.raises(raises):
            checker.validate_json_response(response)
    else:
        checker.validate_json_response(response)
