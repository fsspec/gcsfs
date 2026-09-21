import base64
from hashlib import md5

import pytest

from gcsfs.checkers import Crc32cChecker, MD5Checker, SizeChecker, crcmod
from gcsfs.retry import ChecksumError


def google_response_from_data(expected_data: bytes, actual_data=None):
    actual_data = actual_data or expected_data
    checksum = md5(actual_data)
    checksum_b64 = base64.b64encode(checksum.digest()).decode("UTF-8")
    if crcmod is not None:
        checksum = crcmod.Crc(0x11EDC6F41, initCrc=0, xorOut=0xFFFFFFFF)
        checksum.update(actual_data)
        crc = base64.b64encode(checksum.digest()).decode()

    class response:
        content_length = len(actual_data)
        headers = {"X-Goog-Hash": f"md5={checksum_b64}"}
        if crcmod is not None:
            headers["X-Goog-Hash"] += f", crc32c={crc}"

    return response


def google_response_from_data_with_reverse_header_order(
    expected_data: bytes, actual_data=None
):
    actual_data = actual_data or expected_data
    checksum = md5(actual_data)
    checksum_b64 = base64.b64encode(checksum.digest()).decode("UTF-8")
    if crcmod is not None:
        checksum = crcmod.Crc(0x11EDC6F41, initCrc=0, xorOut=0xFFFFFFFF)
        checksum.update(actual_data)
        crc = base64.b64encode(checksum.digest()).decode()

    class response:
        content_length = len(actual_data)
        headers = {}
        if crcmod is not None:
            headers["X-Goog-Hash"] = f"crc32c={crc}, md5={checksum_b64}"
        else:
            headers["X-Goog-Hash"] = f"md5={checksum_b64}"

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


params = [
    (MD5Checker(), b"hello world", b"different checksum", (ChecksumError,)),
    (MD5Checker(), b"hello world", b"hello world", ()),
]

if crcmod is not None:
    params.append(
        (Crc32cChecker(), b"hello world", b"different checksum", (ChecksumError,))
    )
    params.append((Crc32cChecker(), b"hello world", b"hello world", ()))


@pytest.mark.parametrize("checker, data, actual_data, raises", params)
def test_validate_headers(checker, data, actual_data, raises):
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
]

if crcmod is not None:
    params.append(
        (Crc32cChecker(), b"hello world", b"different checksum", (ChecksumError,))
    )
    params.append((Crc32cChecker(), b"hello world", b"hello world", ()))


@pytest.mark.parametrize("checker, data, actual_data, raises", params)
def test_validate_headers_with_reverse_order(checker, data, actual_data, raises):
    response = google_response_from_data_with_reverse_header_order(actual_data)
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
    params.append((Crc32cChecker(), b"hello world", b"hello world", ()))
    params.append(
        (Crc32cChecker(), b"hello world", b"different size", (ChecksumError,))
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


def test_get_consistency_checker():
    from gcsfs.checkers import ConsistencyChecker, get_consistency_checker

    assert isinstance(get_consistency_checker("size"), SizeChecker)
    assert isinstance(get_consistency_checker("md5"), MD5Checker)
    assert isinstance(get_consistency_checker(None), ConsistencyChecker)
    assert isinstance(get_consistency_checker("unknown"), ConsistencyChecker)

    if crcmod is not None:
        assert isinstance(get_consistency_checker("crc32c"), Crc32cChecker)


def test_consistency_checker_base():
    from gcsfs.checkers import ConsistencyChecker

    base = ConsistencyChecker()
    base.update(b"abc")
    base.validate_json_response({})
    base.validate_headers({})
    base.validate_http_response(None)


def test_checker_missing_headers():
    md5_checker = MD5Checker()
    md5_checker.update(b"test")
    # Headers present but without md5
    with pytest.raises(NotImplementedError, match="No md5 checksum"):
        md5_checker.validate_headers({"X-Goog-Hash": "crc32c=12345"})

    if crcmod is not None:
        crc_checker = Crc32cChecker()
        crc_checker.update(b"test")
        with pytest.raises(NotImplementedError, match="No crc32c checksum"):
            crc_checker.validate_headers({"X-Goog-Hash": "md5=12345"})
