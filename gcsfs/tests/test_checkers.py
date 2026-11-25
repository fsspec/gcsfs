import base64
from hashlib import md5

import pytest

import gcsfs.checkers as checkers_mod
from gcsfs.checkers import (
    ConsistencyChecker,
    Crc32cChecker,
    MD5Checker,
    SizeChecker,
    crcmod,
    get_consistency_checker,
    google_crc32c,
)
from gcsfs.retry import ChecksumError

HAS_CRC32C = google_crc32c is not None or crcmod is not None


def _compute_crc32c_b64(data: bytes) -> str:
    if google_crc32c is not None:
        digest = google_crc32c.Checksum(data).digest()
    else:
        checksum = crcmod.Crc(0x11EDC6F41, initCrc=0, xorOut=0xFFFFFFFF)
        checksum.update(data)
        digest = checksum.digest()
    return base64.b64encode(digest).decode()


def google_response_from_data(expected_data: bytes, actual_data=None):
    actual_data = actual_data or expected_data
    checksum = md5(actual_data)
    checksum_b64 = base64.b64encode(checksum.digest()).decode("UTF-8")
    crc = _compute_crc32c_b64(actual_data) if HAS_CRC32C else None

    class response:
        content_length = len(actual_data)
        headers = {"X-Goog-Hash": f"md5={checksum_b64}"}
        if crc is not None:
            headers["X-Goog-Hash"] += f", crc32c={crc}"

    return response


def google_response_from_data_with_reverse_header_order(
    expected_data: bytes, actual_data=None
):
    actual_data = actual_data or expected_data
    checksum = md5(actual_data)
    checksum_b64 = base64.b64encode(checksum.digest()).decode("UTF-8")
    crc = _compute_crc32c_b64(actual_data) if HAS_CRC32C else None

    class response:
        content_length = len(actual_data)
        headers = {}
        if crc is not None:
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

if HAS_CRC32C:
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

if HAS_CRC32C:
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
    (SizeChecker(), b"hello world", b"different size", (ChecksumError,)),
]

if HAS_CRC32C:
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
    (SizeChecker(), b"hello world", b"different size", (ChecksumError,)),
]
if HAS_CRC32C:
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
    assert isinstance(get_consistency_checker("size"), SizeChecker)
    assert isinstance(get_consistency_checker("md5"), MD5Checker)
    assert isinstance(get_consistency_checker(None), ConsistencyChecker)
    assert isinstance(get_consistency_checker("unknown"), ConsistencyChecker)

    if HAS_CRC32C:
        assert isinstance(get_consistency_checker("crc32c"), Crc32cChecker)


def test_consistency_checker_base():
    base = ConsistencyChecker()
    base.update(b"abc")
    base.validate_json_response({})
    base.validate_headers({})
    base.validate_http_response(None)


def test_checker_missing_headers():
    md5_checker = MD5Checker()
    md5_checker.update(b"test")
    with pytest.raises(NotImplementedError, match="No md5 checksum"):
        md5_checker.validate_headers({"X-Goog-Hash": "crc32c=12345"})

    if HAS_CRC32C:
        crc_checker = Crc32cChecker()
        crc_checker.update(b"test")
        with pytest.raises(NotImplementedError, match="No crc32c checksum"):
            crc_checker.validate_headers({"X-Goog-Hash": "md5=12345"})


def test_checker_missing_json_hash_raises_not_implemented_error():
    md5_checker = MD5Checker()
    md5_checker.update(b"test")
    with pytest.raises(NotImplementedError, match="No md5 checksum"):
        md5_checker.validate_json_response({"size": 4})

    if HAS_CRC32C:
        crc_checker = Crc32cChecker()
        crc_checker.update(b"test")
        with pytest.raises(NotImplementedError, match="No crc32c checksum"):
            crc_checker.validate_json_response({"size": 4})


@pytest.mark.skipif(google_crc32c is None, reason="google_crc32c not installed")
def test_crc32c_checker_uses_google_crc32c_when_crcmod_missing(monkeypatch):
    monkeypatch.setattr(checkers_mod, "crcmod", None)
    checker = get_consistency_checker("crc32c")
    assert isinstance(checker, Crc32cChecker)
    checker.update(b"hello ")
    checker.update(memoryview(b"world\n"))
    checker.validate_json_response({"crc32c": "8P9ykg=="})


@pytest.mark.skipif(crcmod is None, reason="crcmod not installed")
def test_crc32c_checker_falls_back_to_crcmod_when_google_crc32c_missing(monkeypatch):
    monkeypatch.setattr(checkers_mod, "google_crc32c", None)
    checker = get_consistency_checker("crc32c")
    assert isinstance(checker, Crc32cChecker)
    checker.update(b"hello world\n")
    checker.validate_json_response({"crc32c": "8P9ykg=="})


def test_get_consistency_checker_missing_crc_backends_raises_import_error(monkeypatch):
    monkeypatch.setattr(checkers_mod, "google_crc32c", None)
    monkeypatch.setattr(checkers_mod, "crcmod", None)
    with pytest.raises(ImportError, match="crc32c"):
        get_consistency_checker("crc32c")
    with pytest.raises(ImportError, match="crc32c"):
        Crc32cChecker()
