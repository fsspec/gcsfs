import base64
from base64 import b64encode
from hashlib import md5

from .retry import ChecksumError

try:
    import google_crc32c
except ImportError:
    google_crc32c = None

try:
    import crcmod
except ImportError:
    crcmod = None


class ConsistencyChecker:
    def __init__(self):
        pass

    def update(self, data: bytes):
        pass

    def validate_json_response(self, gcs_object):
        pass

    def validate_headers(self, headers):
        pass

    def validate_http_response(self, r):
        pass


class MD5Checker(ConsistencyChecker):
    def __init__(self):
        self.md = md5()

    def update(self, data):
        self.md.update(data)

    def validate_json_response(self, gcs_object):
        mdback = gcs_object.get("md5Hash")
        if not mdback:
            raise NotImplementedError(
                "No md5 checksum available to do consistency check."
            )
        if b64encode(self.md.digest()) != mdback.encode():
            raise ChecksumError("MD5 checksum failed")

    def validate_headers(self, headers):
        if headers is not None and "X-Goog-Hash" in headers:

            dig = [
                bit.split("=")[1]
                for bit in headers["X-Goog-Hash"].split(",")
                if bit and bit.strip().startswith("md5=")
            ]
            if dig:
                if b64encode(self.md.digest()).decode().rstrip("=") != dig[0]:
                    raise ChecksumError("Checksum failure")
            else:
                raise NotImplementedError(
                    "No md5 checksum available to do consistency check. GCS does "
                    "not provide md5 sums for composite objects."
                )

    def validate_http_response(self, r):
        return self.validate_headers(r.headers)


class SizeChecker(ConsistencyChecker):
    def __init__(self):
        self.size = 0

    def update(self, data: bytes):
        self.size += len(data)

    def validate_json_response(self, gcs_object):
        if int(gcs_object["size"]) != self.size:
            raise ChecksumError("Size mismatch")

    def validate_http_response(self, r):
        if r.content_length != self.size:
            raise ChecksumError("Size mismatch")


class Crc32cChecker(ConsistencyChecker):
    def __init__(self):
        if google_crc32c is not None:
            self.crc32c = google_crc32c.Checksum()
        elif crcmod is not None:
            self.crc32c = crcmod.Crc(0x11EDC6F41, initCrc=0, xorOut=0xFFFFFFFF)
        else:
            raise ImportError(
                "Either `google-crc32c` or `crcmod` is required for "
                "`consistency='crc32c'`. This can be installed with "
                "`pip install gcsfs[crc]`"
            )

    def update(self, data: bytes):
        if not isinstance(data, bytes):
            data = bytes(data)
        self.crc32c.update(data)

    def validate_json_response(self, gcs_object):
        # docs for gcs_object: https://cloud.google.com/storage/docs/json_api/v1/objects
        expected = gcs_object.get("crc32c")
        if not expected:
            raise NotImplementedError(
                "No crc32c checksum available to do consistency check."
            )
        digest = self.crc32c.digest()
        digest_b64 = base64.b64encode(digest).decode()

        if digest_b64 != expected:
            raise ChecksumError(f'Expected "{expected}". Got "{digest_b64}"')

    def validate_headers(self, headers):
        if headers is not None:
            hasher = headers.get("X-Goog-Hash", "")
            crc = [h.split("=", 1)[1] for h in hasher.split(",") if "crc32c" in h]
            if not crc:
                raise NotImplementedError("No crc32c checksum was provided by google!")
            if crc[0] != b64encode(self.crc32c.digest()).decode():
                raise ChecksumError()

    def validate_http_response(self, r):
        return self.validate_headers(r.headers)


def get_consistency_checker(consistency: str | None) -> ConsistencyChecker:
    if consistency == "size":
        return SizeChecker()
    elif consistency == "md5":
        return MD5Checker()
    elif consistency == "crc32c":
        if google_crc32c is None and crcmod is None:
            raise ImportError(
                "Either `google-crc32c` or `crcmod` is required for "
                "`consistency='crc32c'`. This can be installed with "
                "`pip install gcsfs[crc]`"
            )
        return Crc32cChecker()
    else:
        return ConsistencyChecker()
