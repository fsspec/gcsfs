from base64 import b64encode, b64decode
import base64
from typing import Optional
from hashlib import md5
from .utils import ChecksumError

import crcmod


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
        mdback = gcs_object["md5Hash"]
        if b64encode(self.md.digest()) != mdback.encode():
            raise ChecksumError("MD5 checksum failed")

    def validate_headers(self, headers):
        if headers is not None and "X-Goog-Hash" in headers:

            dig = [
                bit.split("=")[1]
                for bit in headers["X-Goog-Hash"].split(",")
                if bit.split("=")[0] == "md5"
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
        assert int(gcs_object["size"]) == self.size, "Size mismatch"

    def validate_http_response(self, r):
        assert r.content_length == self.size


class Crc32cChecker(ConsistencyChecker):
    def __init__(self):
        self.crc32c = crcmod.Crc(0x11EDC6F41, initCrc=0, xorOut=0xFFFFFFFF)

    def update(self, data: bytes):
        self.crc32c.update(data)

    def validate_json_response(self, gcs_object):
        # docs for gcs_object: https://cloud.google.com/storage/docs/json_api/v1/objects
        digest = self.crc32c.digest()
        assert base64.b64encode(digest) == gcs_object["crc32c"]


def get_consistency_checker(consistency: Optional[str]) -> ConsistencyChecker:
    if consistency == "size":
        return SizeChecker()
    elif consistency == "md5":
        return MD5Checker()
    elif consistency == "none":
        return ConsistencyChecker()
    elif consistency is None:
        return ConsistencyChecker()
    else:
        raise NotImplementedError()
