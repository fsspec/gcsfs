import binascii
import collections
import hashlib
from datetime import datetime
from urllib.parse import quote

import requests.exceptions
import google.auth.exceptions


class HttpError(Exception):
    """Holds the message and code from cloud errors."""

    def __init__(self, error_response=None):
        if error_response:
            self.message = error_response.get("message", "")
            self.code = error_response.get("code", None)
        else:
            self.message = ""
            self.code = None
        # Call the base class constructor with the parameters it needs
        super(HttpError, self).__init__(self.message)


class ChecksumError(Exception):
    """Raised when the md5 hash of the content does not match the header."""

    pass


RETRIABLE_EXCEPTIONS = (
    requests.exceptions.ChunkedEncodingError,
    requests.exceptions.ConnectionError,
    requests.exceptions.ReadTimeout,
    requests.exceptions.Timeout,
    requests.exceptions.ProxyError,
    requests.exceptions.SSLError,
    requests.exceptions.ContentDecodingError,
    google.auth.exceptions.RefreshError,
    ChecksumError,
)


def is_retriable(exception):
    """Returns True if this exception is retriable."""
    errs = list(range(500, 505)) + [
        # Request Timeout
        408,
        # Too Many Requests
        429,
    ]
    errs += [str(e) for e in errs]
    if isinstance(exception, HttpError):
        return exception.code in errs

    return isinstance(exception, RETRIABLE_EXCEPTIONS)


class FileSender:
    def __init__(self, consistency="none"):
        self.consistency = consistency
        if consistency == "size":
            self.sent = 0
        elif consistency == "md5":
            from hashlib import md5

            self.md5 = md5()

    async def send(self, pre, f, post):
        yield pre
        chunk = f.read(64 * 1024)
        while chunk:
            yield chunk
            if self.consistency == "size":
                self.sent += len(chunk)
            elif self.consistency == "md5":
                self.md5.update(chunk)
            chunk = f.read(64 * 1024)
        yield post

    def __len__(self):
        return self.sent


def generate_signed_url(
    google_credentials,
    bucket_name,
    object_name,
    subresource=None,
    expiration=604800,
    http_method="GET",
    query_parameters=None,
    headers=None,
):
    # This is modified from
    # https://github.com/GoogleCloudPlatform/python-docs-samples/blob/master/storage/signed_urls/generate_signed_urls.py
    # which is licensed under the Apache License, Version 2.0 Copyright 2018 Google, Inc.

    if expiration > 604800:
        raise ValueError(
            "Expiration Time can't be longer than 604800 seconds (7 days)."
        )

    escaped_object_name = quote(object_name.encode(), safe=b"/~")
    canonical_uri = "/{}".format(escaped_object_name)

    datetime_now = datetime.datetime.utcnow()
    request_timestamp = datetime_now.strftime("%Y%m%dT%H%M%SZ")
    datestamp = datetime_now.strftime("%Y%m%d")

    client_email = google_credentials.service_account_email
    credential_scope = "{}/auto/storage/goog4_request".format(datestamp)
    credential = "{}/{}".format(client_email, credential_scope)

    if headers is None:
        headers = dict()
    host = "{}.storage.googleapis.com".format(bucket_name)
    headers["host"] = host

    canonical_headers = ""
    ordered_headers = collections.OrderedDict(sorted(headers.items()))
    for k, v in ordered_headers.items():
        lower_k = str(k).lower()
        strip_v = str(v).lower()
        canonical_headers += "{}:{}\n".format(lower_k, strip_v)

    signed_headers = ""
    for k, _ in ordered_headers.items():
        lower_k = str(k).lower()
        signed_headers += "{};".format(lower_k)
    signed_headers = signed_headers[:-1]  # remove trailing ';'

    if query_parameters is None:
        query_parameters = dict()
    query_parameters["X-Goog-Algorithm"] = "GOOG4-RSA-SHA256"
    query_parameters["X-Goog-Credential"] = credential
    query_parameters["X-Goog-Date"] = request_timestamp
    query_parameters["X-Goog-Expires"] = expiration
    query_parameters["X-Goog-SignedHeaders"] = signed_headers
    if subresource:
        query_parameters[subresource] = ""

    canonical_query_string = ""
    ordered_query_parameters = collections.OrderedDict(sorted(query_parameters.items()))
    for k, v in ordered_query_parameters.items():
        encoded_k = quote(str(k), safe="")
        encoded_v = quote(str(v), safe="")
        canonical_query_string += "{}={}&".format(encoded_k, encoded_v)
    canonical_query_string = canonical_query_string[:-1]  # remove trailing '&'

    canonical_request = "\n".join(
        [
            http_method,
            canonical_uri,
            canonical_query_string,
            canonical_headers,
            signed_headers,
            "UNSIGNED-PAYLOAD",
        ]
    )

    canonical_request_hash = hashlib.sha256(canonical_request.encode()).hexdigest()

    string_to_sign = "\n".join(
        [
            "GOOG4-RSA-SHA256",
            request_timestamp,
            credential_scope,
            canonical_request_hash,
        ]
    )

    # signer.sign() signs using RSA-SHA256 with PKCS1v15 padding
    signature = binascii.hexlify(
        google_credentials.signer.sign(string_to_sign)
    ).decode()

    scheme_and_host = "{}://{}".format("https", host)
    signed_url = "{}{}?{}&x-goog-signature={}".format(
        scheme_and_host, canonical_uri, canonical_query_string, signature
    )

    return signed_url
