import asyncio
from decorator import decorator
import json
import logging
import random


import requests.exceptions
import google.auth.exceptions
import aiohttp.client_exceptions

logger = logging.getLogger("gcsfs")


class HttpError(Exception):
    """Holds the message and code from cloud errors."""

    def __init__(self, error_response=None):
        if error_response:
            self.code = error_response.get("code", None)
            self.message = error_response.get("message", "")
            if self.code:
                if isinstance(self.message, bytes):
                    self.message += (", %s" % self.code).encode()
                else:
                    self.message += ", %s" % self.code
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
    aiohttp.client_exceptions.ClientError,
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


def validate_response(status, content, path, args=None):
    """
    Check the requests object r, raise error if it's not ok.

    Parameters
    ----------
    r: requests response object
    path: associated URL path, for error messages
    """
    if status >= 400:
        if args:
            from .core import quote_plus

            path = path.format(*[quote_plus(p) for p in args])
        if status == 404:
            raise FileNotFoundError(path)

        error = None
        if hasattr(content, "decode"):
            content = content.decode()
        try:
            error = json.loads(content)["error"]
            msg = error["message"]
        except json.decoder.JSONDecodeError:
            msg = content

        if status == 403:
            raise IOError("Forbidden: %s\n%s" % (path, msg))
        elif status == 502:
            raise requests.exceptions.ProxyError()
        elif "invalid" in str(msg):
            raise ValueError("Bad Request: %s\n%s" % (path, msg))
        elif error:
            raise HttpError(error)
        elif status:
            raise HttpError({"code": status, "message": msg})  # text-like
        else:
            raise RuntimeError(msg)


@decorator
async def retry_request(func, retries=6, *args, **kwargs):
    for retry in range(retries):
        try:
            if retry > 0:
                await asyncio.sleep(min(random.random() + 2 ** (retry - 1), 32))
            return await func(*args, **kwargs)
        except (
            HttpError,
            requests.exceptions.RequestException,
            google.auth.exceptions.GoogleAuthError,
            ChecksumError,
            aiohttp.client_exceptions.ClientError,
        ) as e:
            if (
                isinstance(e, HttpError)
                and e.code == 400
                and "requester pays" in e.message
            ):
                msg = (
                    "Bucket is requester pays. "
                    "Set `requester_pays=True` when creating the GCSFileSystem."
                )
                raise ValueError(msg) from e
            # Special test for 404 to avoid retrying the request
            if (
                isinstance(e, aiohttp.client_exceptions.ClientResponseError)
                and e.status == 404
            ):
                logger.debug("Request returned 404, no retries.")
                raise e
            if isinstance(e, HttpError) and e.code == 404:
                logger.debug("Request returned 404, no retries.")
                raise e
            if retry == retries - 1:
                logger.exception(
                    "%s out of retries on exception: %s" % (func.__name__, e)
                )
                raise e
            if is_retriable(e):
                logger.debug("%s retrying after exception: %s" % (func.__name__, e))
                continue
            logger.exception("%s non-retriable exception: %s" % (func.__name__, e))
            raise e
