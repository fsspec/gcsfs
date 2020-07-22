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
