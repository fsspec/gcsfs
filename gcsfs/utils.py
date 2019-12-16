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


RETRIABLE_EXCEPTIONS = (
    requests.exceptions.ChunkedEncodingError,
    requests.exceptions.ConnectionError,
    requests.exceptions.ReadTimeout,
    requests.exceptions.Timeout,
    requests.exceptions.ProxyError,
    requests.exceptions.SSLError,
    requests.exceptions.ContentDecodingError,
    google.auth.exceptions.RefreshError,
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
