import pytest

from gcsfs import GCSFileSystem
from gcsfs.credentials import GoogleCredentials
from gcsfs.retry import HttpError


def test_googlecredentials_none():
    credentials = GoogleCredentials(project="myproject", token=None, access="read_only")
    headers = {}
    credentials.apply(headers)


@pytest.mark.parametrize("token", ["", "incorrect.token", "x" * 100])
def test_credentials_from_raw_token(token):
    with pytest.raises(HttpError, match="Invalid Credentials"):
        fs = GCSFileSystem(project="myproject", token=token)
        fs.ls("/")
