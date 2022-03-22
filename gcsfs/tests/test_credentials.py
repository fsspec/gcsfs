import pytest
from gcsfs.credentials import GoogleCredentials


def test_googlecredentials_none():
    with pytest.raises(ValueError):
        GoogleCredentials(project="myproject", token=None, access="read_only")
