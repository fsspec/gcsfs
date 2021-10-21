import os
from gcsfs.credentials import GoogleCredentials


def test_GoogleCredentials_None():
    credentials = GoogleCredentials(project="myproject", token=None, access="read_only")
    headers = {}
    credentials.apply(headers)
