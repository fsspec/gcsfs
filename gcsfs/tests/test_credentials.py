from gcsfs.credentials import GoogleCredentials


def test_googlecredentials_none():
    credentials = GoogleCredentials(project="myproject", token=None, access="read_only")
    headers = {}
    credentials.apply(headers)
