from gcsfs.credentials import GoogleCredentials
from gcsfs.tests.utils import my_vcr


@my_vcr.use_cassette(match=["all"])
def test_GoogleCredentials_None():
    credentials = GoogleCredentials(project=None, token=None, access="read_only")
    headers = {}
    credentials.apply(headers)
