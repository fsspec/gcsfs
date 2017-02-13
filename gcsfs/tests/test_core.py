
from gcsfs.tests.settings import TEST_PROJECT, GOOGLE_TOKEN
from gcsfs.tests.utils import tempdir, token_restore, my_vcr
from gcsfs.core import GCSFileSystem
import os
import pytest

TEST_BUCKET = 'gcsfs-test'


@my_vcr.use_cassette
def test_simple(token_restore):
    assert not GCSFileSystem.tokens
    gcs = GCSFileSystem(TEST_PROJECT, token=GOOGLE_TOKEN)
    assert gcs.ls('')

    # token is now cached
    gcs = GCSFileSystem(TEST_PROJECT)
    assert gcs.ls('')
