
import os
import pytest

from gcsfs.tests.settings import TEST_PROJECT, GOOGLE_TOKEN, TEST_BUCKET
from gcsfs.tests.utils import tempdir, token_restore, my_vcr, gcs
from gcsfs.core import GCSFileSystem


@my_vcr.use_cassette
def test_simple(token_restore):
    assert not GCSFileSystem.tokens
    gcs = GCSFileSystem(TEST_PROJECT, token=GOOGLE_TOKEN)
    assert gcs.ls('')

    # token is now cached
    gcs = GCSFileSystem(TEST_PROJECT)
    assert gcs.ls('')


@my_vcr.use_cassette
def test_simple_upload(gcs):
    fn = TEST_BUCKET + '/test'
    with gcs.open(fn, 'wb') as f:
        f.write(b'zz')
    assert gcs.cat(fn) == b'zz'


@my_vcr.use_cassette
def test_multi_upload(gcs):
    fn = TEST_BUCKET + '/test'
    d = b'01234567' * 2**15

    # something to write on close
    with gcs.open(fn, 'wb', block_size=2**18) as f:
        f.write(d)
        f.write(b'xx')
    assert gcs.cat(fn) == d + b'xx'

    # empty buffer on close
    with gcs.open(fn, 'wb', block_size=2**19) as f:
        f.write(d)
        f.write(b'xx')
        f.write(d)
    assert gcs.cat(fn) == d + b'xx' + d
