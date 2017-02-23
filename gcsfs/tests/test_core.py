
import os
import pytest

from gcsfs.tests.settings import TEST_PROJECT, GOOGLE_TOKEN, TEST_BUCKET
from gcsfs.tests.utils import (tempdir, token_restore, my_vcr, gcs_maker,
                               files, csv_files, text_files, a, b, c, d)
from gcsfs.core import GCSFileSystem


@my_vcr.use_cassette(match=['all'])
def test_simple(token_restore):
    assert not GCSFileSystem.tokens
    gcs = GCSFileSystem(TEST_PROJECT, token=GOOGLE_TOKEN)
    assert gcs.ls('')

    # token is now cached
    gcs = GCSFileSystem(TEST_PROJECT)
    assert gcs.ls('')


@my_vcr.use_cassette(match=['all'])
def test_simple_upload(token_restore):
    with gcs_maker() as gcs:
        fn = TEST_BUCKET + '/test'
        with gcs.open(fn, 'wb') as f:
            f.write(b'zz')
        assert gcs.cat(fn) == b'zz'


@my_vcr.use_cassette(match=['all'])
def test_multi_upload(token_restore):
    with gcs_maker() as gcs:
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


@my_vcr.use_cassette(match=['all'])
def test_info(token_restore):
    with gcs_maker() as gcs:
        gcs.touch(a)
        assert gcs.info(a) == gcs.ls(a, detail=True)[0]


@my_vcr.use_cassette(match=['all'])
def test_ls(token_restore):
    with gcs_maker() as gcs:
        assert TEST_BUCKET in gcs.ls('')
        with pytest.raises((OSError, IOError)):
            gcs.ls('nonexistent')
        fn = TEST_BUCKET+'/test/accounts.1.json'
        gcs.touch(fn)
        assert fn in gcs.ls(TEST_BUCKET+'/test')


@my_vcr.use_cassette(match=['all'])
def test_pickle(token_restore):
    with gcs_maker() as gcs:
        import pickle
        gcs2 = pickle.loads(pickle.dumps(gcs))
        gcs.touch(a)
        assert gcs.ls(TEST_BUCKET) == gcs.ls(TEST_BUCKET)


@my_vcr.use_cassette(match=['all'])
def test_ls_touch(token_restore):
    with gcs_maker() as gcs:
        assert not gcs.exists(TEST_BUCKET+'/tmp/test')
        gcs.touch(a)
        gcs.touch(b)
        L = gcs.ls(TEST_BUCKET+'/tmp/test', True)
        assert set(d['name'] for d in L) == set([a, b])
        L = gcs.ls(TEST_BUCKET+'/tmp/test', False)
        assert set(L) == set([a, b])


@my_vcr.use_cassette(match=['all'])
def test_rm(token_restore):
    with gcs_maker() as gcs:
        assert not gcs.exists(a)
        gcs.touch(a)
        assert gcs.exists(a)
        gcs.rm(a)
        assert not gcs.exists(a)
        with pytest.raises((OSError, IOError)):
            gcs.rm(TEST_BUCKET+'/nonexistent')
        with pytest.raises((OSError, IOError)):
            gcs.rm('nonexistent')


@my_vcr.use_cassette(match=['all'])
def test_file_access(token_restore):
    with gcs_maker() as gcs:
        fn = TEST_BUCKET+'/nested/file1'
        data = b'hello\n'
        with gcs.open(fn, 'wb') as f:
            f.write(data)
        assert gcs.cat(fn) == data
        assert gcs.head(fn, 3) == data[:3]
        assert gcs.tail(fn, 3) == data[-3:]
        assert gcs.tail(fn, 10000) == data


@my_vcr.use_cassette(match=['all'])
def test_file_info(token_restore):
    with gcs_maker() as gcs:
        fn = TEST_BUCKET+'/nested/file1'
        data = b'hello\n'
        with gcs.open(fn, 'wb') as f:
            f.write(data)
        assert fn in gcs.walk(TEST_BUCKET)
        assert gcs.exists(fn)
        assert not gcs.exists(fn+'another')
        assert gcs.info(fn)['size'] == len(data)
        with pytest.raises((OSError, IOError)):
            gcs.info(fn+'another')


@my_vcr.use_cassette(match=['all'])
def test_du(token_restore):
    with gcs_maker(True) as gcs:
        d = gcs.du(TEST_BUCKET, deep=True)
        assert all(isinstance(v, int) and v >= 0 for v in d.values())
        assert TEST_BUCKET+'/nested/file1' in d

        assert gcs.du(TEST_BUCKET + '/test/', total=True) == sum(
                map(len, files.values()))
        assert gcs.du(TEST_BUCKET) == gcs.du('gcs://'+TEST_BUCKET)


@my_vcr.use_cassette(match=['all'])
def test_ls(token_restore):
    with gcs_maker(True) as gcs:
        fn = TEST_BUCKET+'/nested/file1'
        assert fn not in gcs.ls(TEST_BUCKET+'/')
        assert fn in gcs.ls(TEST_BUCKET+'/nested/')
        assert fn in gcs.ls(TEST_BUCKET+'/nested')
        assert gcs.ls('gcs://'+TEST_BUCKET+'/nested/') == gcs.ls(
                TEST_BUCKET+'/nested')
