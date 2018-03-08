import pytest
from gcsfs.tests.settings import TEST_PROJECT, GOOGLE_TOKEN, TEST_BUCKET
from gcsfs.tests.utils import (tempdir, token_restore, my_vcr, gcs_maker,
                               files, csv_files, text_files, a, b, c, d)
from gcsfs import GCSMap, GCSFileSystem, core

root = TEST_BUCKET+'/mapping'

@my_vcr.use_cassette(match=['all'])
def test_map_simple(token_restore):
    with gcs_maker() as gcs:
        d = GCSMap(root, gcs)
        assert not d

        assert list(d) == list(d.keys()) == []
        assert list(d.values()) == []
        assert list(d.items()) == []
        d = GCSMap(root, gcs, check=True)


@my_vcr.use_cassette(match=['all'])
def test_map_default_gcsfilesystem(token_restore):
    with gcs_maker() as gcs:
        d = GCSMap(root)
        assert d.gcs is gcs


@my_vcr.use_cassette(match=['all'])
def test_map_errors(token_restore):
    with gcs_maker() as gcs:
        d = GCSMap(root, gcs)
        with pytest.raises(KeyError):
            d['nonexistent']
        try:
            GCSMap('does-not-exist')
        except Exception as e:
            assert 'does-not-exist' in str(e)


@my_vcr.use_cassette(match=['all'])
def test_map_with_data(token_restore):
    with gcs_maker() as gcs:
        d = GCSMap(root, gcs)
        d['x'] = b'123'
        assert list(d) == list(d.keys()) == ['x']
        assert list(d.values()) == [b'123']
        assert list(d.items()) == [('x', b'123')]
        assert d['x'] == b'123'
        assert bool(d)

        assert gcs.walk(root) == [TEST_BUCKET+'/mapping/x']
        d['x'] = b'000'
        assert d['x'] == b'000'

        d['y'] = b'456'
        assert d['y'] == b'456'
        assert set(d) == {'x', 'y'}

        d.clear()
        assert list(d) == []


@my_vcr.use_cassette(match=['all'])
def test_map_complex_keys(token_restore):
    with gcs_maker() as gcs:
        d = GCSMap(root, gcs)
        d[1] = b'hello'
        assert d[1] == b'hello'
        del d[1]

        d[1, 2] = b'world'
        assert d[1, 2] == b'world'
        del d[1, 2]

        d['x', 1, 2] = b'hello world'
        assert d['x', 1, 2] == b'hello world'

        assert ('x', 1, 2) in d


@my_vcr.use_cassette(match=['all'])
def test_map_clear_empty(token_restore):
    with gcs_maker() as gcs:
        d = GCSMap(root, gcs)
        d.clear()
        assert list(d) == []
        d[1] = b'1'
        assert list(d) == ['1']
        d.clear()
        assert list(d) == []


@my_vcr.use_cassette(match=['all'])
def test_map_pickle(token_restore):
    with gcs_maker() as gcs:
        d = GCSMap(root, gcs)
        d['x'] = b'1'
        assert d['x'] == b'1'

        import pickle
        d2 = pickle.loads(pickle.dumps(d))

        assert d2['x'] == b'1'


@my_vcr.use_cassette(match=['all'])
def test_map_array(token_restore):
    with gcs_maker() as gcs:
        from array import array
        d = GCSMap(root, gcs)
        d['x'] = array('B', [65] * 1000)

        assert d['x'] == b'A' * 1000


@my_vcr.use_cassette(match=['all'])
def test_map_bytearray(token_restore):
    with gcs_maker() as gcs:
        from array import array
        d = GCSMap(root, gcs)
        d['x'] = bytearray(b'123')

        assert d['x'] == b'123'


@my_vcr.use_cassette(match=['all'])
def test_new_bucket(token_restore):
    with gcs_maker() as gcs:
        new_bucket = TEST_BUCKET + 'new-bucket'
        try:
            gcs.rmdir(new_bucket)
        except:
            pass
        with pytest.raises(Exception) as e:
            d = GCSMap(new_bucket, gcs, check=True)
        assert 'create=True' in str(e)

        try:
            d = GCSMap(new_bucket, gcs, create=True)
            assert not d

            d = GCSMap(new_bucket + '/new-directory', gcs)
            assert not d
        finally:
            gcs.rmdir(new_bucket)


@my_vcr.use_cassette(match=['all'])
def test_map_pickle(token_restore):
    import pickle
    with gcs_maker() as gcs:
        d = GCSMap(root, gcs)
        d['x'] = b'1234567890'

        b = pickle.dumps(d)
        assert b'1234567890' not in b

        e = pickle.loads(b)

        assert dict(e) == {'x': b'1234567890'}
