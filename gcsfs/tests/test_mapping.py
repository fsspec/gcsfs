import pytest
from gcsfs.tests.utils import gcs, token_restore
from gcsfs.tests.settings import TEST_BUCKET as test_bucket_name
from gcsfs import GCSMap, GCSFileSystem

root = test_bucket_name+'/mapping'


def test_simple(gcs):
    d = GCSMap(root, gcs)
    assert not d

    assert list(d) == list(d.keys()) == []
    assert list(d.values()) == []
    assert list(d.items()) == []
    d = GCSMap(root, gcs, check=True)


def test_default_gcsfilesystem(gcs):
    d = GCSMap(root)
    assert d.gcs is gcs


def test_errors(gcs):
    d = GCSMap(root, gcs)
    with pytest.raises(KeyError):
        d['nonexistent']

    try:
        GCSMap('does-not-exist')
    except Exception as e:
        assert 'does-not-exist' in str(e)


def test_with_data(gcs):
    d = GCSMap(root, gcs)
    d['x'] = b'123'
    assert list(d) == list(d.keys()) == ['x']
    assert list(d.values()) == [b'123']
    assert list(d.items()) == [('x', b'123')]
    assert d['x'] == b'123'
    assert bool(d)

    assert gcs.walk(root) == [test_bucket_name+'/mapping/x']
    d['x'] = b'000'
    assert d['x'] == b'000'

    d['y'] = b'456'
    assert d['y'] == b'456'
    assert set(d) == {'x', 'y'}

    d.clear()
    assert list(d) == []


def test_complex_keys(gcs):
    d = GCSMap(root, gcs)
    d[1] = b'hello'
    assert d[1] == b'hello'
    del d[1]

    d[1, 2] = b'world'
    assert d[1, 2] == b'world'
    del d[1, 2]

    d['x', 1, 2] = b'hello world'
    assert d['x', 1, 2] == b'hello world'
    print(list(d))

    assert ('x', 1, 2) in d


def test_clear_empty(gcs):
    d = GCSMap(root, gcs)
    d.clear()
    assert list(d) == []
    d[1] = b'1'
    assert list(d) == ['1']
    d.clear()
    assert list(d) == []


def test_pickle(gcs):
    d = GCSMap(root, gcs)
    d['x'] = b'1'

    import pickle
    d2 = pickle.loads(pickle.dumps(d))

    assert d2['x'] == b'1'


def test_array(gcs):
    from array import array
    d = GCSMap(root, gcs)
    d['x'] = array('B', [65] * 1000)

    assert d['x'] == b'A' * 1000


def test_bytearray(gcs):
    from array import array
    d = GCSMap(root, gcs)
    d['x'] = bytearray(b'123')

    assert d['x'] == b'123'


def test_new_bucket(gcs):
    new_bucket = test_bucket_name + 'new-bucket'
    try:
        gcs.rmdir(new_bucket)
    except:
        pass
    with pytest.raises(ValueError) as e:
        d = GCSMap(new_bucket, gcs)
    assert 'create=True' in str(e)

    d = GCSMap(new_bucket, gcs, create=True)
    assert not d

    d = GCSMap(new_bucket + '/new-directory', gcs)
    assert not d
