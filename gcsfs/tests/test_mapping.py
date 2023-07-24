import pytest

from gcsfs.tests.settings import TEST_BUCKET

MAPPING_ROOT = TEST_BUCKET + "/mapping"


def test_api():
    import gcsfs

    assert "GCSMap" in dir(gcsfs)
    assert "mapping" in dir(gcsfs)


def test_map_simple(gcs):
    d = gcs.get_mapper(MAPPING_ROOT)
    assert not d

    assert list(d) == list(d.keys()) == []
    assert list(d.values()) == []
    assert list(d.items()) == []


def test_map_default_gcsfilesystem(gcs):
    d = gcs.get_mapper(MAPPING_ROOT)
    assert d.fs is gcs


def test_map_errors(gcs):
    d = gcs.get_mapper(MAPPING_ROOT)
    with pytest.raises(KeyError):
        d["nonexistent"]
    try:
        gcs.get_mapper("does-not-exist")
    except Exception as e:
        assert "does-not-exist" in str(e)


def test_map_with_data(gcs):
    d = gcs.get_mapper(MAPPING_ROOT)
    d["x"] = b"123"
    assert list(d) == list(d.keys()) == ["x"]
    assert list(d.values()) == [b"123"]
    assert list(d.items()) == [("x", b"123")]
    assert d["x"] == b"123"
    assert bool(d)

    assert gcs.find(MAPPING_ROOT) == [TEST_BUCKET + "/mapping/x"]
    d["x"] = b"000"
    assert d["x"] == b"000"

    d["y"] = b"456"
    assert d["y"] == b"456"
    assert set(d) == {"x", "y"}

    d.clear()
    assert list(d) == []


def test_map_clear_empty(gcs):
    d = gcs.get_mapper(MAPPING_ROOT)
    d.clear()
    assert list(d) == []
    d["1"] = b"1"
    assert list(d) == ["1"] or list(d) == ["1"]
    d.clear()
    assert list(d) == []


def test_map_pickle(gcs):
    d = gcs.get_mapper(MAPPING_ROOT)
    d["x"] = b"1"
    assert d["x"] == b"1"

    import pickle

    d2 = pickle.loads(pickle.dumps(d))

    assert d2["x"] == b"1"


def test_map_array(gcs):
    from array import array

    d = gcs.get_mapper(MAPPING_ROOT)
    d["x"] = array("B", [65] * 1000)

    assert d["x"] == b"A" * 1000


def test_map_bytearray(gcs):
    d = gcs.get_mapper(MAPPING_ROOT)
    d["x"] = bytearray(b"123")

    assert d["x"] == b"123"


def test_new_bucket(gcs):
    new_bucket = TEST_BUCKET + "new-bucket"
    try:
        gcs.rmdir(new_bucket)
    except:  # noqa: E722
        pass
    with pytest.raises(Exception) as e:
        d = gcs.get_mapper(new_bucket, check=True)
    assert "create=True" in str(e.value)

    try:
        d = gcs.get_mapper(new_bucket, create=True)
        assert not d

        d = gcs.get_mapper(new_bucket + "/new-directory")
        assert not d
    finally:
        gcs.rmdir(new_bucket)


def test_map_pickle(gcs):
    import pickle

    d = gcs.get_mapper(MAPPING_ROOT)
    d["x"] = b"1234567890"

    b = pickle.dumps(d)
    assert b"1234567890" not in b

    e = pickle.loads(b)

    assert dict(e) == {"x": b"1234567890"}
