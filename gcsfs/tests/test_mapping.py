import pytest
from gcsfs.tests.settings import TEST_BUCKET
from gcsfs.tests.utils import my_vcr, gcs_maker

root = TEST_BUCKET + "/mapping"


pytestmark = pytest.mark.usefixtures("token_restore")


def test_api():
    import gcsfs

    assert "GCSMap" in dir(gcsfs)
    assert "mapping" in dir(gcsfs)


@my_vcr.use_cassette(match=["all"])
def test_map_simple():
    with gcs_maker() as gcs:
        d = gcs.get_mapper(root)
        assert not d

        assert list(d) == list(d.keys()) == []
        assert list(d.values()) == []
        assert list(d.items()) == []


@my_vcr.use_cassette(match=["all"])
def test_map_default_gcsfilesystem():
    with gcs_maker() as gcs:
        d = gcs.get_mapper(root)
        assert d.fs is gcs


@my_vcr.use_cassette(match=["all"])
def test_map_errors():
    with gcs_maker() as gcs:
        d = gcs.get_mapper(root)
        with pytest.raises(KeyError):
            d["nonexistent"]
        try:
            gcs.get_mapper("does-not-exist")
        except Exception as e:
            assert "does-not-exist" in str(e)


@pytest.mark.xfail(reason="only passes for the py version where it was recorded")
@my_vcr.use_cassette(match=["all"])
def test_map_with_data():
    with gcs_maker() as gcs:
        d = gcs.get_mapper(root)
        d["x"] = b"123"
        assert list(d) == list(d.keys()) == ["x"]
        assert list(d.values()) == [b"123"]
        assert list(d.items()) == [("x", b"123")]
        assert d["x"] == b"123"
        assert bool(d)

        assert gcs.find(root) == [TEST_BUCKET + "/mapping/x"]
        d["x"] = b"000"
        assert d["x"] == b"000"

        d["y"] = b"456"
        assert d["y"] == b"456"
        assert set(d) == {"x", "y"}

        d.clear()
        assert list(d) == []


@my_vcr.use_cassette(match=["all"])
def test_map_complex_keys():
    with gcs_maker() as gcs:
        d = gcs.get_mapper(root)
        d[1] = b"hello"
        assert d[1] == b"hello"
        del d[1]

        d[1, 2] = b"world"
        assert d[1, 2] == b"world"
        del d[1, 2]

        d["x", 1, 2] = b"hello world"
        assert d["x", 1, 2] == b"hello world"

        assert ("x", 1, 2) in d


@my_vcr.use_cassette(match=["all"])
def test_map_clear_empty():
    with gcs_maker() as gcs:
        d = gcs.get_mapper(root)
        d.clear()
        assert list(d) == []
        d[1] = b"1"
        # may repeat the test below, since VCR sometimes picks the wrong call to ls
        assert list(d) == ["1"] or list(d) == ["1"]
        d.clear()
        assert list(d) == []


@my_vcr.use_cassette(match=["all"])
def test_map_pickle():
    with gcs_maker() as gcs:
        d = gcs.get_mapper(root)
        d["x"] = b"1"
        assert d["x"] == b"1"

        import pickle

        d2 = pickle.loads(pickle.dumps(d))

        assert d2["x"] == b"1"


@my_vcr.use_cassette(match=["all"])
def test_map_array():
    with gcs_maker() as gcs:
        from array import array

        d = gcs.get_mapper(root)
        d["x"] = array("B", [65] * 1000)

        assert d["x"] == b"A" * 1000


@my_vcr.use_cassette(match=["all"])
def test_map_bytearray():
    with gcs_maker() as gcs:
        d = gcs.get_mapper(root)
        d["x"] = bytearray(b"123")

        assert d["x"] == b"123"


@my_vcr.use_cassette(match=["all"])
def test_new_bucket():
    with gcs_maker() as gcs:
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


@my_vcr.use_cassette(match=["all"])
def test_map_pickle():
    import pickle

    with gcs_maker() as gcs:
        d = gcs.get_mapper(root)
        d["x"] = b"1234567890"

        b = pickle.dumps(d)
        assert b"1234567890" not in b

        e = pickle.loads(b)

        assert dict(e) == {"x": b"1234567890"}
