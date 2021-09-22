# -*- coding: utf-8 -*-

import io
from builtins import FileNotFoundError
from itertools import chain
from unittest import mock
from urllib.parse import urlparse, parse_qs, unquote
import pytest
import requests

from fsspec.utils import seek_delimiter

from gcsfs.tests.settings import (
    TEST_PROJECT,
    GOOGLE_TOKEN,
    TEST_BUCKET,
    TEST_REQUESTER_PAYS_BUCKET,
    ON_VCR,
)
from gcsfs.tests.utils import (
    tempdir,
    my_vcr,
    gcs_maker,
    files,
    csv_files,
    text_files,
    a,
    b,
    tmpfile,
)
from gcsfs.core import GCSFileSystem, quote_plus
from gcsfs.credentials import GoogleCredentials
import gcsfs.checkers
from gcsfs import __version__ as version

pytestmark = pytest.mark.usefixtures("token_restore")


@my_vcr.use_cassette(match=["all"])
def test_simple():
    assert not GoogleCredentials.tokens
    gcs = GCSFileSystem(TEST_PROJECT, token=GOOGLE_TOKEN)
    gcs.ls(TEST_BUCKET)  # no error
    gcs.ls("/" + TEST_BUCKET)  # OK to lead with '/'


@my_vcr.use_cassette(match=["all"])
def test_many_connect():
    from multiprocessing.pool import ThreadPool

    GCSFileSystem(TEST_PROJECT, token=GOOGLE_TOKEN)

    def task(i):
        GCSFileSystem(TEST_PROJECT, token=GOOGLE_TOKEN).ls("")
        return True

    pool = ThreadPool(processes=20)
    out = pool.map(task, range(40))
    assert all(out)
    pool.close()
    pool.join()


@my_vcr.use_cassette(match=["all"])
def test_many_connect_new():
    from multiprocessing.pool import ThreadPool

    def task(i):
        # first instance is made within thread - creating loop
        GCSFileSystem(TEST_PROJECT, token=GOOGLE_TOKEN).ls("")
        return True

    pool = ThreadPool(processes=20)
    out = pool.map(task, range(40))
    assert all(out)
    pool.close()
    pool.join()


@my_vcr.use_cassette(match=["all"])
def test_simple_upload():
    with gcs_maker() as gcs:
        fn = TEST_BUCKET + "/test"
        with gcs.open(fn, "wb", content_type="text/plain") as f:
            f.write(b"zz")
        with gcs.open(fn, "wb") as f:
            f.write(b"zz")
        assert gcs.cat(fn) == b"zz"


@my_vcr.use_cassette(match=["all"])
def test_large_upload():
    orig = gcsfs.core.GCS_MAX_BLOCK_SIZE
    gcsfs.core.GCS_MAX_BLOCK_SIZE = 262144  # minimum block size
    try:
        with gcs_maker() as gcs:
            fn = TEST_BUCKET + "/test"
            d = b"7123" * 262144
            with gcs.open(fn, "wb", content_type="application/octet-stream") as f:
                f.write(d)
            assert gcs.cat(fn) == d
    finally:
        gcsfs.core.GCS_MAX_BLOCK_SIZE = orig


@pytest.mark.xfail(reason="oddness in repeat VCR calls")
@my_vcr.use_cassette(match=["all"])
def test_multi_upload():
    with gcs_maker() as gcs:
        fn = TEST_BUCKET + "/test"
        d = b"01234567" * 2 ** 15

        # something to write on close
        with gcs.open(fn, "wb", content_type="text/plain", block_size=2 ** 18) as f:
            f.write(d)
            f.write(b"xx")
        assert gcs.cat(fn) == d + b"xx"
        assert gcs.info(fn)["contentType"] == "text/plain"
        # empty buffer on close
        with gcs.open(fn, "wb", content_type="text/plain", block_size=2 ** 19) as f:
            f.write(d)
            f.write(b"xx")
            f.write(d)
        assert gcs.cat(fn) == d + b"xx" + d
        assert gcs.info(fn)["contentType"] == "text/plain"

    # if content-type is not provided then default should be application/octet-stream
    with gcs_maker() as gcs:
        fn = TEST_BUCKET + "/test"
        d = b"01234567" * 2 ** 15

        # something to write on close
        with gcs.open(fn, "wb", block_size=2 ** 18) as f:
            f.write(d)
            f.write(b"xx")
        assert gcs.cat(fn) == d + b"xx"
        assert gcs.info(fn)["contentType"] == "application/octet-stream"
        # empty buffer on close
        with gcs.open(fn, "wb", block_size=2 ** 19) as f:
            f.write(d)
            f.write(b"xx")
            f.write(d)
        assert gcs.cat(fn) == d + b"xx" + d
        assert gcs.info(fn)["contentType"] == "application/octet-stream"


@my_vcr.use_cassette(match=["all"])
def test_info():
    with gcs_maker() as gcs:
        gcs.touch(a)
        assert gcs.info(a) == gcs.ls(a, detail=True)[0]


@my_vcr.use_cassette(match=["all"])
def test_ls2():
    with gcs_maker() as gcs:
        assert TEST_BUCKET + "/" in gcs.ls("")
        with pytest.raises((OSError, IOError)):
            gcs.ls("nonexistent")
        fn = TEST_BUCKET + "/test/accounts.1.json"
        gcs.touch(fn)
        assert fn in gcs.ls(TEST_BUCKET + "/test")


@my_vcr.use_cassette(match=["all"])
def test_pickle():
    import pickle

    with gcs_maker() as gcs:

        # Write data to distinct filename
        fn = TEST_BUCKET + "/nested/abcdefg"
        with gcs.open(fn, "wb") as f:
            f.write(b"1234567")

        # verify that that filename is not in the serialized form
        b = pickle.dumps(gcs)
        assert b"abcdefg" not in b
        assert b"1234567" not in b
        assert b"listing_cache" not in b

        gcs2 = pickle.loads(b)

        # since https://github.com/intake/filesystem_spec/pull/155
        assert gcs.session is gcs2.session
        gcs.touch(a)
        assert gcs.ls(TEST_BUCKET) == gcs2.ls(TEST_BUCKET)


@my_vcr.use_cassette(match=["all"])
def test_ls_touch():
    with gcs_maker() as gcs:
        assert not gcs.exists(TEST_BUCKET + "/tmp/test")

        gcs.touch(a)
        gcs.touch(b)

        L = gcs.ls(TEST_BUCKET + "/tmp/test", False)
        assert set(L) == set([a, b])

        L_d = gcs.ls(TEST_BUCKET + "/tmp/test", True)
        assert set(d["name"] for d in L_d) == set([a, b])


@my_vcr.use_cassette(match=["all"])
def test_rm():
    with gcs_maker() as gcs:
        assert not gcs.exists(a)
        gcs.touch(a)
        assert gcs.exists(a)
        gcs.rm(a)
        assert not gcs.exists(a)
        # silently ignored for now
        # with pytest.raises((OSError, IOError)):
        #    gcs.rm(TEST_BUCKET + "/nonexistent")
        with pytest.raises((OSError, IOError)):
            gcs.rm("nonexistent")


@my_vcr.use_cassette(match=["all"])
def test_rm_batch():
    with gcs_maker() as gcs:
        gcs.touch(a)
        gcs.touch(b)
        assert a in gcs.find(TEST_BUCKET)
        assert b in gcs.find(TEST_BUCKET)
        gcs.rm([a, b])
        assert a not in gcs.find(TEST_BUCKET)
        assert b not in gcs.find(TEST_BUCKET)


@my_vcr.use_cassette(match=["all"])
def test_rm_recursive():
    files = ["/a", "/a/b", "/a/c"]
    with gcs_maker() as gcs:
        for fn in files:
            gcs.touch(TEST_BUCKET + fn)
        gcs.rm(TEST_BUCKET + files[0], True)
        assert not gcs.exists(TEST_BUCKET + files[-1])


@my_vcr.use_cassette(match=["all"])
def test_file_access():
    with gcs_maker() as gcs:
        fn = TEST_BUCKET + "/nested/file1"
        data = b"hello\n"
        with gcs.open(fn, "wb") as f:
            f.write(data)
        assert gcs.cat(fn) == data
        assert gcs.head(fn, 3) == data[:3]
        assert gcs.tail(fn, 3) == data[-3:]
        assert gcs.tail(fn, 10000) == data


@my_vcr.use_cassette(match=["all"])
def test_file_info():
    with gcs_maker() as gcs:
        fn = TEST_BUCKET + "/nested/file1"
        data = b"hello\n"
        with gcs.open(fn, "wb") as f:
            f.write(data)
        assert fn in gcs.find(TEST_BUCKET)
        assert gcs.exists(fn)
        assert not gcs.exists(fn + "another")
        assert gcs.info(fn)["size"] == len(data)
        with pytest.raises((OSError, IOError)):
            gcs.info(fn + "another")


@my_vcr.use_cassette(match=["all"])
def test_du():
    with gcs_maker(True) as gcs:
        d = gcs.du(TEST_BUCKET, total=False)
        assert all(isinstance(v, int) and v >= 0 for v in d.values())
        assert TEST_BUCKET + "/nested/file1" in d

        assert gcs.du(TEST_BUCKET + "/test/", total=True) == sum(
            map(len, files.values())
        )


@my_vcr.use_cassette(match=["all"])
def test_ls():
    with gcs_maker(True) as gcs:
        fn = TEST_BUCKET + "/nested/file1"
        gcs.touch(fn)

        assert fn not in gcs.ls(TEST_BUCKET + "/")
        assert fn in gcs.ls(TEST_BUCKET + "/nested/")
        assert fn in gcs.ls(TEST_BUCKET + "/nested")


@my_vcr.use_cassette(match=["all"])
def test_ls_detail():
    with gcs_maker(True) as gcs:
        L = gcs.ls(TEST_BUCKET + "/nested", detail=True)
        assert all(isinstance(item, dict) for item in L)


@my_vcr.use_cassette(match=["all"])
def test_gcs_glob():
    with gcs_maker(True) as gcs:
        fn = TEST_BUCKET + "/nested/file1"
        assert fn not in gcs.glob(TEST_BUCKET + "/")
        assert fn not in gcs.glob(TEST_BUCKET + "/*")
        assert fn in gcs.glob(TEST_BUCKET + "/nested/")
        assert fn in gcs.glob(TEST_BUCKET + "/nested/*")
        assert fn in gcs.glob(TEST_BUCKET + "/nested/file*")
        assert fn in gcs.glob(TEST_BUCKET + "/*/*")
        assert fn in gcs.glob(TEST_BUCKET + "/**")
        assert fn in gcs.glob(TEST_BUCKET + "/**1")
        assert all(
            f in gcs.find(TEST_BUCKET)
            for f in gcs.glob(TEST_BUCKET + "/nested/*")
            if gcs.isfile(f)
        )
        # Ensure the glob only fetches prefixed folders
        gcs.dircache.clear()
        gcs.glob(TEST_BUCKET + "/nested**1")
        assert all(d.startswith(TEST_BUCKET + "/nested") for d in gcs.dircache)
        gcs.glob(TEST_BUCKET + "/test*")
        assert TEST_BUCKET + "/test" in gcs.dircache


@my_vcr.use_cassette(match=["all"])
def test_read_keys_from_bucket():
    with gcs_maker(True) as gcs:
        for k, data in files.items():
            file_contents = gcs.cat("/".join([TEST_BUCKET, k]))
            assert file_contents == data

        assert all(
            gcs.cat("/".join([TEST_BUCKET, k]))
            == gcs.cat("gcs://" + "/".join([TEST_BUCKET, k]))
            for k in files
        )


@my_vcr.use_cassette(match=["all"])
def test_url():
    with gcs_maker(True) as gcs:
        fn = TEST_BUCKET + "/nested/file1"
        url = gcs.url(fn)
        assert "http" in url
        assert quote_plus("nested/file1") in url
        with gcs.open(fn) as f:
            assert "http" in f.url()


@my_vcr.use_cassette(match=["all"])
def test_seek():
    with gcs_maker(True) as gcs:
        with gcs.open(a, "wb") as f:
            f.write(b"123")

        with gcs.open(a) as f:
            f.seek(1000)
            with pytest.raises(ValueError):
                f.seek(-1)
            with pytest.raises(ValueError):
                f.seek(-5, 2)
            with pytest.raises(ValueError):
                f.seek(0, 10)
            f.seek(0)
            assert f.read(1) == b"1"
            f.seek(0)
            assert f.read(1) == b"1"
            f.seek(3)
            assert f.read(1) == b""
            f.seek(-1, 2)
            assert f.read(1) == b"3"
            f.seek(-1, 1)
            f.seek(-1, 1)
            assert f.read(1) == b"2"
            for i in range(4):
                assert f.seek(i) == i


@my_vcr.use_cassette(match=["all"])
def test_bad_open():
    with gcs_maker() as gcs:
        with pytest.raises((IOError, OSError)):
            gcs.open("")


@my_vcr.use_cassette(match=["all"])
def test_copy():
    with gcs_maker(True) as gcs:
        fn = TEST_BUCKET + "/test/accounts.1.json"
        gcs.copy(fn, fn + "2")
        assert gcs.cat(fn) == gcs.cat(fn + "2")


@my_vcr.use_cassette(match=["all"])
def test_copy_recursive():
    with gcs_maker(True) as gcs:
        src = TEST_BUCKET + "/nested"
        dest = TEST_BUCKET + "/dest"
        gcs.copy(src, dest, recursive=True)
        for fn in gcs.ls(dest):
            if not gcs.isdir(fn):
                assert gcs.cat(fn) == gcs.cat(fn.replace("dest", "nested"))


@my_vcr.use_cassette(match=["all"])
def test_copy_errors():
    with gcs_maker(True) as gcs:

        src = TEST_BUCKET + "/test/"
        file1 = TEST_BUCKET + "/test/accounts.1.json"
        file2 = TEST_BUCKET + "/test/accounts.2.json"
        dne = TEST_BUCKET + "/test/notafile"
        dest1 = TEST_BUCKET + "/dest1/"
        dest2 = TEST_BUCKET + "/dest2/"

        # Non recursive should raise an error unless we specify ignore
        with pytest.raises(FileNotFoundError):
            gcs.copy([file1, file2, dne], dest1)

        gcs.copy([file1, file2, dne], dest1, on_error="ignore")
        assert gcs.ls(dest1) == [dest1 + "accounts.1.json", dest1 + "accounts.2.json"]

        # Recursive should raise an error only if we specify raise
        # the patch simulates the filesystem finding a file that does not exist in the directory
        current_files = gcs.expand_path(src, recursive=True)
        with mock.patch.object(gcs, "expand_path", return_value=current_files + [dne]):
            with pytest.raises(FileNotFoundError):
                gcs.copy(src, dest2, recursive=True, on_error="raise")

            gcs.copy(src, dest2, recursive=True)
            assert gcs.ls(dest2) == [
                dest2 + "accounts.1.json",
                dest2 + "accounts.2.json",
            ]


@my_vcr.use_cassette(match=["all"])
def test_move():
    with gcs_maker(True) as gcs:
        fn = TEST_BUCKET + "/test/accounts.1.json"
        data = gcs.cat(fn)
        gcs.mv(fn, fn + "2")
        assert gcs.cat(fn + "2") == data
        assert not gcs.exists(fn)


@my_vcr.use_cassette(match=["all"])
def test_cat_file():
    with gcs_maker(True) as gcs:
        fn = TEST_BUCKET + "/test/accounts.1.json"
        data = gcs.cat_file(fn)
        assert data[1:10] == gcs.cat_file(fn, start=1, end=10)
        assert data[1:] == gcs.cat_file(fn, start=1)
        assert data[:1] == gcs.cat_file(fn, end=1)


@pytest.mark.skipif(ON_VCR, reason="async fail")
@my_vcr.use_cassette(match=["all"])
@pytest.mark.parametrize("consistency", [None, "size", "md5", "crc32c"])
def test_get_put(consistency):
    if consistency == "crc32c" and gcsfs.checkers.crcmod is None:
        pytest.skip("No CRC")
    with gcs_maker(True) as gcs:
        gcs.consistency = consistency
        with tmpfile() as fn:
            gcs.get(TEST_BUCKET + "/test/accounts.1.json", fn)
            data = files["test/accounts.1.json"]
            assert open(fn, "rb").read() == data
            gcs.put(fn, TEST_BUCKET + "/temp")
            assert gcs.du(TEST_BUCKET + "/temp") == len(data)
            assert gcs.cat(TEST_BUCKET + "/temp") == data


@pytest.mark.skipif(ON_VCR, reason="async fail")
@my_vcr.use_cassette(match=["all"])
def test_get_put_list():
    with gcs_maker(True) as gcs:
        with tmpfile() as fn:
            gcs.get([TEST_BUCKET + "/test/accounts.1.json"], [fn])
            data = files["test/accounts.1.json"]
            assert open(fn, "rb").read() == data
            gcs.put([fn], [TEST_BUCKET + "/temp"])
            assert gcs.du(TEST_BUCKET + "/temp") == len(data)
            assert gcs.cat(TEST_BUCKET + "/temp") == data


@pytest.mark.skipif(ON_VCR, reason="async fail")
@pytest.mark.parametrize("protocol", ["", "gs://", "gcs://"])
@my_vcr.use_cassette(match=["all"])
def test_get_put_recursive(protocol):
    with gcs_maker(True) as gcs:
        with tempdir() as dn:
            gcs.get(protocol + TEST_BUCKET + "/test/", dn + "/temp_dir", recursive=True)
            # there is now in local directory:
            # dn+'/temp_dir/accounts.1.json'
            # dn+'/temp_dir/accounts.2.json'
            data1 = files["test/accounts.1.json"]
            data2 = files["test/accounts.2.json"]
            assert open(dn + "/temp_dir/accounts.1.json", "rb").read() == data1
            assert open(dn + "/temp_dir/accounts.2.json", "rb").read() == data2
            gcs.put(
                dn + "/temp_dir", protocol + TEST_BUCKET + "/temp_dir", recursive=True
            )
            # there is now in remote directory:
            # protocol+TEST_BUCKET+'/temp_dir/accounts.1.json'
            # protocol+TEST_BUCKET+'/temp_dir/accounts.2.json'
            assert gcs.du(protocol + TEST_BUCKET + "/temp_dir/accounts.1.json") == len(
                data1
            )
            assert (
                gcs.cat(protocol + TEST_BUCKET + "/temp_dir/accounts.1.json") == data1
            )
            assert gcs.du(protocol + TEST_BUCKET + "/temp_dir/accounts.2.json") == len(
                data2
            )
            assert (
                gcs.cat(protocol + TEST_BUCKET + "/temp_dir/accounts.2.json") == data2
            )


@pytest.mark.skipif(ON_VCR, reason="async fail")
@pytest.mark.parametrize("protocol", ["", "gs://", "gcs://"])
@my_vcr.use_cassette(match=["all"])
def test_get_put_file_in_dir(protocol):
    with gcs_maker(True) as gcs:
        with tempdir() as dn:
            gcs.get(protocol + TEST_BUCKET + "/test/", dn + "/temp_dir", recursive=True)
            # there is now in local directory:
            # dn+'/temp_dir/accounts.1.json'
            # dn+'/temp_dir/accounts.2.json'
            data1 = files["test/accounts.1.json"]
            assert open(dn + "/temp_dir/accounts.1.json", "rb").read() == data1
            gcs.put(
                dn + "/temp_dir/accounts.1.json",
                protocol + TEST_BUCKET + "/temp_dir/",
                recursive=True,
            )
            # there is now in remote directory:
            # protocol+TEST_BUCKET+'/temp_dir/accounts.1.json'
            assert gcs.du(protocol + TEST_BUCKET + "/temp_dir/accounts.1.json") == len(
                data1
            )
            assert (
                gcs.cat(protocol + TEST_BUCKET + "/temp_dir/accounts.1.json") == data1
            )


@my_vcr.use_cassette(match=["all"])
def test_errors():
    with gcs_maker() as gcs:
        with pytest.raises((IOError, OSError)):
            gcs.open(TEST_BUCKET + "/tmp/test/shfoshf", "rb")

        ## This is fine, no need for interleving directories on gcs
        # with pytest.raises((IOError, OSError)):
        #    gcs.touch('tmp/test/shfoshf/x')

        # silently ignoed for now
        # with pytest.raises((IOError, OSError)):
        #    gcs.rm(TEST_BUCKET + "/tmp/test/shfoshf/x")

        with pytest.raises((IOError, OSError)):
            gcs.mv(TEST_BUCKET + "/tmp/test/shfoshf/x", "tmp/test/shfoshf/y")

        with pytest.raises((IOError, OSError)):
            gcs.open("x", "rb")

        with pytest.raises((IOError, OSError)):
            gcs.rm("unknown")

        with pytest.raises(ValueError):
            with gcs.open(TEST_BUCKET + "/temp", "wb") as f:
                f.read()

        with pytest.raises(ValueError):
            f = gcs.open(TEST_BUCKET + "/temp", "rb")
            f.close()
            f.read()

        with pytest.raises(ValueError) as e:
            gcs.mkdir("/")
            assert "bucket" in str(e)


@my_vcr.use_cassette(match=["all"])
def test_read_small():
    with gcs_maker(True) as gcs:
        fn = TEST_BUCKET + "/2014-01-01.csv"
        with gcs.open(fn, "rb", block_size=10) as f:
            out = []
            while True:
                data = f.read(3)
                if data == b"":
                    break
                out.append(data)
            assert gcs.cat(fn) == b"".join(out)
            # cache drop
            assert len(f.cache.cache) < len(out)


@my_vcr.use_cassette(match=["all"])
def test_seek_delimiter():
    with gcs_maker(True) as gcs:
        fn = "test/accounts.1.json"
        data = files[fn]
        with gcs.open("/".join([TEST_BUCKET, fn])) as f:
            seek_delimiter(f, b"}", 0)
            assert f.tell() == 0
            f.seek(1)
            seek_delimiter(f, b"}", 5)
            assert f.tell() == data.index(b"}") + 1
            seek_delimiter(f, b"\n", 5)
            assert f.tell() == data.index(b"\n") + 1
            f.seek(1, 1)
            ind = data.index(b"\n") + data[data.index(b"\n") + 1 :].index(b"\n") + 1
            seek_delimiter(f, b"\n", 5)
            assert f.tell() == ind + 1


@my_vcr.use_cassette(match=["all"])
def test_read_block():
    with gcs_maker(True) as gcs:
        data = files["test/accounts.1.json"]
        lines = io.BytesIO(data).readlines()
        path = TEST_BUCKET + "/test/accounts.1.json"
        assert gcs.read_block(path, 1, 35, b"\n") == lines[1]
        assert gcs.read_block(path, 0, 30, b"\n") == lines[0]
        assert gcs.read_block(path, 0, 35, b"\n") == lines[0] + lines[1]
        gcs.read_block(path, 0, 5000, b"\n")
        assert gcs.read_block(path, 0, 5000, b"\n") == data
        assert len(gcs.read_block(path, 0, 5)) == 5
        assert len(gcs.read_block(path, 4, 5000)) == len(data) - 4
        assert gcs.read_block(path, 5000, 5010) == b""

        assert gcs.read_block(path, 5, None) == gcs.read_block(path, 5, 1000)


@my_vcr.use_cassette(match=["all"])
def test_flush():
    with gcs_maker() as gcs:
        gcs.touch(a)
        with gcs.open(a, "rb") as ro:
            with pytest.raises(ValueError):
                ro.write(b"abc")

            ro.flush()

        with gcs.open(b, "wb") as wo:
            wo.write(b"abc")
            wo.flush()
            assert not gcs.exists(b)

        assert gcs.exists(b)
        with pytest.raises(ValueError):
            wo.write(b"abc")


@my_vcr.use_cassette(match=["all"])
def test_write_fails():
    with gcs_maker() as gcs:
        with pytest.raises(ValueError):
            gcs.touch(TEST_BUCKET + "/temp")
            gcs.open(TEST_BUCKET + "/temp", "rb").write(b"hello")

            with gcs.open(TEST_BUCKET + "/temp", "wb") as f:
                f.write(b"hello")
                f.flush(force=True)
            with pytest.raises(ValueError):
                f.write(b"world")

        f = gcs.open(TEST_BUCKET + "/temp", "wb")
        f.close()
        with pytest.raises(ValueError):
            f.write(b"hello")
        with pytest.raises((OSError, IOError)):
            gcs.open("nonexistentbucket/temp", "wb").close()


@my_vcr.use_cassette(match=["all"])
def text_mode():
    text = "Hello µ"
    with gcs_maker() as gcs:
        with gcs.open(TEST_BUCKET + "/temp", "w") as f:
            f.write(text)
        with gcs.open(TEST_BUCKET + "/temp", "r") as f:
            assert f.read() == text


@my_vcr.use_cassette(match=["all"])
def test_write_blocks():
    with gcs_maker() as gcs:
        with gcs.open(TEST_BUCKET + "/temp", "wb", block_size=2 ** 18) as f:
            f.write(b"a" * 100000)
            assert f.buffer.tell() == 100000
            assert not (f.offset)
            f.write(b"a" * 100000)
            f.write(b"a" * 100000)
            assert f.offset
        assert gcs.info(TEST_BUCKET + "/temp")["size"] == 300000


@my_vcr.use_cassette(match=["all"])
def test_write_blocks2():
    with gcs_maker() as gcs:
        with gcs.open(TEST_BUCKET + "/temp1", "wb", block_size=2 ** 18) as f:
            f.write(b"a" * (2 ** 18 + 1))
            # leftover bytes: GCS accepts blocks in multiples of 2**18 bytes
            assert f.buffer.tell() == 1
        assert gcs.info(TEST_BUCKET + "/temp1")["size"] == 2 ** 18 + 1


@my_vcr.use_cassette(match=["all"])
def test_readline():
    with gcs_maker(True) as gcs:
        all_items = chain.from_iterable(
            [files.items(), csv_files.items(), text_files.items()]
        )
        for k, data in all_items:
            with gcs.open("/".join([TEST_BUCKET, k]), "rb") as f:
                result = f.readline()
                expected = data.split(b"\n")[0] + (b"\n" if data.count(b"\n") else b"")
            assert result == expected


@my_vcr.use_cassette(match=["all"])
def test_readline_from_cache():
    with gcs_maker() as gcs:
        data = b"a,b\n11,22\n3,4"
        with gcs.open(a, "wb") as f:
            f.write(data)

        with gcs.open(a, "rb") as f:
            result = f.readline()
            assert result == b"a,b\n"
            assert f.loc == 4
            assert f.cache.cache == data

            result = f.readline()
            assert result == b"11,22\n"
            assert f.loc == 10
            assert f.cache.cache == data

            result = f.readline()
            assert result == b"3,4"
            assert f.loc == 13
            assert f.cache.cache == data


@my_vcr.use_cassette(match=["all"])
def test_readline_empty():
    with gcs_maker() as gcs:
        data = b""
        with gcs.open(a, "wb") as f:
            f.write(data)
        with gcs.open(a, "rb") as f:
            result = f.readline()
            assert result == data


@my_vcr.use_cassette(match=["all"])
def test_readline_blocksize():
    with gcs_maker() as gcs:
        data = b"ab\n" + b"a" * (2 ** 18) + b"\nab"
        with gcs.open(a, "wb") as f:
            f.write(data)
        with gcs.open(a, "rb", block_size=2 ** 18) as f:
            result = f.readline()
            expected = b"ab\n"
            assert result == expected

            result = f.readline()
            expected = b"a" * (2 ** 18) + b"\n"
            assert result == expected

            result = f.readline()
            expected = b"ab"
            assert result == expected


@my_vcr.use_cassette(match=["all"])
def test_next():
    with gcs_maker(True) as gcs:
        expected = csv_files["2014-01-01.csv"].split(b"\n")[0] + b"\n"
        with gcs.open(TEST_BUCKET + "/2014-01-01.csv") as f:
            result = next(f)
            assert result == expected


@my_vcr.use_cassette(match=["all"])
def test_iterable():
    with gcs_maker() as gcs:
        data = b"abc\n123"
        with gcs.open(a, "wb") as f:
            f.write(data)
        with gcs.open(a) as f, io.BytesIO(data) as g:
            for fromgcs, fromio in zip(f, g):
                assert fromgcs == fromio
            f.seek(0)
            assert f.readline() == b"abc\n"
            assert f.readline() == b"123"
            f.seek(1)
            assert f.readline() == b"bc\n"

        with gcs.open(a) as f:
            out = list(f)
        with gcs.open(a) as f:
            out2 = f.readlines()
        assert out == out2
        assert b"".join(out) == data


@my_vcr.use_cassette(match=["all"])
def test_readable():
    with gcs_maker() as gcs:
        with gcs.open(a, "wb") as f:
            assert not f.readable()

        with gcs.open(a, "rb") as f:
            assert f.readable()


@my_vcr.use_cassette(match=["all"])
def test_seekable():
    with gcs_maker() as gcs:
        with gcs.open(a, "wb") as f:
            assert not f.seekable()

        with gcs.open(a, "rb") as f:
            assert f.seekable()


@my_vcr.use_cassette(match=["all"])
def test_writable():
    with gcs_maker() as gcs:
        with gcs.open(a, "wb") as f:
            assert f.writable()

        with gcs.open(a, "rb") as f:
            assert not f.writable()


@my_vcr.use_cassette(match=["all"])
def test_merge():
    with gcs_maker() as gcs:
        with gcs.open(a, "wb") as f:
            f.write(b"a" * 10)

        with gcs.open(b, "wb") as f:
            f.write(b"a" * 10)
        gcs.merge(TEST_BUCKET + "/joined", [a, b])
        assert gcs.info(TEST_BUCKET + "/joined")["size"] == 20


@my_vcr.use_cassette(match=["all"])
def test_bigger_than_block_read():
    with gcs_maker(True) as gcs:
        with gcs.open(TEST_BUCKET + "/2014-01-01.csv", "rb", block_size=3) as f:
            out = []
            while True:
                data = f.read(20)
                out.append(data)
                if len(data) == 0:
                    break
        assert b"".join(out) == csv_files["2014-01-01.csv"]


@my_vcr.use_cassette(match=["all"])
def test_current():
    with gcs_maker() as gcs:
        assert GCSFileSystem.current() is gcs
        gcs2 = GCSFileSystem(TEST_PROJECT, token=GOOGLE_TOKEN)
        assert gcs2.session is gcs.session


@my_vcr.use_cassette(match=["all"])
def test_array():
    with gcs_maker() as gcs:
        from array import array

        data = array("B", [65] * 1000)

        with gcs.open(a, "wb") as f:
            f.write(data)

        with gcs.open(a, "rb") as f:
            out = f.read()
            assert out == b"A" * 1000


@my_vcr.use_cassette(match=["all"])
def test_attrs():
    with gcs_maker() as gcs:
        gcs.touch(a)
        assert "metadata" not in gcs.info(a)
        with pytest.raises(KeyError):
            gcs.getxattr(a, "foo")

        gcs.touch(a, metadata={"foo": "blob"})
        assert gcs.getxattr(a, "foo") == "blob"

        gcs.setxattrs(a, foo="blah")
        assert gcs.getxattr(a, "foo") == "blah"

        with gcs.open(a, "wb") as f:
            f.metadata = {"something": "not"}

        with pytest.raises(KeyError):
            gcs.getxattr(a, "foo")
        assert gcs.getxattr(a, "something") == "not"


@my_vcr.use_cassette(match=["all"])
def test_request_user_project():
    with gcs_maker():
        gcs = GCSFileSystem(TEST_PROJECT, token=GOOGLE_TOKEN, requester_pays=True)
        # test directly against `_call` to inspect the result
        r = gcs.call(
            "GET",
            "b/{}/o/",
            TEST_REQUESTER_PAYS_BUCKET,
            delimiter="/",
            prefix="test",
            maxResults=100,
            info_out=True,
        )
        qs = urlparse(r.url.human_repr()).query
        result = parse_qs(qs)
        assert result["userProject"] == [TEST_PROJECT]


@my_vcr.use_cassette(match=["all"])
def test_request_user_project_string():
    with gcs_maker():
        gcs = GCSFileSystem(
            TEST_PROJECT, token=GOOGLE_TOKEN, requester_pays=TEST_PROJECT
        )
        assert gcs.requester_pays == TEST_PROJECT
        # test directly against `_call` to inspect the result
        r = gcs.call(
            "GET",
            "b/{}/o/",
            TEST_REQUESTER_PAYS_BUCKET,
            delimiter="/",
            prefix="test",
            maxResults=100,
            info_out=True,
        )
        qs = urlparse(r.url.human_repr()).query
        result = parse_qs(qs)
        assert result["userProject"] == [TEST_PROJECT]


@my_vcr.use_cassette(match=["all"])
def test_request_header():
    with gcs_maker():
        gcs = GCSFileSystem(TEST_PROJECT, token=GOOGLE_TOKEN, requester_pays=True)
        # test directly against `_call` to inspect the result
        r = gcs.call(
            "GET",
            "b/{}/o/",
            TEST_REQUESTER_PAYS_BUCKET,
            delimiter="/",
            prefix="test",
            maxResults=100,
            info_out=True,
        )
        assert r.headers["User-Agent"] == "python-gcsfs/" + version


@mock.patch("gcsfs.credentials.gauth")
def test_user_project_fallback_google_default(mock_auth):
    mock_auth.default.return_value = (requests.Session(), "my_default_project")
    fs = GCSFileSystem(token="google_default")
    assert fs.project == "my_default_project"


@my_vcr.use_cassette(match=["all"])
def test_user_project_cat():
    gcs = GCSFileSystem(TEST_PROJECT, token=GOOGLE_TOKEN, requester_pays=True)
    result = gcs.cat(TEST_REQUESTER_PAYS_BUCKET + "/foo.csv")
    assert len(result)


@mock.patch("gcsfs.credentials.gauth")
def test_raise_on_project_mismatch(mock_auth):
    mock_auth.default.return_value = (requests.Session(), "my_other_project")
    match = "'my_project' does not match the google default project 'my_other_project'"
    with pytest.raises(ValueError, match=match):
        GCSFileSystem(project="my_project", token="google_default")

    result = GCSFileSystem(token="google_default")
    assert result.project == "my_other_project"


@my_vcr.use_cassette(match=["all"])
def test_ls_prefix_cache():
    with gcs_maker(True) as gcs:
        gcs.touch(f"gs://{TEST_BUCKET}/a/file1")
        gcs.touch(f"gs://{TEST_BUCKET}/a/file2")

        gcs.ls(f"gs://{TEST_BUCKET}/", prefix="a/file")
        gcs.info(f"gs://{TEST_BUCKET}/a/file1")


@my_vcr.use_cassette(match=["all"])
def test_placeholder_dir_cache_validity():
    with gcs_maker(True) as gcs:
        gcs.touch(f"gs://{TEST_BUCKET}/a/")
        gcs.touch(f"gs://{TEST_BUCKET}/a/file")
        gcs.touch(f"gs://{TEST_BUCKET}/b")

        gcs.find(f"gs://{TEST_BUCKET}/a/")
        gcs.info(f"gs://{TEST_BUCKET}/b")


@my_vcr.use_cassette(match=["all"])
def test_pseudo_dir_find():
    with gcs_maker(False) as fs:
        fs.touch(f"{TEST_BUCKET}/a/b/file")
        b = set(fs.glob(f"{TEST_BUCKET}/a/*"))
        assert f"{TEST_BUCKET}/a/b" in b
        a = set(fs.glob(f"{TEST_BUCKET}/*"))
        assert f"{TEST_BUCKET}/a" in a
        assert fs.find(TEST_BUCKET) == [f"{TEST_BUCKET}/a/b/file"]
        assert fs.find(f"{TEST_BUCKET}/a", withdirs=True) == [
            f"{TEST_BUCKET}/a",
            f"{TEST_BUCKET}/a/b",
            f"{TEST_BUCKET}/a/b/file",
        ]


@my_vcr.use_cassette(match=["all"])
def test_zero_cache_timeout():
    with gcs_maker(True, cache_timeout=0) as gcs:
        gcs.touch(f"gs://{TEST_BUCKET}/a/file")
        gcs.find(f"gs://{TEST_BUCKET}/a/")
        gcs.info(f"gs://{TEST_BUCKET}/a/file")
        gcs.ls(f"gs://{TEST_BUCKET}/a/")

        # The _times entry and exception below should only be present after
        # https://github.com/intake/filesystem_spec/pull/513.
        if f"{TEST_BUCKET}/a" not in gcs.dircache._times:
            pytest.skip("fsspec version too early")

        with pytest.raises(KeyError):
            gcs.dircache[f"{TEST_BUCKET}/a"]


@my_vcr.use_cassette(match=["all"])
def test_find_with_prefix_partial_cache():
    base_dir = f"{TEST_BUCKET}/test_find_with_prefix"
    with gcs_maker(False) as gcs:
        gcs.touch(base_dir + "/test_1")
        gcs.touch(base_dir + "/test_2")

        for with_cache in (True, False):
            # Test once with cached, and once with no cache
            gcs.invalidate_cache()
            if with_cache:
                gcs.ls(base_dir)
            assert gcs.find(base_dir, prefix="non_existent_") == []
            assert gcs.find(base_dir, prefix="test_") == [
                base_dir + "/test_1",
                base_dir + "/test_2",
            ]
            assert gcs.find(base_dir + "/test_1") == [base_dir + "/test_1"]
            assert gcs.find(base_dir + "/non_existent") == []
            assert (
                gcs.find(base_dir + "/non_existent", prefix="more_non_existent") == []
            )


@my_vcr.use_cassette(match=["all"])
def test_percent_file_name():
    with gcs_maker(False) as gcs:
        parent = f"{TEST_BUCKET}/test/onefile"
        fn = f"{parent}/a%25.txt"
        data = b"zz"
        with gcs.open(fn, "wb", content_type="text/plain") as f:
            f.write(data)
        assert gcs.cat(fn) == data
        fn2 = unquote(fn)
        gcs.touch(fn2)
        assert gcs.cat(fn2) != data
        assert set(gcs.ls(parent)) == set([fn, fn2])
