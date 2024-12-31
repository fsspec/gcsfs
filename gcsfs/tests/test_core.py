import io
import os
from builtins import FileNotFoundError
from datetime import datetime, timezone
from itertools import chain
from unittest import mock
from urllib.parse import parse_qs, unquote, urlparse
from uuid import uuid4

import fsspec.core
import pytest
import requests
from fsspec.asyn import sync
from fsspec.utils import seek_delimiter

import gcsfs.checkers
import gcsfs.tests.settings
from gcsfs import __version__ as version
from gcsfs.core import GCSFileSystem, quote
from gcsfs.credentials import GoogleCredentials
from gcsfs.tests.conftest import a, allfiles, b, csv_files, files, text_files
from gcsfs.tests.utils import tempdir, tmpfile

TEST_BUCKET = gcsfs.tests.settings.TEST_BUCKET
TEST_PROJECT = gcsfs.tests.settings.TEST_PROJECT
TEST_REQUESTER_PAYS_BUCKET = gcsfs.tests.settings.TEST_REQUESTER_PAYS_BUCKET


def test_simple(gcs, monkeypatch):
    monkeypatch.setattr(GoogleCredentials, "tokens", None)
    gcs.ls(TEST_BUCKET)  # no error
    gcs.ls("/" + TEST_BUCKET)  # OK to lead with '/'


def test_dircache_filled(gcs):
    assert not dict(gcs.dircache)
    gcs.ls(TEST_BUCKET)
    assert len(gcs.dircache) == 1
    gcs.dircache[TEST_BUCKET][0]["CHECK"] = True
    out = gcs.ls(TEST_BUCKET, detail=True)
    assert [o for o in out if o.get("CHECK", None)]

    gcs.invalidate_cache()
    assert not dict(gcs.dircache)

    gcs.find(TEST_BUCKET)
    assert len(gcs.dircache)


def test_many_connect(docker_gcs):
    from multiprocessing.pool import ThreadPool

    GCSFileSystem(endpoint_url=docker_gcs)

    def task(i):
        GCSFileSystem(endpoint_url=docker_gcs).ls("")
        return True

    pool = ThreadPool(processes=20)
    out = pool.map(task, range(40))
    assert all(out)
    pool.close()
    pool.join()


def test_many_connect_new(docker_gcs):
    from multiprocessing.pool import ThreadPool

    def task(i):
        # first instance is made within thread - creating loop
        GCSFileSystem(endpoint_url=docker_gcs).ls("")
        return True

    pool = ThreadPool(processes=20)
    out = pool.map(task, range(40))
    assert all(out)
    pool.close()
    pool.join()


def test_simple_upload(gcs):
    fn = TEST_BUCKET + "/test"
    with gcs.open(fn, "wb", content_type="text/plain") as f:
        f.write(b"zz")
    with gcs.open(fn, "wb") as f:
        f.write(b"zz")
    assert gcs.cat(fn) == b"zz"


def test_large_upload(gcs):
    orig = gcsfs.core.GCS_MAX_BLOCK_SIZE
    gcsfs.core.GCS_MAX_BLOCK_SIZE = 262144  # minimum block size
    try:
        fn = TEST_BUCKET + "/test"
        d = b"7123" * 262144
        with gcs.open(fn, "wb", content_type="application/octet-stream") as f:
            f.write(d)
        assert gcs.cat(fn) == d
    finally:
        gcsfs.core.GCS_MAX_BLOCK_SIZE = orig


def test_multi_upload(gcs):
    fn = TEST_BUCKET + "/test"
    d = b"01234567" * 2**15

    # something to write on close
    with gcs.open(fn, "wb", content_type="text/plain", block_size=2**18) as f:
        f.write(d)
        f.write(b"xx")
    assert gcs.cat(fn) == d + b"xx"
    assert gcs.info(fn)["contentType"] == "text/plain"
    # empty buffer on close
    with gcs.open(fn, "wb", content_type="text/plain", block_size=2**19) as f:
        f.write(d)
        f.write(b"xx")
        f.write(d)
    assert gcs.cat(fn) == d + b"xx" + d
    assert gcs.info(fn)["contentType"] == "text/plain"

    fn = TEST_BUCKET + "/test"
    d = b"01234567" * 2**15

    # something to write on close
    with gcs.open(fn, "wb", block_size=2**18) as f:
        f.write(d)
        f.write(b"xx")
    assert gcs.cat(fn) == d + b"xx"
    assert gcs.info(fn)["contentType"] == "application/octet-stream"
    # empty buffer on close
    with gcs.open(fn, "wb", block_size=2**19) as f:
        f.write(d)
        f.write(b"xx")
        f.write(d)
    assert gcs.cat(fn) == d + b"xx" + d
    assert gcs.info(fn)["contentType"] == "application/octet-stream"


def test_info(gcs):
    gcs.touch(a)
    assert gcs.info(a) == gcs.ls(a, detail=True)[0]

    today = datetime.utcnow().date().isoformat()
    assert gcs.created(a).isoformat().startswith(today)
    assert gcs.modified(a).isoformat().startswith(today)
    # Check conformance with expected info attribute names.
    assert gcs.info(a)["ctime"] == gcs.created(a)
    assert gcs.info(a)["mtime"] == gcs.modified(a)


def test_ls2(gcs):
    assert TEST_BUCKET + "/" in gcs.ls("")
    with pytest.raises((OSError, IOError)):
        gcs.ls("nonexistent")
    fn = TEST_BUCKET + "/test/accounts.1.json"
    gcs.touch(fn)
    assert fn in gcs.ls(TEST_BUCKET + "/test")


def test_pickle(gcs):
    import pickle

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

    # since https://github.com/fsspec/filesystem_spec/pull/155
    assert gcs.session is gcs2.session
    gcs.touch(a)
    assert gcs.ls(TEST_BUCKET) == gcs2.ls(TEST_BUCKET)


def test_ls_touch(gcs):
    assert not gcs.exists(TEST_BUCKET + "/tmp/test")

    gcs.touch(a)
    gcs.touch(b)

    L = gcs.ls(TEST_BUCKET + "/tmp/test", False)
    assert set(L) == {a, b}

    L_d = gcs.ls(TEST_BUCKET + "/tmp/test", True)
    assert {d["name"] for d in L_d} == {a, b}


def test_rm(gcs):
    assert not gcs.exists(a)
    gcs.touch(a)
    assert gcs.exists(a)
    gcs.rm(a)
    assert not gcs.exists(a)
    with pytest.raises((OSError, IOError)):
        gcs.rm(TEST_BUCKET + "/nonexistent")
    with pytest.raises((OSError, IOError)):
        gcs.rm("nonexistent")


def test_rm_batch(gcs):
    gcs.touch(a)
    gcs.touch(b)
    assert a in gcs.find(TEST_BUCKET)
    assert b in gcs.find(TEST_BUCKET)
    gcs.rm([a, b])
    assert a not in gcs.find(TEST_BUCKET)
    assert b not in gcs.find(TEST_BUCKET)


def test_rm_recursive(gcs):
    files = ["/a", "/a/b", "/a/c"]
    for fn in files:
        gcs.touch(TEST_BUCKET + fn)
    gcs.rm(TEST_BUCKET + files[0], True)
    assert not gcs.exists(TEST_BUCKET + files[-1])


def test_rm_chunked_batch(gcs):
    files = [f"{TEST_BUCKET}/t{i}" for i in range(303)]
    for fn in files:
        gcs.touch(fn)

    files_created = gcs.find(TEST_BUCKET)
    for fn in files:
        assert fn in files_created

    gcs.rm(files)

    files_removed = gcs.find(TEST_BUCKET)
    for fn in files:
        assert fn not in files_removed


def test_file_access(gcs):
    fn = TEST_BUCKET + "/nested/file1"
    data = b"hello\n"
    with gcs.open(fn, "wb") as f:
        f.write(data)
    assert gcs.cat(fn) == data
    assert gcs.head(fn, 3) == data[:3]
    assert gcs.tail(fn, 3) == data[-3:]
    assert gcs.tail(fn, 10000) == data


def test_file_info(gcs):
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


def test_du(gcs):
    d = gcs.du(TEST_BUCKET, total=False)
    assert all(isinstance(v, int) and v >= 0 for v in d.values())
    assert TEST_BUCKET + "/nested/file1" in d

    assert gcs.du(TEST_BUCKET + "/test/", total=True) == sum(map(len, files.values()))


def test_ls(gcs):
    fn = TEST_BUCKET + "/nested/file1"
    gcs.touch(fn)

    assert fn not in gcs.ls(TEST_BUCKET + "/")
    assert fn in gcs.ls(TEST_BUCKET + "/nested/")
    assert fn in gcs.ls(TEST_BUCKET + "/nested")


def test_ls_detail(gcs):
    L = gcs.ls(TEST_BUCKET + "/nested", detail=True)
    assert all(isinstance(item, dict) for item in L)


@pytest.mark.parametrize("refresh", (False, True))
def test_ls_refresh(gcs, refresh):
    with mock.patch.object(gcs, "invalidate_cache") as mock_invalidate_cache:
        gcs.ls(TEST_BUCKET, refresh=refresh)
    assert mock_invalidate_cache.called is refresh


def test_gcs_glob(gcs):
    fn = TEST_BUCKET + "/nested/file1"
    assert fn not in gcs.glob(TEST_BUCKET + "/")
    assert fn not in gcs.glob(TEST_BUCKET + "/*")
    assert fn not in gcs.glob(TEST_BUCKET + "/nested/")
    assert fn in gcs.glob(TEST_BUCKET + "/nested/*")
    assert fn in gcs.glob(TEST_BUCKET + "/nested/file*")
    assert fn in gcs.glob(TEST_BUCKET + "/*/*")
    assert fn in gcs.glob(TEST_BUCKET + "/**")
    assert fn in gcs.glob(TEST_BUCKET + "/**/*1")
    assert all(
        f in gcs.find(TEST_BUCKET)
        for f in gcs.glob(TEST_BUCKET + "/nested/*")
        if gcs.isfile(f)
    )
    # the following is no longer true since the glob method list the root path
    # Ensure the glob only fetches prefixed folders
    # gcs.dircache.clear()
    # gcs.glob(TEST_BUCKET + "/nested**1")
    # assert all(d.startswith(TEST_BUCKET + "/nested") for d in gcs.dircache)
    # the following is no longer true as of #437
    # gcs.glob(TEST_BUCKET + "/test*")
    # assert TEST_BUCKET + "/test" in gcs.dircache


def test_read_keys_from_bucket(gcs):
    for k, data in files.items():
        file_contents = gcs.cat("/".join([TEST_BUCKET, k]))
        assert file_contents == data

    assert all(
        gcs.cat("/".join([TEST_BUCKET, k]))
        == gcs.cat("gcs://" + "/".join([TEST_BUCKET, k]))
        for k in files
    )


def test_url(gcs):
    fn = TEST_BUCKET + "/nested/file1"
    url = gcs.url(fn)
    assert "http" in url
    assert quote("nested/file1") in url
    with gcs.open(fn) as f:
        assert "http" in f.url()


def test_seek(gcs):
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


def test_bad_open(gcs):
    with pytest.raises((IOError, OSError)):
        gcs.open("")


def test_copy(gcs):
    fn = TEST_BUCKET + "/test/accounts.1.json"
    gcs.copy(fn, fn + "2")
    assert gcs.cat(fn) == gcs.cat(fn + "2")


def test_copy_recursive(gcs):
    src = TEST_BUCKET + "/nested"
    dest = TEST_BUCKET + "/dest"
    gcs.copy(src, dest, recursive=True)
    for fn in gcs.ls(dest):
        if not gcs.isdir(fn):
            assert gcs.cat(fn) == gcs.cat(fn.replace("dest", "nested"))


def test_copy_errors(gcs):
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


def test_move(gcs):
    fn = TEST_BUCKET + "/test/accounts.1.json"
    data = gcs.cat(fn)
    gcs.mv(fn, fn + "2")
    assert gcs.cat(fn + "2") == data
    assert not gcs.exists(fn)


@pytest.mark.parametrize("slash_from", ([False, True]))
def test_move_recursive(gcs, slash_from):
    # See issue #489
    dir_from = TEST_BUCKET + "/nested"
    if slash_from:
        dir_from += "/"
    dir_to = TEST_BUCKET + "/new_name"

    gcs.mv(dir_from, dir_to, recursive=True)
    assert not gcs.exists(dir_from)
    assert gcs.ls(dir_to) == [dir_to + "/file1", dir_to + "/file2", dir_to + "/nested2"]


def test_cat_file(gcs):
    fn = TEST_BUCKET + "/test/accounts.1.json"
    data = gcs.cat_file(fn)
    assert data[1:10] == gcs.cat_file(fn, start=1, end=10)
    assert data[1:] == gcs.cat_file(fn, start=1)
    assert data[:1] == gcs.cat_file(fn, end=1)


@pytest.mark.parametrize("consistency", [None, "size", "md5", "crc32c"])
def test_get_put(consistency, gcs):
    if consistency == "crc32c" and gcsfs.checkers.crcmod is None:
        pytest.skip("No CRC")
    if consistency == "size" and not gcs.on_google:
        pytest.skip("emulator does not return size")
    gcs.consistency = consistency
    with tmpfile() as fn:
        gcs.get(TEST_BUCKET + "/test/accounts.1.json", fn)
        data = files["test/accounts.1.json"]
        assert open(fn, "rb").read() == data
        gcs.put(fn, TEST_BUCKET + "/temp")
        assert gcs.du(TEST_BUCKET + "/temp") == len(data)
        assert gcs.cat(TEST_BUCKET + "/temp") == data


def test_get_put_list(gcs):
    with tmpfile() as fn:
        gcs.get([TEST_BUCKET + "/test/accounts.1.json"], [fn])
        data = files["test/accounts.1.json"]
        assert open(fn, "rb").read() == data
        gcs.put([fn], [TEST_BUCKET + "/temp"])
        assert gcs.du(TEST_BUCKET + "/temp") == len(data)
        assert gcs.cat(TEST_BUCKET + "/temp") == data


@pytest.mark.parametrize("protocol", ["", "gs://", "gcs://"])
def test_get_put_recursive(protocol, gcs):
    with tempdir() as dn:
        gcs.get(protocol + TEST_BUCKET + "/test/", dn + "/temp_dir", recursive=True)
        # there is now in local directory:
        # dn+'/temp_dir/accounts.1.json'
        # dn+'/temp_dir/accounts.2.json'
        data1 = files["test/accounts.1.json"]
        data2 = files["test/accounts.2.json"]
        assert open(dn + "/temp_dir/accounts.1.json", "rb").read() == data1
        assert open(dn + "/temp_dir/accounts.2.json", "rb").read() == data2
        gcs.put(dn + "/temp_dir", protocol + TEST_BUCKET + "/temp_dir", recursive=True)
        # there is now in remote directory:
        # protocol+TEST_BUCKET+'/temp_dir/accounts.1.json'
        # protocol+TEST_BUCKET+'/temp_dir/accounts.2.json'
        assert gcs.du(protocol + TEST_BUCKET + "/temp_dir/accounts.1.json") == len(
            data1
        )
        assert gcs.cat(protocol + TEST_BUCKET + "/temp_dir/accounts.1.json") == data1
        assert gcs.du(protocol + TEST_BUCKET + "/temp_dir/accounts.2.json") == len(
            data2
        )
        assert gcs.cat(protocol + TEST_BUCKET + "/temp_dir/accounts.2.json") == data2


@pytest.mark.parametrize("protocol", ["", "gs://", "gcs://"])
def test_get_put_file_in_dir(protocol, gcs):
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
        assert gcs.cat(protocol + TEST_BUCKET + "/temp_dir/accounts.1.json") == data1


@pytest.mark.parametrize("protocol", ["", "gs://", "gcs://"])
def test_get_file_to_current_working_directory(monkeypatch, protocol, gcs):
    fn = protocol + TEST_BUCKET + "/temp"
    gcs.pipe(fn, b"hello world")

    with tempdir() as dn:
        os.makedirs(dn)
        monkeypatch.chdir(dn)
        gcs.get_file(fn, "temp")
        with open("temp", mode="rb") as f:
            assert f.read() == b"hello world"


def test_special_characters_filename(gcs: GCSFileSystem):
    special_filename = """'!"`#$%&'()+,-.<=>?@[]^_{}~/'"""
    full_path = TEST_BUCKET + "/" + special_filename
    gcs.touch(full_path)
    info = gcs.info(full_path)
    assert info["name"] == full_path
    # Normal cat currently doesn't work with special characters,
    # because it invokes expand_path (and in turn glob) without escaping the characters.
    # This would need to be fixed in fsspec.
    assert gcs.cat_file(full_path) == b""


def test_slash_filename(gcs: GCSFileSystem):
    slash_filename = """abc/def"""
    full_path = TEST_BUCKET + "/" + slash_filename
    gcs.touch(full_path)
    info = gcs.info(full_path)
    assert info["name"] == full_path
    assert gcs.cat_file(full_path) == b""


def test_hash_filename(gcs: GCSFileSystem):
    slash_filename = """a#b#c"""
    full_path = TEST_BUCKET + "/" + slash_filename
    gcs.touch(full_path)
    info = gcs.info(full_path)
    assert info["name"] == full_path
    assert gcs.cat_file(full_path) == b""


def test_errors(gcs):
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


def test_read_small(gcs):
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


def test_seek_delimiter(gcs):
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


def test_read_block(gcs):
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


def test_flush(gcs):
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


def test_write_fails(gcs):
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


def text_mode(gcs):
    text = "Hello µ"
    with gcs.open(TEST_BUCKET + "/temp", "w") as f:
        f.write(text)
    with gcs.open(TEST_BUCKET + "/temp", "r") as f:
        assert f.read() == text


def test_write_blocks(gcs):
    with gcs.open(TEST_BUCKET + "/temp", "wb", block_size=2**18) as f:
        f.write(b"a" * 100000)
        assert f.buffer.tell() == 100000
        assert not (f.offset)
        f.write(b"a" * 100000)
        f.write(b"a" * 100000)
        assert f.offset
    assert gcs.info(TEST_BUCKET + "/temp")["size"] == 300000


def test_write_blocks2(gcs):
    if not gcs.on_google:
        pytest.skip("emulator always accepts whole request")
    with gcs.open(TEST_BUCKET + "/temp1", "wb", block_size=2**18) as f:
        f.write(b"a" * (2**18 + 1))
        # leftover bytes: GCS accepts blocks in multiples of 2**18 bytes
        assert f.buffer.tell() == 1
    assert gcs.info(TEST_BUCKET + "/temp1")["size"] == 2**18 + 1


def test_readline(gcs):
    all_items = chain.from_iterable(
        [files.items(), csv_files.items(), text_files.items()]
    )
    for k, data in all_items:
        with gcs.open("/".join([TEST_BUCKET, k]), "rb") as f:
            result = f.readline()
            expected = data.split(b"\n")[0] + (b"\n" if data.count(b"\n") else b"")
        assert result == expected


def test_readline_from_cache(gcs):
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


def test_readline_empty(gcs):
    data = b""
    with gcs.open(a, "wb") as f:
        f.write(data)
    with gcs.open(a, "rb") as f:
        result = f.readline()
        assert result == data


def test_readline_blocksize(gcs):
    data = b"ab\n" + b"a" * (2**18) + b"\nab"
    with gcs.open(a, "wb") as f:
        f.write(data)
    with gcs.open(a, "rb", block_size=2**18) as f:
        result = f.readline()
        expected = b"ab\n"
        assert result == expected

        result = f.readline()
        expected = b"a" * (2**18) + b"\n"
        assert result == expected

        result = f.readline()
        expected = b"ab"
        assert result == expected


def test_next(gcs):
    expected = csv_files["2014-01-01.csv"].split(b"\n")[0] + b"\n"
    with gcs.open(TEST_BUCKET + "/2014-01-01.csv") as f:
        result = next(f)
        assert result == expected


def test_iterable(gcs):
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


@pytest.mark.parametrize(
    "metadata_attribute, value, result_attribute",
    [
        ("custom_time", "2021-10-21T17:00:00Z", "customTime"),
        ("cache_control", "public, max-age=3600", "cacheControl"),
        ("content_encoding", "gzip", "contentEncoding"),
        ("content_language", "en", "contentLanguage"),
        (
            "content_disposition",
            "Attachment; filename=sample.empty",
            "contentDisposition",
        ),
    ],
)
def test_fixed_key_metadata(metadata_attribute, value, result_attribute, gcs):
    if not gcs.on_google:
        # might be added in the future
        # follow https://github.com/fsouza/fake-gcs-server/issues/477
        pytest.skip("no google metadata support on emulation")

    # open
    gcs.touch(a)
    assert metadata_attribute not in gcs.info(a)
    gcs.touch(a, fixed_key_metadata={metadata_attribute: value})
    file_info = gcs.info(a)
    assert result_attribute in file_info
    assert file_info[result_attribute] == value

    # setxattrs
    gcs.touch(b)
    assert metadata_attribute not in gcs.info(b)
    gcs.setxattrs(b, fixed_key_metadata={metadata_attribute: value})
    file_info = gcs.info(b)
    assert result_attribute in file_info
    assert file_info[result_attribute] == value


def test_readable(gcs):
    with gcs.open(a, "wb") as f:
        assert not f.readable()

    with gcs.open(a, "rb") as f:
        assert f.readable()


def test_seekable(gcs):
    with gcs.open(a, "wb") as f:
        assert not f.seekable()

    with gcs.open(a, "rb") as f:
        assert f.seekable()


def test_writable(gcs):
    with gcs.open(a, "wb") as f:
        assert f.writable()

    with gcs.open(a, "rb") as f:
        assert not f.writable()


def test_merge(gcs):
    with gcs.open(a, "wb") as f:
        f.write(b"a" * 10)

    with gcs.open(b, "wb") as f:
        f.write(b"a" * 10)
    gcs.merge(TEST_BUCKET + "/joined", [a, b])
    assert gcs.info(TEST_BUCKET + "/joined")["size"] == 20


def test_bigger_than_block_read(gcs):
    with gcs.open(TEST_BUCKET + "/2014-01-01.csv", "rb", block_size=3) as f:
        out = []
        while True:
            data = f.read(20)
            out.append(data)
            if len(data) == 0:
                break
    assert b"".join(out) == csv_files["2014-01-01.csv"]


def test_current(gcs):
    from gcsfs.tests import conftest

    assert GCSFileSystem.current() is gcs
    gcs2 = GCSFileSystem(**conftest.params)
    assert gcs2.session is gcs.session


def test_array(gcs):
    from array import array

    data = array("B", [65] * 1000)

    with gcs.open(a, "wb") as f:
        f.write(data)

    with gcs.open(a, "rb") as f:
        out = f.read()
        assert out == b"A" * 1000


def test_content_type_set(gcs):
    fn = TEST_BUCKET + "/content_type"
    with gcs.open(fn, "wb", content_type="text/html") as f:
        f.write(b"<html>")
    assert gcs.info(fn)["contentType"] == "text/html"


def test_content_type_guess(gcs):
    fn = TEST_BUCKET + "/content_type.txt"
    with gcs.open(fn, "wb") as f:
        f.write(b"zz")
    assert gcs.info(fn)["contentType"] == "text/plain"


def test_content_type_default(gcs):
    fn = TEST_BUCKET + "/content_type.abcdef"
    with gcs.open(fn, "wb") as f:
        f.write(b"zz")
    assert gcs.info(fn)["contentType"] == "application/octet-stream"


def test_content_type_put_guess(gcs):
    dst = TEST_BUCKET + "/content_type_put_guess"
    with tmpfile(extension="txt") as fn:
        with open(fn, "w") as f:
            f.write("zz")
        gcs.put(fn, f"gs://{dst}", b"")
    assert gcs.info(dst)["contentType"] == "text/plain"


def test_attrs(gcs):
    if not gcs.on_google:
        # https://github.com/fsspec/gcsfs/pull/479
        pytest.skip("fake-gcs-server:latest only supports PUT for metadata, not PATCH")
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


def test_request_user_project(gcs):
    gcs = GCSFileSystem(
        endpoint_url=gcs._endpoint, requester_pays=True, project=TEST_PROJECT
    )
    # test directly against `_call` to inspect the result
    r = gcs.call(
        "GET",
        "b/{}/o",
        TEST_BUCKET,
        delimiter="/",
        prefix="test",
        maxResults=100,
        info_out=True,
    )
    qs = urlparse(r.url.human_repr()).query
    result = parse_qs(qs)
    assert result["userProject"] == [TEST_PROJECT]


def test_request_user_project_string(gcs):
    gcs = GCSFileSystem(endpoint_url=gcs._endpoint, requester_pays=TEST_PROJECT)
    assert gcs.requester_pays == TEST_PROJECT
    # test directly against `_call` to inspect the result
    r = gcs.call(
        "GET",
        "b/{}/o",
        TEST_BUCKET,
        delimiter="/",
        prefix="",
        maxResults=100,
        info_out=True,
    )
    qs = urlparse(r.url.human_repr()).query
    result = parse_qs(qs)
    assert result["userProject"] == [TEST_PROJECT]


def test_request_header(gcs):
    gcs = GCSFileSystem(endpoint_url=gcs._endpoint, requester_pays=True)
    # test directly against `_call` to inspect the result
    r = gcs.call(
        "GET",
        "b/{}/o",
        TEST_BUCKET,
        delimiter="/",
        prefix="test",
        maxResults=100,
        info_out=True,
    )
    assert r.headers["User-Agent"] == "python-gcsfs/" + version


def test_user_project_fallback_google_default(monkeypatch):
    monkeypatch.setattr(gcsfs.core, "DEFAULT_PROJECT", "my_default_project")
    monkeypatch.setattr(fsspec.config, "conf", {})
    monkeypatch.setattr(
        gcsfs.credentials.gauth,
        "default",
        lambda *__, **_: (requests.Session(), "my_default_project"),
    )
    fs = GCSFileSystem(skip_instance_cache=True)
    assert fs.project == "my_default_project"


def test_user_project_cat(gcs):
    if not gcs.on_google:
        pytest.skip("no requester-pays on emulation")
    gcs.mkdir(TEST_REQUESTER_PAYS_BUCKET)
    try:
        gcs.pipe(TEST_REQUESTER_PAYS_BUCKET + "/foo.csv", b"data")
        gcs.make_bucket_requester_pays(TEST_REQUESTER_PAYS_BUCKET)
        gcs = GCSFileSystem(requester_pays=True)
        result = gcs.cat(TEST_REQUESTER_PAYS_BUCKET + "/foo.csv")
        assert len(result)
    finally:
        gcs.rm(TEST_REQUESTER_PAYS_BUCKET, recursive=True)


@mock.patch("gcsfs.credentials.gauth")
def test_raise_on_project_mismatch(mock_auth):
    mock_auth.default.return_value = (requests.Session(), "my_other_project")
    match = "'my_project' does not match the google default project 'my_other_project'"
    with pytest.raises(ValueError, match=match):
        GCSFileSystem(project="my_project", token="google_default")

    result = GCSFileSystem(project="my_other_project", token="google_default")
    assert result.project == "my_other_project"


def test_ls_prefix_cache(gcs):
    gcs.touch(f"gs://{TEST_BUCKET}/a/file1")
    gcs.touch(f"gs://{TEST_BUCKET}/a/file2")

    gcs.ls(f"gs://{TEST_BUCKET}/", prefix="a/file")
    gcs.info(f"gs://{TEST_BUCKET}/a/file1")


def test_placeholder_dir_cache_validity(gcs):
    gcs.touch(f"gs://{TEST_BUCKET}/a/")
    gcs.touch(f"gs://{TEST_BUCKET}/a/file")
    gcs.touch(f"gs://{TEST_BUCKET}/b")

    gcs.find(f"gs://{TEST_BUCKET}/a/")
    gcs.info(f"gs://{TEST_BUCKET}/b")


def test_pipe_small_cache_validity(gcs):
    folder = f"{TEST_BUCKET}/{str(uuid4())}"

    gcs.pipe(f"gs://{folder}/a/file.txt", b"")

    assert gcs.ls(f"gs://{folder}") == [f"{folder}/a"]

    gcs.pipe(f"gs://{folder}/b/file.txt", b"")

    ls_res = gcs.ls(f"gs://{folder}")
    assert len(ls_res) == 2
    assert f"{folder}/b" in ls_res


def test_put_small_cache_validity(gcs):
    folder = f"{TEST_BUCKET}/{str(uuid4())}"

    gcs.pipe(f"gs://{folder}/a/file.txt", b"")

    assert gcs.ls(f"gs://{folder}") == [f"{folder}/a"]

    with tmpfile() as fn:
        with open(fn, "w") as f:
            f.write("")

        gcs.put(fn, f"gs://{folder}/b/file.txt", b"")

    ls_res = gcs.ls(f"gs://{folder}")
    assert len(ls_res) == 2
    assert f"{folder}/b" in ls_res


def test_pseudo_dir_find(gcs):
    gcs.rm(f"{TEST_BUCKET}/*", recursive=True)
    gcs.touch(f"{TEST_BUCKET}/a/b/file")

    c = gcs.glob(f"{TEST_BUCKET}/a/b/*")
    assert c == [f"{TEST_BUCKET}/a/b/file"]

    b = set(gcs.glob(f"{TEST_BUCKET}/a/*"))
    assert b == {f"{TEST_BUCKET}/a/b"}

    a = set(gcs.glob(f"{TEST_BUCKET}/*"))
    assert a == {f"{TEST_BUCKET}/a"}

    assert gcs.find(TEST_BUCKET) == [f"{TEST_BUCKET}/a/b/file"]
    assert gcs.find(f"{TEST_BUCKET}/a", withdirs=True) == [
        f"{TEST_BUCKET}/a",
        f"{TEST_BUCKET}/a/b",
        f"{TEST_BUCKET}/a/b/file",
    ]


def test_zero_cache_timeout(gcs):
    gcs.touch(f"gs://{TEST_BUCKET}/a/file")
    gcs.find(f"gs://{TEST_BUCKET}/a/")
    gcs.info(f"gs://{TEST_BUCKET}/a/file")
    gcs.ls(f"gs://{TEST_BUCKET}/a/")

    # The _times entry and exception below should only be present after
    # https://github.com/fsspec/filesystem_spec/pull/513.
    if f"{TEST_BUCKET}/a" not in gcs.dircache._times:
        pytest.skip("fsspec version too early")

    with pytest.raises(KeyError):
        gcs.dircache[f"{TEST_BUCKET}/a"]


@pytest.mark.parametrize("with_cache", (False, True))
def test_find_with_prefix_partial_cache(gcs, with_cache):
    base_dir = f"{TEST_BUCKET}/test_find_with_prefix"
    gcs.touch(base_dir + "/test_1")
    gcs.touch(base_dir + "/test_2")

    gcs.invalidate_cache()
    if with_cache:
        gcs.ls(base_dir)
    precache = dict(gcs.dircache)
    assert gcs.find(base_dir, prefix="non_existent_") == []
    assert gcs.find(base_dir, prefix="test_") == [
        base_dir + "/test_1",
        base_dir + "/test_2",
    ]
    assert dict(gcs.dircache) == precache  # find qwith prefix shouldn't touch cache
    assert gcs.find(base_dir + "/test_1") == [base_dir + "/test_1"]
    assert gcs.find(base_dir + "/non_existent") == []
    assert gcs.find(base_dir + "/non_existent", prefix="more_non_existent") == []


def test_find_dircache(gcs):
    """Running `ls` after find should not corrupt the dir cache"""
    assert set(gcs.find(TEST_BUCKET)) == {f"{TEST_BUCKET}/{path}" for path in allfiles}
    assert set(gcs.ls(TEST_BUCKET)) == {
        f"{TEST_BUCKET}/test",
        f"{TEST_BUCKET}/nested",
        f"{TEST_BUCKET}/2014-01-01.csv",
        f"{TEST_BUCKET}/2014-01-02.csv",
        f"{TEST_BUCKET}/2014-01-03.csv",
    }
    assert set(gcs.ls(f"{TEST_BUCKET}/nested")) == {
        f"{TEST_BUCKET}/nested/file1",
        f"{TEST_BUCKET}/nested/file2",
        f"{TEST_BUCKET}/nested/nested2",
    }


def test_percent_file_name(gcs):
    parent = f"{TEST_BUCKET}/test/onefile"
    fn = f"{parent}/a%25.txt"
    data = b"zz"
    with gcs.open(fn, "wb", content_type="text/plain") as f:
        f.write(data)
    assert gcs.cat(fn) == data
    fn2 = unquote(fn)
    gcs.touch(fn2)
    assert gcs.cat(fn2) != data
    assert set(gcs.ls(parent)) == {fn, fn2}


@pytest.mark.parametrize(
    "location",
    [
        (None),
        ("US"),
        ("EUROPE-WEST3"),
        ("europe-west3"),
    ],
)
def test_bucket_location(gcs_factory, location):
    gcs = gcs_factory(default_location=location)
    if not gcs.on_google:
        pytest.skip("emulator can only create buckets in the 'US-CENTRAL1' location.")
    bucket_name = str(uuid4())
    try:
        gcs.mkdir(bucket_name)
        bucket = [
            b
            for b in sync(gcs.loop, gcs._list_buckets, timeout=gcs.timeout)
            if b["name"] == bucket_name + "/"
        ][0]
        assert bucket["location"] == (location or "US").upper()
    finally:
        gcs.rm(bucket_name, recursive=True)


def test_bucket_default_location_overwrite(gcs_factory):
    gcs = gcs_factory(default_location="US")
    if not gcs.on_google:
        pytest.skip("emulator can only create buckets in the 'US-CENTRAL1' location.")
    bucket_name = str(uuid4())
    try:
        gcs.mkdir(bucket_name, location="EUROPE-WEST3")
        bucket = [
            b
            for b in sync(gcs.loop, gcs._list_buckets, timeout=gcs.timeout)
            if b["name"] == bucket_name + "/"
        ][0]
        assert bucket["location"] == "EUROPE-WEST3"
    finally:
        gcs.rm(bucket_name, recursive=True)


def test_dir_marker(gcs):
    gcs.touch(f"{TEST_BUCKET}/placeholder/")
    gcs.touch(f"{TEST_BUCKET}/placeholder/inner")
    out = gcs.find(TEST_BUCKET)
    assert f"{TEST_BUCKET}/placeholder/" in out
    gcs.invalidate_cache()
    out2 = gcs.info(f"{TEST_BUCKET}/placeholder/")
    out3 = gcs.info(f"{TEST_BUCKET}/placeholder/")
    assert out2 == out3
    assert out2["type"] == "directory"


def test_mkdir_with_path(gcs):
    with pytest.raises(FileNotFoundError):
        gcs.mkdir(f"{TEST_BUCKET + 'new'}/path", create_parents=False)
    assert not gcs.exists(f"{TEST_BUCKET + 'new'}")
    gcs.mkdir(f"{TEST_BUCKET + 'new'}/path", create_parents=True)
    assert gcs.exists(f"{TEST_BUCKET + 'new'}")

    # these lines do nothing, but should not fail
    gcs.mkdir(f"{TEST_BUCKET + 'new'}/path", create_parents=False)
    gcs.mkdir(f"{TEST_BUCKET + 'new'}/path", create_parents=True)

    gcs.rm(f"{TEST_BUCKET + 'new'}", recursive=True)


def test_deep_find_wthdirs(gcs):
    gcs.touch(f"{TEST_BUCKET}/deep/nested/dir")
    assert gcs.find(f"{TEST_BUCKET}/deep/nested") == [f"{TEST_BUCKET}/deep/nested/dir"]
    assert gcs.find(f"{TEST_BUCKET}/deep/nested", withdirs=True) == [
        f"{TEST_BUCKET}/deep/nested",
        f"{TEST_BUCKET}/deep/nested/dir",
    ]


def test_info_versioned(gcs_versioned):
    with gcs_versioned.open(a, "wb") as wo:
        wo.write(b"v1")
    v1 = gcs_versioned.info(a)["generation"]
    assert v1 is not None
    with gcs_versioned.open(a, "wb") as wo:
        wo.write(b"v2")
    v2 = gcs_versioned.info(a)["generation"]
    assert v2 is not None and v1 != v2
    assert gcs_versioned.info(f"{a}#{v1}")["generation"] == v1
    assert gcs_versioned.info(f"{a}?generation={v2}")["generation"] == v2


def test_cat_versioned(gcs_versioned):
    with gcs_versioned.open(a, "wb") as wo:
        wo.write(b"v1")
    v1 = gcs_versioned.info(a)["generation"]
    assert v1 is not None
    with gcs_versioned.open(a, "wb") as wo:
        wo.write(b"v2")
    gcs_versioned.cat(f"{a}#{v1}") == b"v1"


def test_cp_versioned(gcs_versioned):
    with gcs_versioned.open(a, "wb") as wo:
        wo.write(b"v1")
    v1 = gcs_versioned.info(a)["generation"]
    assert v1 is not None
    with gcs_versioned.open(a, "wb") as wo:
        wo.write(b"v2")
    gcs_versioned.cp_file(f"{a}#{v1}", b)
    assert gcs_versioned.cat(b) == b"v1"


def test_ls_versioned(gcs_versioned):
    import posixpath

    with gcs_versioned.open(a, "wb") as wo:
        wo.write(b"v1")
    v1 = gcs_versioned.info(a)["generation"]
    with gcs_versioned.open(a, "wb") as wo:
        wo.write(b"v2")
    v2 = gcs_versioned.info(a)["generation"]
    dpath = posixpath.dirname(a)
    versions = {f"{a}#{v1}", f"{a}#{v2}"}
    assert versions == set(gcs_versioned.ls(dpath, versions=True))
    assert versions == {
        entry["name"] for entry in gcs_versioned.ls(dpath, detail=True, versions=True)
    }
    assert gcs_versioned.ls(TEST_BUCKET, versions=True) == ["gcsfs_test/tmp"]


def test_find_versioned(gcs_versioned):
    with gcs_versioned.open(a, "wb") as wo:
        wo.write(b"v1")
    v1 = gcs_versioned.info(a)["generation"]
    with gcs_versioned.open(a, "wb") as wo:
        wo.write(b"v2")
    v2 = gcs_versioned.info(a)["generation"]
    versions = {f"{a}#{v1}", f"{a}#{v2}"}
    assert versions == set(gcs_versioned.find(a, versions=True))
    assert versions == set(gcs_versioned.find(a, detail=True, versions=True))


def test_cp_directory_recursive(gcs):
    src = TEST_BUCKET + "/src"
    src_file = src + "/file"
    gcs.mkdir(src)
    gcs.touch(src_file)

    target = TEST_BUCKET + "/target"

    # cp without slash
    assert not gcs.exists(target)
    for loop in range(2):
        gcs.cp(src, target, recursive=True)
        assert gcs.isdir(target)

        if loop == 0:
            correct = [target + "/file"]
            assert gcs.find(target) == correct
        else:
            correct = [target + "/file", target + "/src/file"]
            assert sorted(gcs.find(target)) == correct

    gcs.rm(target, recursive=True)

    # cp with slash
    assert not gcs.exists(target)
    for loop in range(2):
        gcs.cp(src + "/", target, recursive=True)
        assert gcs.isdir(target)
        correct = [target + "/file"]
        assert gcs.find(target) == correct


def test_get_directory_recursive(gcs):
    src = TEST_BUCKET + "/src"
    src_file = src + "/file"
    gcs.mkdir(src)
    gcs.touch(src_file)

    with tempdir() as tmpdir:
        target = os.path.join(tmpdir, "target")
        target_fs = fsspec.filesystem("file")

        # get without slash
        assert not target_fs.exists(target)
        for loop in range(2):
            gcs.get(src, target, recursive=True)
            assert target_fs.isdir(target)

            if loop == 0:
                assert target_fs.find(target) == [os.path.join(target, "file")]
            else:
                assert sorted(target_fs.find(target)) == [
                    os.path.join(target, "file"),
                    os.path.join(target, "src", "file"),
                ]

        target_fs.rm(target, recursive=True)

        # get with slash
        assert not target_fs.exists(target)
        for loop in range(2):
            gcs.get(src + "/", target, recursive=True)
            assert target_fs.isdir(target)
            assert target_fs.find(target) == [os.path.join(target, "file")]


def test_put_directory_recursive(gcs):
    with tempdir() as tmpdir:
        src = os.path.join(tmpdir, "src")
        src_file = os.path.join(src, "file")

        source_fs = fsspec.filesystem("file")
        source_fs.mkdir(src)
        source_fs.touch(src_file)

        target = TEST_BUCKET + "/target"

        # put without slash
        assert not gcs.exists(target)
        for loop in range(2):
            gcs.put(src, target, recursive=True)
            assert gcs.isdir(target)

            if loop == 0:
                assert gcs.find(target) == [target + "/file"]
            else:
                assert sorted(gcs.find(target)) == [
                    target + "/file",
                    target + "/src/file",
                ]

        gcs.rm(target, recursive=True)

        # put with slash
        assert not gcs.exists(target)
        for loop in range(2):
            gcs.put(src + "/", target, recursive=True)
            assert gcs.isdir(target)
            assert gcs.find(target) == [target + "/file"]


def test_cp_two_files(gcs):
    src = TEST_BUCKET + "/src"
    file0 = src + "/file0"
    file1 = src + "/file1"
    gcs.mkdir(src)
    gcs.touch(file0)
    gcs.touch(file1)

    target = TEST_BUCKET + "/target"
    assert not gcs.exists(target)

    gcs.cp([file0, file1], target)

    assert gcs.isdir(target)
    assert sorted(gcs.find(target)) == [
        target + "/file0",
        target + "/file1",
    ]


def test_multiglob(gcs):
    # #530
    root = TEST_BUCKET

    ggparent = root + "/t1"
    gparent = ggparent + "/t2"
    parent = gparent + "/t3"
    leaf1 = parent + "/foo.txt"
    leaf2 = parent + "/bar.txt"
    leaf3 = parent + "/baz.txt"

    gcs.touch(leaf1)
    gcs.touch(leaf2)
    gcs.touch(leaf3)
    gcs.invalidate_cache()

    assert gcs.ls(gparent, detail=False) == [f"{root}/t1/t2/t3"]
    gcs.glob(ggparent + "/")
    assert gcs.ls(gparent, detail=False) == [f"{root}/t1/t2/t3"]


def test_expiry_keyword():
    gcs = GCSFileSystem(listings_expiry_time=1, token="anon")
    assert gcs.dircache.listings_expiry_time == 1
    gcs = GCSFileSystem(cache_timeout=1, token="anon")
    assert gcs.dircache.listings_expiry_time == 1
    gcs = GCSFileSystem(cache_timeout=0, token="anon")
    assert gcs.dircache.listings_expiry_time == 0


def test_copy_cache_invalidated(gcs):
    # Issue https://github.com/fsspec/gcsfs/issues/562
    source = TEST_BUCKET + "/source"
    gcs.mkdir(source)
    gcs.touch(source + "/file2")

    target = TEST_BUCKET + "/target"
    assert not gcs.exists(target)
    gcs.touch(target + "/dummy")
    assert gcs.isdir(target)

    target_file2 = target + "/file2"
    gcs.cp(source + "/file2", target)

    # Explicitly check that target has been removed from DirCache
    assert target not in gcs.dircache

    # Prior to fix the following failed as cache stale
    assert gcs.isfile(target_file2)


def test_transaction(gcs):
    # https://github.com/fsspec/gcsfs/issues/389
    if not gcs.on_google:
        pytest.skip()
    try:
        with gcs.transaction:
            with gcs.open(f"{TEST_BUCKET}/foo", "wb") as f:
                f.write(b"This is a test string")
            f.discard()
            assert not gcs.exists(f"{TEST_BUCKET}/foo")
            raise ZeroDivisionError
    except ZeroDivisionError:
        ...
    assert not gcs.exists(f"{TEST_BUCKET}/foo")

    with gcs.transaction:
        with gcs.open(f"{TEST_BUCKET}/foo", "wb") as f:
            f.write(b"This is a test string")
        assert not gcs.exists(f"{TEST_BUCKET}/foo")

    assert gcs.cat(f"{TEST_BUCKET}/foo") == b"This is a test string"


def test_find_maxdepth(gcs):
    assert gcs.find(f"{TEST_BUCKET}/nested", maxdepth=None) == [
        f"{TEST_BUCKET}/nested/file1",
        f"{TEST_BUCKET}/nested/file2",
        f"{TEST_BUCKET}/nested/nested2/file1",
        f"{TEST_BUCKET}/nested/nested2/file2",
    ]

    assert gcs.find(f"{TEST_BUCKET}/nested", maxdepth=None, withdirs=True) == [
        f"{TEST_BUCKET}/nested",
        f"{TEST_BUCKET}/nested/file1",
        f"{TEST_BUCKET}/nested/file2",
        f"{TEST_BUCKET}/nested/nested2",
        f"{TEST_BUCKET}/nested/nested2/file1",
        f"{TEST_BUCKET}/nested/nested2/file2",
    ]

    assert gcs.find(f"{TEST_BUCKET}/nested", maxdepth=1) == [
        f"{TEST_BUCKET}/nested/file1",
        f"{TEST_BUCKET}/nested/file2",
    ]

    assert gcs.find(f"{TEST_BUCKET}/nested", maxdepth=1, withdirs=True) == [
        f"{TEST_BUCKET}/nested",
        f"{TEST_BUCKET}/nested/file1",
        f"{TEST_BUCKET}/nested/file2",
        f"{TEST_BUCKET}/nested/nested2",
    ]

    with pytest.raises(ValueError, match="maxdepth must be at least 1"):
        gcs.find(f"{TEST_BUCKET}/nested", maxdepth=0)


def test_sign(gcs, monkeypatch):
    file = TEST_BUCKET + "/test.jpg"
    with gcs.open(file, "wb") as f:
        f.write(b"This is a test string")
    assert gcs.cat(file) == b"This is a test string"

    # `sign` is creating a google Client on its own, it needs a realistically
    # looking credentials file.
    if not gcs.on_google:
        monkeypatch.setenv(
            "GOOGLE_APPLICATION_CREDENTIALS",
            os.path.dirname(__file__) + "/fake-service-account-credentials.json",
        )

    current_ts_utc = int(datetime.now(tz=timezone.utc).timestamp())
    result = gcs.sign(file)

    # Check it here since emulator doesn't really validate those values
    params = parse_qs(urlparse(result).query)
    assert int(params["Expires"][0]) >= current_ts_utc + 100

    response = requests.get(result)
    assert response.text == "This is a test string"


@pytest.mark.xfail(reason="emulator does not support condition")
def test_write_x_mpu(gcs):
    fn = TEST_BUCKET + "/test.file"
    with gcs.open(fn, mode="xb", block_size=5 * 2**20) as f:
        assert f.mode == "xb"
        f.write(b"0" * 5 * 2**20)
        f.write(b"done")
    with pytest.raises(FileExistsError):
        with gcs.open(fn, mode="xb", block_size=5 * 2**20) as f:
            f.write(b"0" * 5 * 2**20)
            f.write(b"done")
