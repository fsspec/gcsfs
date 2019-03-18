# -*- coding: utf-8 -*-

import io
from itertools import chain
import os
import pytest

from gcsfs.tests.settings import TEST_PROJECT, GOOGLE_TOKEN, TEST_BUCKET
from gcsfs.tests.utils import (tempdir, token_restore, my_vcr, gcs_maker,
                               files, csv_files, text_files, a, b, c, d,
                               tmpfile)
from gcsfs.core import GCSFileSystem, quote_plus, GCS_MIN_BLOCK_SIZE
from gcsfs.utils import seek_delimiter


@my_vcr.use_cassette(match=['all'])
def test_simple(token_restore):
    assert not GCSFileSystem.tokens
    gcs = GCSFileSystem(TEST_PROJECT, token=GOOGLE_TOKEN)
    gcs.ls(TEST_BUCKET)  # no error
    gcs.ls('/' + TEST_BUCKET)  # OK to lead with '/'


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
def test_ls2(token_restore):
    with gcs_maker() as gcs:
        assert TEST_BUCKET in gcs.ls('')
        with pytest.raises((OSError, IOError)):
            gcs.ls('nonexistent')
        fn = TEST_BUCKET+'/test/accounts.1.json'
        gcs.touch(fn)
        assert fn in gcs.ls(TEST_BUCKET+'/test')

@my_vcr.use_cassette(match=['all'])
def test_pickle(token_restore):
    import pickle
    with gcs_maker() as gcs:

        # Write data to distinct filename
        fn = TEST_BUCKET+'/nested/abcdefg'
        data = b'hello\n'
        with gcs.open(fn, 'wb') as f:
            f.write(b'1234567')

        # verify that that filename is not in the serialized form
        b = pickle.dumps(gcs)
        assert b'abcdefg' not in b
        assert b'1234567' not in b

        gcs2 = pickle.loads(b)

        assert gcs.session is not gcs2.session
        gcs.touch(a)
        assert gcs.ls(TEST_BUCKET) == gcs2.ls(TEST_BUCKET)


@my_vcr.use_cassette(match=['all'])
def test_ls_touch(token_restore):
    with gcs_maker() as gcs:
        assert not gcs.exists(TEST_BUCKET+'/tmp/test')

        gcs.touch(a)
        gcs.touch(b)

        L = gcs.ls(TEST_BUCKET+'/tmp/test', False)
        assert set(L) == set([a, b])

        L_d = gcs.ls(TEST_BUCKET+'/tmp/test', True)
        assert set(d['path'] for d in L_d) == set([a, b])


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
def test_rm_batch(token_restore):
    with gcs_maker() as gcs:
        gcs.touch(a)
        gcs.touch(b)
        assert a in gcs.walk(TEST_BUCKET)
        assert b in gcs.walk(TEST_BUCKET)
        gcs.rm([a, b])
        assert a not in gcs.walk(TEST_BUCKET)
        assert b not in gcs.walk(TEST_BUCKET)


@my_vcr.use_cassette(match=['all'])
def test_rm_recursive(token_restore):
    files = ['/a', '/a/b', '/a/c']
    with gcs_maker() as gcs:
        for fn in files:
            gcs.touch(TEST_BUCKET + fn)
        gcs.rm(TEST_BUCKET + files[0], True)
        assert gcs.ls(TEST_BUCKET) == []


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
        gcs.touch(fn)

        assert fn not in gcs.ls(TEST_BUCKET+'/')
        assert fn in gcs.ls(TEST_BUCKET+'/nested/')
        assert fn in gcs.ls(TEST_BUCKET+'/nested')
        assert list(sorted(gcs.ls('gcs://'+TEST_BUCKET+'/nested/'))) == \
               list(sorted(gcs.ls(TEST_BUCKET+'/nested')))


@my_vcr.use_cassette(match=['all'])
def test_ls_detail(token_restore):
    with gcs_maker(True) as gcs:
        L = gcs.ls(TEST_BUCKET+'/nested', detail=True)
        assert all(isinstance(item, dict) for item in L)


@my_vcr.use_cassette(match=['all'])
def test_gcs_glob(token_restore):
    with gcs_maker(True) as gcs:
        fn = TEST_BUCKET+'/nested/file1'
        assert fn not in gcs.glob(TEST_BUCKET+'/')
        assert fn not in gcs.glob(TEST_BUCKET+'/*')
        assert fn in gcs.glob(TEST_BUCKET+'/nested')
        assert fn in gcs.glob(TEST_BUCKET+'/nested/*')
        assert fn in gcs.glob(TEST_BUCKET+'/nested/file*')
        assert fn in gcs.glob(TEST_BUCKET+'/*/*')
        assert fn in gcs.glob(TEST_BUCKET+'/**')
        assert all(f in gcs.walk(TEST_BUCKET) for f in
                   gcs.glob(TEST_BUCKET+'/nested/*'))
        with pytest.raises(ValueError):
            gcs.glob('*')


@my_vcr.use_cassette(match=['all'])
def test_read_keys_from_bucket(token_restore):
    with gcs_maker(True) as gcs:
        for k, data in files.items():
            file_contents = gcs.cat('/'.join([TEST_BUCKET, k]))
            assert file_contents == data

        assert all(gcs.cat('/'.join([TEST_BUCKET, k])) ==
                   gcs.cat('gcs://' + '/'.join([TEST_BUCKET, k]))
                   for k in files)


@my_vcr.use_cassette(match=['all'])
def test_url(token_restore):
    with gcs_maker(True) as gcs:
        fn = TEST_BUCKET+'/nested/file1'
        url = gcs.url(fn)
        assert 'http' in url
        assert quote_plus('nested/file1') in url
        with gcs.open(fn) as f:
            assert 'http' in f.url()


@my_vcr.use_cassette(match=['all'])
def test_seek(token_restore):
    with gcs_maker(True) as gcs:
        with gcs.open(a, 'wb') as f:
            f.write(b'123')

        with gcs.open(a) as f:
            f.seek(1000)
            with pytest.raises(ValueError):
                f.seek(-1)
            with pytest.raises(ValueError):
                f.seek(-5, 2)
            with pytest.raises(ValueError):
                f.seek(0, 10)
            f.seek(0)
            assert f.read(1) == b'1'
            f.seek(0)
            assert f.read(1) == b'1'
            f.seek(3)
            assert f.read(1) == b''
            f.seek(-1, 2)
            assert f.read(1) == b'3'
            f.seek(-1, 1)
            f.seek(-1, 1)
            assert f.read(1) == b'2'
            for i in range(4):
                assert f.seek(i) == i


@my_vcr.use_cassette(match=['all'])
def test_bad_open(token_restore):
    with gcs_maker() as gcs:
        with pytest.raises((IOError, OSError)):
            gcs.open('')


@my_vcr.use_cassette(match=['all'])
def test_copy(token_restore):
    with gcs_maker(True) as gcs:
        fn = TEST_BUCKET+'/test/accounts.1.json'
        gcs.copy(fn, fn+'2')
        assert gcs.cat(fn) == gcs.cat(fn+'2')


@my_vcr.use_cassette(match=['all'])
def test_move(token_restore):
    with gcs_maker(True) as gcs:
        fn = TEST_BUCKET+'/test/accounts.1.json'
        data = gcs.cat(fn)
        gcs.mv(fn, fn+'2')
        assert gcs.cat(fn+'2') == data
        assert not gcs.exists(fn)


@my_vcr.use_cassette(match=['all'])
def test_get_put(token_restore):
    with gcs_maker(True) as gcs:
        with tmpfile() as fn:
            gcs.get(TEST_BUCKET+'/test/accounts.1.json', fn)
            data = files['test/accounts.1.json']
            assert open(fn, 'rb').read() == data
            gcs.put(fn, TEST_BUCKET+'/temp')
            assert gcs.du(TEST_BUCKET+'/temp')[
                       TEST_BUCKET+'/temp'] == len(data)
            assert gcs.cat(TEST_BUCKET+'/temp') == data


@my_vcr.use_cassette(match=['all'])
def test_get_put_recursive(token_restore):
    with gcs_maker(True) as gcs:
        with tempdir() as dn:
            gcs.get(TEST_BUCKET+'/test/', dn+'/temp_dir', recursive=True)
            # there is now in local directory:
            # dn+'/temp_dir/accounts.1.json'
            # dn+'/temp_dir/accounts.2.json'
            data1 = files['test/accounts.1.json']
            data2 = files['test/accounts.2.json']
            assert open(dn+'/temp_dir/accounts.1.json', 'rb').read() == data1
            assert open(dn+'/temp_dir/accounts.2.json', 'rb').read() == data2
            gcs.put(dn+'/temp_dir', TEST_BUCKET+'/temp_dir', recursive=True)
            # there is now in remote directory:
            # TEST_BUCKET+'/temp_dir/accounts.1.json'
            # TEST_BUCKET+'/temp_dir/accounts.2.json'
            assert gcs.du(TEST_BUCKET+'/temp_dir/accounts.1.json')[
                       TEST_BUCKET+'/temp_dir/accounts.1.json'] == len(data1)
            assert gcs.cat(TEST_BUCKET+'/temp_dir/accounts.1.json') == data1
            assert gcs.du(TEST_BUCKET+'/temp_dir/accounts.2.json')[
                       TEST_BUCKET+'/temp_dir/accounts.2.json'] == len(data2)
            assert gcs.cat(TEST_BUCKET+'/temp_dir/accounts.2.json') == data2


@my_vcr.use_cassette(match=['all'])
def test_errors(token_restore):
    with gcs_maker() as gcs:
        with pytest.raises((IOError, OSError)):
            gcs.open(TEST_BUCKET+'/tmp/test/shfoshf', 'rb')

        ## This is fine, no need for interleving directories on gcs
        #with pytest.raises((IOError, OSError)):
        #    gcs.touch('tmp/test/shfoshf/x')

        with pytest.raises((IOError, OSError)):
            gcs.rm(TEST_BUCKET+'/tmp/test/shfoshf/x')

        with pytest.raises((IOError, OSError)):
            gcs.mv(TEST_BUCKET+'/tmp/test/shfoshf/x', 'tmp/test/shfoshf/y')

        with pytest.raises((IOError, OSError)):
            gcs.open('x', 'rb')

        with pytest.raises((IOError, OSError)):
            gcs.rm('unknown')

        with pytest.raises(ValueError):
            with gcs.open(TEST_BUCKET+'/temp', 'wb') as f:
                f.read()

        with pytest.raises(ValueError):
            f = gcs.open(TEST_BUCKET+'/temp', 'rb')
            f.close()
            f.read()

        with pytest.raises(ValueError) as e:
            gcs.mkdir('/')
            assert 'bucket' in str(e)

        with pytest.raises(ValueError):
            gcs.walk('')

        with pytest.raises(ValueError):
            gcs.walk('gcs://')


@my_vcr.use_cassette(match=['all'])
def test_read_small(token_restore):
    with gcs_maker(True) as gcs:
        fn = TEST_BUCKET+'/2014-01-01.csv'
        with gcs.open(fn, 'rb', block_size=10) as f:
            out = []
            while True:
                data = f.read(3)
                if data == b'':
                    break
                out.append(data)
            assert gcs.cat(fn) == b''.join(out)
            # cache drop
            assert len(f.cache) < len(out)


@my_vcr.use_cassette(match=['all'])
def test_seek_delimiter(token_restore):
    with gcs_maker(True) as gcs:
        fn = 'test/accounts.1.json'
        data = files[fn]
        with gcs.open('/'.join([TEST_BUCKET, fn])) as f:
            seek_delimiter(f, b'}', 0)
            assert f.tell() == 0
            f.seek(1)
            seek_delimiter(f, b'}', 5)
            assert f.tell() == data.index(b'}') + 1
            seek_delimiter(f, b'\n', 5)
            assert f.tell() == data.index(b'\n') + 1
            f.seek(1, 1)
            ind = data.index(b'\n') + data[data.index(b'\n')+1:].index(b'\n') + 1
            seek_delimiter(f, b'\n', 5)
            assert f.tell() == ind + 1


@my_vcr.use_cassette(match=['all'])
def test_read_block(token_restore):
    with gcs_maker(True) as gcs:
        data = files['test/accounts.1.json']
        lines = io.BytesIO(data).readlines()
        path = TEST_BUCKET+'/test/accounts.1.json'
        assert gcs.read_block(path, 1, 35, b'\n') == lines[1]
        assert gcs.read_block(path, 0, 30, b'\n') == lines[0]
        assert gcs.read_block(path, 0, 35, b'\n') == lines[0] + lines[1]
        out = gcs.read_block(path, 0, 5000, b'\n')
        assert gcs.read_block(path, 0, 5000, b'\n') == data
        assert len(gcs.read_block(path, 0, 5)) == 5
        assert len(gcs.read_block(path, 4, 5000)) == len(data) - 4
        assert gcs.read_block(path, 5000, 5010) == b''

        assert gcs.read_block(path, 5, None) == gcs.read_block(path, 5, 1000)


@my_vcr.use_cassette(match=['all'])
def test_flush(token_restore):
    with gcs_maker() as gcs:
        gcs.touch(a)
        with gcs.open(a, 'rb') as ro:
            with pytest.raises(ValueError):
                ro.write(b"abc")

            ro.flush()


        with gcs.open(b, 'wb') as wo:
            wo.write(b"abc")
            wo.flush()
            assert not gcs.exists(b)

        assert gcs.exists(b)
        with pytest.raises(ValueError):
            wo.write(b"abc")



@my_vcr.use_cassette(match=['all'])
def test_write_fails(token_restore):
    with gcs_maker() as gcs:
        with pytest.raises(ValueError):
            gcs.touch(TEST_BUCKET+'/temp')
            gcs.open(TEST_BUCKET+'/temp', 'rb').write(b'hello')

            with gcs.open(TEST_BUCKET+'/temp', 'wb') as f:
                f.write(b'hello')
                f.flush(force=True)
            with pytest.raises(ValueError):
                f.write(b'world')

        f = gcs.open(TEST_BUCKET+'/temp', 'wb')
        f.close()
        with pytest.raises(ValueError):
            f.write(b'hello')
        with pytest.raises((OSError, IOError)):
            gcs.open('nonexistentbucket/temp', 'wb').close()


@my_vcr.use_cassette(match=['all'])
def text_mode(token_restore):
    text = 'Hello µ'
    with gcs_maker() as gcs:
        with gcs.open(TEST_BUCKET+'/temp', 'w') as f:
            f.write(text)
        with gcs.open(TEST_BUCKET+'/temp', 'r') as f:
            assert f.read() == text


@my_vcr.use_cassette(match=['all'])
def test_write_blocks(token_restore):
    with gcs_maker() as gcs:
        with gcs.open(TEST_BUCKET+'/temp', 'wb', block_size=2**18) as f:
            f.write(b'a' * 100000)
            assert f.buffer.tell() == 100000
            assert not(f.offset)
            f.write(b'a' * 100000)
            f.write(b'a' * 100000)
            assert f.offset
        assert gcs.info(TEST_BUCKET+'/temp')['size'] == 300000


@my_vcr.use_cassette(match=['all'])
def test_write_blocks2(token_restore):
    with gcs_maker() as gcs:
        with gcs.open(TEST_BUCKET+'/temp1', 'wb', block_size=2**18) as f:
            f.write(b'a' * (2**18+1))
            # leftover bytes: GCS accepts blocks in multiples of 2**18 bytes
            assert f.buffer.tell() == 1
        assert gcs.info(TEST_BUCKET+'/temp1')['size'] == 2**18+1


@my_vcr.use_cassette(match=['all'])
def test_readline(token_restore):
    with gcs_maker(True) as gcs:
        all_items = chain.from_iterable([
            files.items(), csv_files.items(), text_files.items()
        ])
        for k, data in all_items:
            with gcs.open('/'.join([TEST_BUCKET, k]), 'rb') as f:
                result = f.readline()
                expected = data.split(b'\n')[0] + (b'\n' if data.count(b'\n')
                                                   else b'')
            assert result == expected


@my_vcr.use_cassette(match=['all'])
def test_readline_from_cache(token_restore):
    with gcs_maker() as gcs:
        data = b'a,b\n11,22\n3,4'
        with gcs.open(a, 'wb') as f:
            f.write(data)

        with gcs.open(a, 'rb') as f:
            result = f.readline()
            assert result == b'a,b\n'
            assert f.loc == 4
            assert f.cache == data

            result = f.readline()
            assert result == b'11,22\n'
            assert f.loc == 10
            assert f.cache == data

            result = f.readline()
            assert result == b'3,4'
            assert f.loc == 13
            assert f.cache == data


@my_vcr.use_cassette(match=['all'])
def test_readline_partial(token_restore):
    with gcs_maker() as gcs:
        data = b'aaaaa,bbbbb\n12345,6789\n'
        with gcs.open(a, 'wb') as f:
            f.write(data)
        with gcs.open(a, 'rb') as f:
            result = f.readline(5)
            assert result == b'aaaaa'
            result = f.readline(5)
            assert result == b',bbbb'
            result = f.readline(5)
            assert result == b'b\n'
            result = f.readline()
            assert result == b'12345,6789\n'


@my_vcr.use_cassette(match=['all'])
def test_readline_empty(token_restore):
    with gcs_maker() as gcs:
        data = b''
        with gcs.open(a, 'wb') as f:
            f.write(data)
        with gcs.open(a, 'rb') as f:
            result = f.readline()
            assert result == data


@my_vcr.use_cassette(match=['all'])
def test_readline_blocksize(token_restore):
    with gcs_maker() as gcs:
        data = b'ab\n' + b'a' * (2**18) + b'\nab'
        with gcs.open(a, 'wb') as f:
            f.write(data)
        with gcs.open(a, 'rb', block_size=2**18) as f:
            result = f.readline()
            expected = b'ab\n'
            assert result == expected

            result = f.readline()
            expected = b'a' * (2**18) + b'\n'
            assert result == expected

            result = f.readline()
            expected = b'ab'
            assert result == expected


@my_vcr.use_cassette(match=['all'])
def test_next(token_restore):
    with gcs_maker(True) as gcs:
        expected = csv_files['2014-01-01.csv'].split(b'\n')[0] + b'\n'
        with gcs.open(TEST_BUCKET + '/2014-01-01.csv') as f:
            result = next(f)
            assert result == expected


@my_vcr.use_cassette(match=['all'])
def test_iterable(token_restore):
    with gcs_maker() as gcs:
        data = b'abc\n123'
        with gcs.open(a, 'wb') as f:
            f.write(data)
        with gcs.open(a) as f, io.BytesIO(data) as g:
            for fromgcs, fromio in zip(f, g):
                assert fromgcs == fromio
            f.seek(0)
            assert f.readline() == b'abc\n'
            assert f.readline() == b'123'
            f.seek(1)
            assert f.readline() == b'bc\n'
            assert f.readline(1) == b'1'
            assert f.readline() == b'23'

        with gcs.open(a) as f:
            out = list(f)
        with gcs.open(a) as f:
            out2 = f.readlines()
        assert out == out2
        assert b"".join(out) == data


@my_vcr.use_cassette(match=['all'])
def test_readable(token_restore):
    with gcs_maker() as gcs:
        with gcs.open(a, 'wb') as f:
            assert not f.readable()

        with gcs.open(a, 'rb') as f:
            assert f.readable()


@my_vcr.use_cassette(match=['all'])
def test_seekable(token_restore):
    with gcs_maker() as gcs:
        with gcs.open(a, 'wb') as f:
            assert not f.seekable()

        with gcs.open(a, 'rb') as f:
            assert f.seekable()


@my_vcr.use_cassette(match=['all'])
def test_writable(token_restore):
    with gcs_maker() as gcs:
        with gcs.open(a, 'wb') as f:
            assert f.writable()

        with gcs.open(a, 'rb') as f:
            assert not f.writable()


@my_vcr.use_cassette(match=['all'])
def test_merge(token_restore):
    with gcs_maker() as gcs:
        with gcs.open(a, 'wb') as f:
            f.write(b'a' * 100)

        with gcs.open(b, 'wb') as f:
            f.write(b'a' * 100)
        gcs.merge(TEST_BUCKET+'/joined', [a, b])
        assert gcs.info(TEST_BUCKET+'/joined')['size'] == 200


@my_vcr.use_cassette(match=['all'])
def test_bigger_than_block_read(token_restore):
    with gcs_maker(True) as gcs:
        with gcs.open(TEST_BUCKET+'/2014-01-01.csv', 'rb', block_size=3) as f:
            out = []
            while True:
                data = f.read(20)
                out.append(data)
                if len(data) == 0:
                    break
        assert b''.join(out) == csv_files['2014-01-01.csv']


@my_vcr.use_cassette(match=['all'])
def test_current(token_restore):
    from google.oauth2.credentials import Credentials

    with gcs_maker() as gcs:
        assert GCSFileSystem.current() is gcs
        gcs2 = GCSFileSystem(TEST_PROJECT, token=GOOGLE_TOKEN)
        assert gcs2.session is gcs.session
        gcs2 = GCSFileSystem(TEST_PROJECT, token=GOOGLE_TOKEN,
                             secure_serialize=False)
        assert isinstance(gcs2.token, Credentials)


@my_vcr.use_cassette(match=['all'])
def test_array(token_restore):
    with gcs_maker() as gcs:
        from array import array
        data = array('B', [65] * 1000)

        with gcs.open(a, 'wb') as f:
            f.write(data)

        with gcs.open(a, 'rb') as f:
            out = f.read()
            assert out == b'A' * 1000


@my_vcr.use_cassette(match=['all'])
def test_attrs(token_restore):
    with gcs_maker() as gcs:
        gcs.touch(a)
        assert 'metadata' not in gcs.info(a)
        with pytest.raises(KeyError):
            gcs.getxattr(a, 'foo')

        gcs.touch(a, metadata={'foo': 'blob'})
        assert gcs.getxattr(a, 'foo') == 'blob'

        gcs.setxattrs(a, foo='blah')
        assert gcs.getxattr(a, 'foo') == 'blah'

        with gcs.open(a, 'wb') as f:
            f.metadata = {'something': 'not'}

        with pytest.raises(KeyError):
            gcs.getxattr(a, 'foo')
        assert gcs.getxattr(a, 'something') == 'not'
