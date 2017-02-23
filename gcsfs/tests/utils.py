
import json
import pytest
import sys
import vcr

from gcsfs.core import GCSFileSystem
from gcsfs.tests.settings import (TEST_BUCKET, TEST_PROJECT, RECORD_MODE,
                                  GOOGLE_TOKEN)


def before_record_response(response):
    try:
        data = json.loads(response['body']['string'].decode())
        if 'access_token' in data:
            data['access_token'] = 'xxx'
        if 'id_token' in data:
            data['id_token'] = 'xxx'
        response['body']['string'] = json.dumps(data).encode()
    except:
        pass
    return response

my_vcr = vcr.VCR(
    record_mode=RECORD_MODE,
    path_transformer=vcr.VCR.ensure_suffix('.yaml'),
    filter_headers=['Authorization'],
    filter_query_parameters=['refresh_token', 'upload_id'],
    decode_compressed_response=True,
    before_record_response=before_record_response
    )
files = {'test/accounts.1.json':  (b'{"amount": 100, "name": "Alice"}\n'
                                   b'{"amount": 200, "name": "Bob"}\n'
                                   b'{"amount": 300, "name": "Charlie"}\n'
                                   b'{"amount": 400, "name": "Dennis"}\n'),
         'test/accounts.2.json':  (b'{"amount": 500, "name": "Alice"}\n'
                                   b'{"amount": 600, "name": "Bob"}\n'
                                   b'{"amount": 700, "name": "Charlie"}\n'
                                   b'{"amount": 800, "name": "Dennis"}\n')}

csv_files = {'2014-01-01.csv': (b'name,amount,id\n'
                                b'Alice,100,1\n'
                                b'Bob,200,2\n'
                                b'Charlie,300,3\n'),
             '2014-01-02.csv': (b'name,amount,id\n'),
             '2014-01-03.csv': (b'name,amount,id\n'
                                b'Dennis,400,4\n'
                                b'Edith,500,5\n'
                                b'Frank,600,6\n')}
text_files = {'nested/file1': b'hello\n',
              'nested/file2': b'world',
              'nested/nested2/file1': b'hello\n',
              'nested/nested2/file2': b'world'}
a = TEST_BUCKET+'/tmp/test/a'
b = TEST_BUCKET+'/tmp/test/b'
c = TEST_BUCKET+'/tmp/test/c'
d = TEST_BUCKET+'/tmp/test/d'


@pytest.yield_fixture()
def tempdir():
    d = tempfile.mkdtemp()
    yield d
    if os.path.exists(d):
        shutil.rmtree(d, ignore_errors=True)


@pytest.yield_fixture
def token_restore():
    try:
        cache = GCSFileSystem.tokens
        GCSFileSystem.tokens = {}
        yield
    finally:
        GCSFileSystem.tokens = cache
        GCSFileSystem._save_tokens(GCSFileSystem)


@pytest.yield_fixture
def gcs(token_restore):
    gcs = GCSFileSystem(TEST_PROJECT, token=GOOGLE_TOKEN)
    try:
        if not gcs.exists(TEST_BUCKET):
            gcs.mkdir(TEST_BUCKET)
        for k in [a, b, c, d]:
            try:
                gcs.rm(k)
            except:
                pass
        for flist in [files, csv_files, text_files]:
            for fname, data in flist.items():
                with gcs.open(TEST_BUCKET+'/'+fname, 'wb') as f:
                    f.write(data)
        yield gcs
    finally:
        gcs.ls(TEST_BUCKET)
        [gcs.rm(f) for f in gcs.ls(TEST_BUCKET)]
        # gcs.rmdir(settings.TEST_BUCKET)
