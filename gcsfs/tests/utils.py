
from gcsfs.core import GCSFileSystem
from gcsfs.tests import settings
import pytest
import sys
import vcr

my_vcr = vcr.VCR(
    record_mode=settings.RECORD_MODE,
    path_transformer=vcr.VCR.ensure_suffix('.yaml'),
    filter_headers=['Authorization'],
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
a = test_bucket_name+'/tmp/test/a'
b = test_bucket_name+'/tmp/test/b'
c = test_bucket_name+'/tmp/test/c'
d = test_bucket_name+'/tmp/test/d'


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
    gcs = GCSFileSystem(settings.TEST_PROJECT, token=settings.GOOGLE_TOKEN)
    try:
        if not gcs.exists(settings.TEST_BUCKET):
            gcs.mkdir(settings.TEST_BUCKET)
        for k in [a, b, c, d]:
            try:
                client.delete_object(Bucket=test_bucket_name, Key=k)
            except:
                pass
        for flist in [files, csv_files, text_files]:
            for f, data in flist.items():
                client.put_object(Bucket=test_bucket_name, Key=f, Body=data)
        yield gcs
    finally:
        gcs.ls(settings.TEST_BUCKET)
        [gcs.rm(f) for f in gcs.ls(settings.TEST_BUCKET)]
        # gcs.rmdir(settings.TEST_BUCKET)
