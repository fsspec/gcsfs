
from gcsfs.core import GCSFileSystem
from gcsfs.tests import settings
import pytest
import vcr

my_vcr = vcr.VCR(
    record_mode=settings.RECORD_MODE,
    path_transformer=vcr.VCR.ensure_suffix('.yaml'),
    filter_headers=['Authorization'],
    )


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
        GCSFileSystem._save_tokens()
