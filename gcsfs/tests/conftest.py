import pytest

from gcsfs.core import GCSFileSystem


@pytest.yield_fixture
def token_restore():
    cache = GCSFileSystem.tokens
    try:
        GCSFileSystem.tokens = {}
        yield
    finally:
        GCSFileSystem.tokens = cache
        GCSFileSystem._save_tokens()
        GCSFileSystem.clear_instance_cache()
