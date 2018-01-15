import os
import pytest
fuse = pytest.importorskip('fuse')
from gcsfs.gcsfuse import GCSFS
import tempfile
import sys
from gcsfs.tests.settings import (TEST_BUCKET, TEST_PROJECT, RECORD_MODE,
                                  GOOGLE_TOKEN, FAKE_GOOGLE_TOKEN, DEBUG)
from gcsfs.tests.utils import gcs_maker, token_restore, my_vcr
import gcsfs
import threading
import time


PY2 = sys.version_info.major < 3


@pytest.mark.skipif("TRAVIS" in os.environ and os.environ["TRAVIS"] == "true",
                    reason="Skipping this test on Travis CI.")
@my_vcr.use_cassette(match=['all'])
def test_fuse(token_restore):
    mountpath = tempfile.mkdtemp()
    with gcs_maker() as gcs:
        th = threading.Thread(
            target=lambda: fuse.FUSE(
                GCSFS(TEST_BUCKET, gcs=gcs), mountpath, nothreads=False,
                foreground=True))
        th.daemon = True
        th.start()
        time.sleep(2)
        with open(os.path.join(mountpath, 'hello'), 'w') as f:
            # NB this is in TEXT mode
            f.write('hello')
        files = os.listdir(mountpath)
        assert 'hello' in files
        with open(os.path.join(mountpath, 'hello'), 'r') as f:
            # NB this is in TEXT mode
            assert f.read() == 'hello'
