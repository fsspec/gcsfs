import logging
import os
import sys
import tempfile
import threading
import time
from functools import partial

import pytest

from gcsfs.tests.settings import TEST_BUCKET


@pytest.mark.timeout(180)
@pytest.fixture
def fsspec_fuse_run():
    """Fixture catches other errors on fuse import."""
    try:
        _fuse = pytest.importorskip("fuse")  # noqa

        from fsspec.fuse import run as _fsspec_fuse_run

        return _fsspec_fuse_run
    except Exception as error:
        logging.debug("Error importing fuse: %s", error)
        pytest.skip("Error importing fuse.")


@pytest.mark.skipif(sys.version_info < (3, 9), reason="Test fuse causes hang.")
@pytest.mark.xfail(reason="Failing test not previously tested.")
@pytest.mark.timeout(180)
def test_fuse(gcs, fsspec_fuse_run):
    mountpath = tempfile.mkdtemp()
    _run = partial(fsspec_fuse_run, gcs, TEST_BUCKET + "/", mountpath)
    th = threading.Thread(target=_run)
    th.daemon = True
    th.start()

    time.sleep(5)
    timeout = 20
    n = 40
    for i in range(n):
        logging.debug(f"Attempt # {i+1}/{n} to create lock file.")
        try:
            open(os.path.join(mountpath, "lock"), "w").close()
            os.remove(os.path.join(mountpath, "lock"))
            break
        except Exception as error:  # noqa: E722
            logging.debug("Error: %s", error)
            time.sleep(0.5)
        timeout -= 0.5
        assert timeout > 0
    else:
        raise AssertionError(f"Attempted lock file failed after {n} attempts.")

    with open(os.path.join(mountpath, "hello"), "w") as f:
        # NB this is in TEXT mode
        f.write("hello")
    files = os.listdir(mountpath)
    assert "hello" in files
    with open(os.path.join(mountpath, "hello")) as f:
        # NB this is in TEXT mode
        assert f.read() == "hello"
