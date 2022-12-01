import logging
import os
import tempfile
import threading
import time
from functools import partial

import pytest

try:
    fuse = pytest.importorskip("fuse")
except Exception as error:
    logging.debug("Error importing fuse: %s", error)
    pytest.skip("Error importing fuse.")

from fsspec.fuse import run

from gcsfs.tests.settings import TEST_BUCKET


@pytest.mark.xfail(reason="Failing test not previously tested.")
@pytest.mark.timeout(180)
def test_fuse(gcs):
    mountpath = tempfile.mkdtemp()
    _run = partial(run, gcs, TEST_BUCKET + "/", mountpath)
    th = threading.Thread(target=_run)
    th.daemon = True
    th.start()

    time.sleep(5)
    timeout = 20
    while True:
        try:
            logging.debug("Trying to create lock file.")
            open(os.path.join(mountpath, "lock"), "w").close()
            os.remove(os.path.join(mountpath, "lock"))
            break
        except Exception as error:  # noqa: E722
            logging.debug("Error: %s", error)
            time.sleep(0.5)
        timeout -= 0.5
        assert timeout > 0

    with open(os.path.join(mountpath, "hello"), "w") as f:
        # NB this is in TEXT mode
        f.write("hello")
    files = os.listdir(mountpath)
    assert "hello" in files
    with open(os.path.join(mountpath, "hello")) as f:
        # NB this is in TEXT mode
        assert f.read() == "hello"
