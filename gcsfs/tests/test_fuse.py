import os

import pytest

import tempfile

fuse = pytest.importorskip("fuse")
from fsspec.fuse import run
from gcsfs.tests.settings import TEST_BUCKET
import threading
import time


def test_fuse(gcs):
    mountpath = tempfile.mkdtemp()
    th = threading.Thread(target=lambda: run(gcs, TEST_BUCKET + "/", mountpath))
    th.daemon = True
    th.start()

    time.sleep(5)
    timeout = 20
    while True:
        try:
            open(os.path.join(mountpath, "lock"), "w").close()
            os.remove(os.path.join(mountpath, "lock"))
            break
        except:  # noqa: E722
            time.sleep(0.5)
        timeout -= 0.5
        assert timeout > 0

    with open(os.path.join(mountpath, "hello"), "w") as f:
        # NB this is in TEXT mode
        f.write("hello")
    files = os.listdir(mountpath)
    assert "hello" in files
    with open(os.path.join(mountpath, "hello"), "r") as f:
        # NB this is in TEXT mode
        assert f.read() == "hello"
