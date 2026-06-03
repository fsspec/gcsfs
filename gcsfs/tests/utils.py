import os
import shutil
import tempfile
from contextlib import contextmanager


@contextmanager
def ignoring(*exceptions):
    try:
        yield
    except exceptions:
        pass


@contextmanager
def tempdir(dir=None):
    dirname = tempfile.mkdtemp(dir=dir)
    shutil.rmtree(dirname, ignore_errors=True)

    try:
        yield dirname
    finally:
        if os.path.exists(dirname):
            shutil.rmtree(dirname, ignore_errors=True)


@contextmanager
def tmpfile(extension="", dir=None):
    extension = "." + extension.lstrip(".")
    handle, filename = tempfile.mkstemp(extension, dir=dir)
    os.close(handle)
    os.remove(filename)

    try:
        yield filename
    finally:
        if os.path.exists(filename):
            if os.path.isdir(filename):
                shutil.rmtree(filename)
            else:
                with ignoring(OSError):
                    os.remove(filename)


def is_real_gcs():
    """Checks if tests are explicitly running against real GCS."""
    if (
        "STORAGE_EMULATOR_HOST" not in os.environ
        and "GOOGLE_CLOUD_UNIVERSE_DOMAIN" not in os.environ
    ):
        return False

    from gcsfs.core import _location

    host = _location()
    return host.startswith("https://")
