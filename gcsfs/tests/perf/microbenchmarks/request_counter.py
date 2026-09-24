"""Counting of the HTTP calls gcsfs issues while a benchmark runs.

Wall-clock latency on its own does not catch request amplification. An extra
metadata round-trip is invisible in throughput on the multi-GB objects the
`read` group uses, and on small objects it hides inside run-to-run network
noise. Counting the calls turns that into a deterministic number.

This exists because of fsspec/gcsfs#1048: raising the default concurrency to 4
routed ``cat_file()`` through ``_cat_file_concurrent()``, which looks the object
size up with ``_info()`` before downloading. For an object smaller than
``MIN_CHUNK_SIZE_FOR_CONCURRENCY`` the range is never split, so the lookup buys
nothing and a one-round-trip read silently became three (object GET +
objects.list + download).

Counts are taken at ``GCSFileSystem._call``, i.e. requests that were issued.
``_info()`` races the object GET against the listing and cancels the loser, so
a cancelled-in-flight listing is still counted -- it was sent, and GCS charges
for it. If the loser is cancelled before it ever reaches ``_call`` it is not
counted, which makes the number a floor rather than an over-estimate.

Only the HTTP (JSON API) path is counted. Reads from zonal buckets go over gRPC
via the multi-range downloader and do not pass through ``_call``, so their
counts read as ~0.
"""

import functools
import re
import threading
from collections import Counter

from gcsfs.core import GCSFileSystem

# Request kinds. `download` is the only one a whole-object read needs.
DOWNLOAD = "download"
OBJECT_GET = "object_get"
LIST = "list"
OTHER = "other"

# `_call` receives an unformatted path template, e.g. "b/{}/o/{}" for an object
# metadata GET and "b/{}/o" for a listing, so both match before substitution.
_OBJECT_PATH = re.compile(r"/o/[^/]+$")


def classify_request(method, path):
    """Bucket a single gcsfs request by the GCS operation it performs."""
    if "alt=media" in path or "/download/" in path:
        return DOWNLOAD
    if method.upper() == "GET":
        if _OBJECT_PATH.search(path):
            return OBJECT_GET
        if path.endswith("/o"):
            return LIST
    return OTHER


class RequestCounter:
    """Context manager tallying gcsfs HTTP calls by request kind.

    Patches ``GCSFileSystem._call`` for the duration of the block, so it
    captures every filesystem instance in this process -- including the
    ExtendedGCSFileSystem subclass, which does not override ``_call``. It does
    not reach into child processes, so multi-process cases are not counted.
    """

    def __init__(self):
        self.counts = Counter()
        self._lock = threading.Lock()
        self._original_call = None

    @property
    def total(self):
        return sum(self.counts.values())

    def record(self, method, path):
        with self._lock:
            self.counts[classify_request(method, path)] += 1

    def per_operation(self, operations):
        """Requests issued per filesystem operation, or None if unknown."""
        if not operations:
            return None
        return self.total / operations

    def __enter__(self):
        if self._original_call is not None:
            raise RuntimeError("RequestCounter is already active")

        self.counts.clear()
        original = GCSFileSystem._call
        self._original_call = original
        counter = self

        @functools.wraps(original)
        async def counting_call(fs_self, method, path, *args, **kwargs):
            counter.record(method, path)
            return await original(fs_self, method, path, *args, **kwargs)

        GCSFileSystem._call = counting_call
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        # Guard against a second exit. _call is a class attribute, so restoring
        # None here would not just end this measurement -- it would break every
        # GCSFileSystem in the process for the rest of the run.
        if self._original_call is not None:
            GCSFileSystem._call = self._original_call
            self._original_call = None
        return False
