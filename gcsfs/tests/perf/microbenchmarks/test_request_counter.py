"""Request-budget guards for whole-object reads.

These run in CI (``pytest gcsfs/tests/perf/microbenchmarks --run-benchmarks-infra``)
and need no network: ``GCSFileSystem._request`` is replaced with a stub that
serves canned responses and records what was asked for.

The point is fsspec/gcsfs#1048. Reading a small object went from one HTTP
round-trip to three and nothing caught it, because every existing microbenchmark
measures wall-clock time on objects large enough for two extra metadata calls to
disappear into the transfer. A round-trip budget is exact, so it catches the
regression on the first run instead of after a user reports a 50% slowdown.
"""

import json

import pytest

from gcsfs.core import GCSFileSystem
from gcsfs.tests.perf.microbenchmarks.request_counter import (
    DOWNLOAD,
    LIST,
    OBJECT_GET,
    OTHER,
    RequestCounter,
    classify_request,
)

CONTENT = b"x" * 8
OBJECT_PATH = "test-bucket/small-object"


@pytest.fixture
def offline_gcs(monkeypatch):
    """A GCSFileSystem whose requests are served locally, never over the wire."""

    async def fake_request(self, method, path, *args, headers=None, **kwargs):
        url = path.format(*args) if args else path

        if "alt=media" in url or "/download/" in url:
            body = CONTENT
            byte_range = (headers or {}).get("Range")
            if byte_range:
                start, _, end = byte_range.replace("bytes=", "").partition("-")
                body = CONTENT[int(start) : int(end) + 1 if end else None]
            return 200, {}, None, body

        metadata = {
            "kind": "storage#object",
            "name": "small-object",
            "bucket": "test-bucket",
            "size": str(len(CONTENT)),
        }
        if "/o/" in url:
            return 200, {}, None, json.dumps(metadata).encode()
        if url.endswith("/o"):
            return (
                200,
                {},
                None,
                json.dumps({"kind": "storage#objects", "items": [metadata]}).encode(),
            )
        raise AssertionError(f"unexpected request: {method} {url}")

    monkeypatch.setattr(GCSFileSystem, "_request", fake_request)
    GCSFileSystem.clear_instance_cache()
    fs = GCSFileSystem(token="anon", project="test-project")
    fs.invalidate_cache()
    yield fs
    GCSFileSystem.clear_instance_cache()


def _count_read(fs, **kwargs):
    with RequestCounter() as counter:
        assert fs.cat_file(OBJECT_PATH, **kwargs) == CONTENT
    return counter


def test_classify_request():
    download = "https://storage.googleapis.com/download/storage/v1/b/b/o/k?alt=media"
    assert classify_request("GET", download) == DOWNLOAD
    assert classify_request("GET", "b/{}/o/{}") == OBJECT_GET
    assert classify_request("GET", "b/{}/o") == LIST
    # Only reads are classified; an upload posts to the same listing path.
    assert classify_request("POST", "b/{}/o") == OTHER
    assert classify_request("DELETE", "b/{}/o/{}") == OTHER


def test_counter_restores_call():
    original = GCSFileSystem._call
    with RequestCounter():
        assert GCSFileSystem._call is not original
    assert GCSFileSystem._call is original


def test_counter_restores_call_on_error():
    original = GCSFileSystem._call
    with pytest.raises(ValueError):
        with RequestCounter():
            raise ValueError("boom")
    assert GCSFileSystem._call is original


def test_counter_rejects_reentry():
    with RequestCounter() as counter:
        with pytest.raises(RuntimeError):
            counter.__enter__()


def test_counter_per_operation():
    counter = RequestCounter()
    counter.record("GET", "https://x/download/storage/v1/b/b/o/k?alt=media")
    counter.record("GET", "b/{}/o/{}")
    assert counter.total == 2
    assert counter.per_operation(2) == 1.0
    assert counter.per_operation(0) is None


def test_ranged_read_costs_one_request(offline_gcs):
    """A caller-supplied range needs no size lookup, so it stays at one call."""
    counter = _count_read(offline_gcs, start=0, end=len(CONTENT))

    assert counter.total == 1
    assert counter.counts[DOWNLOAD] == 1


def test_sequential_read_costs_one_request(offline_gcs):
    """concurrency=1 takes _cat_file_sequential, which never resolves the size."""
    counter = _count_read(offline_gcs, concurrency=1)

    assert counter.total == 1
    assert counter.counts[DOWNLOAD] == 1


@pytest.mark.xfail(
    strict=False,
    reason=(
        "fsspec/gcsfs#1048: with the default concurrency of 4, cat_file() routes "
        "through _cat_file_concurrent(), which resolves the size via _info() -- an "
        "object GET raced against an objects.list -- before downloading. For an "
        "object below MIN_CHUNK_SIZE_FOR_CONCURRENCY the range is never split, so "
        "both lookups are wasted and the read costs 3 round-trips instead of 1. "
        "Remove this marker once cat_file() takes the size from the Content-Range "
        "of a first ranged GET."
    ),
)
def test_whole_object_read_costs_one_request(offline_gcs):
    """An object smaller than MIN_CHUNK_SIZE_FOR_CONCURRENCY needs one GET.

    Nothing about a whole-object read requires knowing the size up front: the
    server returns the whole object, and the concurrent path would not split a
    range this small anyway.
    """
    assert len(CONTENT) < GCSFileSystem.MIN_CHUNK_SIZE_FOR_CONCURRENCY

    counter = _count_read(offline_gcs)

    assert (
        counter.total == 1
    ), f"whole-object read cost {counter.total} round-trips: {dict(counter.counts)}"
