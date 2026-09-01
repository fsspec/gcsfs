import asyncio
import gc
import sys
import threading
import time
from unittest import mock

import fsspec.asyn
import pytest

from gcsfs import core
from gcsfs.zonal_file import ZonalFile


def wait_until(predicate, timeout=10.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.005)
    return predicate()


@pytest.fixture
def io_loop():
    return fsspec.asyn.get_loop()


@pytest.fixture
def fake_fs(io_loop):
    fs = mock.Mock()
    fs.split_path.return_value = ("b", "test-key", "1")
    fs.info.return_value = {"size": 1000, "generation": "1", "name": "test-key"}
    fs.loop = io_loop
    return fs


def make_read_file(fake_fs, pool_close):
    pool = mock.Mock(persisted_size=1000, details=None, close=pool_close)
    fake_fs._mrd_pool_cache.get = mock.AsyncMock(return_value=pool)
    return ZonalFile(gcsfs=fake_fs, path="gs://b/test-key", mode="rb")


def finalize_on_loop(io_loop, holder):
    async def _drop_and_collect():
        holder.clear()
        gc.collect()

    fsspec.asyn.sync(io_loop, _drop_and_collect)


def test_read_file_finalized_on_loop_thread_releases_mrd_pool(
    fake_fs, io_loop, monkeypatch
):
    seen = []
    monkeypatch.setattr(sys, "unraisablehook", seen.append)
    closed = threading.Event()
    zf = make_read_file(fake_fs, lambda: closed.set() or asyncio.sleep(0))
    holder = [zf]
    del zf
    finalize_on_loop(io_loop, holder)
    assert wait_until(closed.is_set), "mrd_pool.close() was never awaited"
    assert seen == [], f"exception escaped the finalizer: {seen}"


def test_write_file_finalized_on_loop_thread_flushes_and_finalizes(fake_fs, io_loop):
    zf = ZonalFile(
        gcsfs=fake_fs, path="gs://b/test-key", mode="wb", finalize_on_close=True
    )
    aaow = mock.Mock()
    aaow.flush = mock.AsyncMock()
    aaow.close = mock.AsyncMock()
    zf.aaow = aaow
    zf.buffer.write(b"important data")
    zf.loc = 14
    holder = [zf]
    del zf
    finalize_on_loop(io_loop, holder)
    assert wait_until(lambda: aaow.flush.called), "buffered data was never flushed"
    assert wait_until(lambda: aaow.close.called), "writer was never finalized"
    assert aaow.close.call_args.kwargs.get("finalize_on_close") is True


def test_close_on_loop_thread_is_idempotent(fake_fs, io_loop):
    calls = []

    async def pool_close():
        calls.append(1)

    zf = make_read_file(fake_fs, pool_close)

    async def close_twice():
        zf.close()
        zf.close()

    fsspec.asyn.sync(io_loop, close_twice)
    assert wait_until(lambda: len(calls) == 1), "teardown did not run exactly once"
    time.sleep(0.2)
    assert calls == [1], f"mrd_pool.close() ran {len(calls)} times"


def test_deferred_closes_share_a_bounded_thread_pool(fake_fs, io_loop):
    n_files = 64
    workers = []
    lock = threading.Lock()

    class RecordingZonalFile(ZonalFile):
        def _close_impl(self):
            current = threading.current_thread()
            with lock:
                workers.append((current.ident, current.name))
            super()._close_impl()

    async def pool_close():
        pass

    files = []
    for _ in range(n_files):
        pool = mock.Mock(persisted_size=1000, details=None, close=pool_close)
        fake_fs._mrd_pool_cache.get = mock.AsyncMock(return_value=pool)
        zf = RecordingZonalFile(gcsfs=fake_fs, path="gs://b/test-key", mode="rb")
        zf.mrd_pool = pool
        files.append(zf)

    async def close_all():
        for zf in files:
            zf.close()

    fsspec.asyn.sync(io_loop, close_all)

    assert wait_until(
        lambda: len(workers) == n_files
    ), f"only {len(workers)}/{n_files} deferred closes ran"
    idents = {ident for ident, _ in workers}
    assert len(idents) == 1, (
        f"{len(idents)} threads used for {n_files} closes; deferred closes "
        "should share one worker"
    )
    assert all(
        name.startswith(core._DEFERRED_CLOSE_THREAD_NAME) for _, name in workers
    ), f"closes ran on unexpected threads: {sorted({n for _, n in workers})}"


def test_off_loop_close_still_blocks_and_propagates_errors(fake_fs):
    started = threading.Event()

    async def failing_close():
        started.set()
        raise RuntimeError("MRD pool teardown failed")

    zf = make_read_file(fake_fs, failing_close)
    with pytest.raises(RuntimeError, match="MRD pool teardown failed"):
        zf.close()
    assert started.is_set(), "close() must run the teardown synchronously"


def test_gcsfile_finalizer_when_already_closed_or_deferred(fake_fs):
    f = core.GCSFile(fake_fs, "gs://b/test-key", mode="rb")
    f.closed = True
    with mock.patch.object(core, "_defer_close") as mock_defer:
        f.__del__()
        mock_defer.assert_not_called()

    f2 = core.GCSFile(fake_fs, "gs://b/test-key", mode="rb")
    f2._close_deferred = True
    with mock.patch.object(core, "_defer_close") as mock_defer:
        f2.__del__()
        mock_defer.assert_not_called()


def test_gcsfile_finalizer_when_sys_finalizing(fake_fs, monkeypatch):
    monkeypatch.setattr(sys, "is_finalizing", lambda: True)
    f = core.GCSFile(fake_fs, "gs://b/test-key", mode="rb")
    with mock.patch.object(core, "_defer_close") as mock_defer:
        f.__del__()
        assert f.closed is True
        mock_defer.assert_not_called()


def test_gcsfile_finalizer_when_queue_is_none(fake_fs, monkeypatch):
    f = core.GCSFile(fake_fs, "gs://b/test-key", mode="rb", cache_type="readahead")
    monkeypatch.setattr(core, "_deferred_close_queue", None)
    with mock.patch.object(core, "_defer_close") as mock_defer:
        f.__del__()
        assert f.closed is True
        mock_defer.assert_not_called()


def test_gcsfile_finalized_on_loop_thread_defers_close(fake_fs, io_loop, monkeypatch):
    seen = []
    monkeypatch.setattr(sys, "unraisablehook", seen.append)
    closed = threading.Event()

    class RecordingGCSFile(core.GCSFile):
        def _close_impl(self):
            closed.set()
            super()._close_impl()

    gf = RecordingGCSFile(fake_fs, "gs://b/test-key", mode="rb", cache_type="readahead")
    holder = [gf]
    del gf
    finalize_on_loop(io_loop, holder)
    assert wait_until(closed.is_set), "GCSFile._close_impl() was never called"
    assert seen == [], f"exception escaped the finalizer: {seen}"
