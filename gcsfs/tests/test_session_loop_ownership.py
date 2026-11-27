"""Offline regression for gcsfs session close routing (no connections opened)."""

import asyncio
import gc
import queue
import threading
import weakref
from contextlib import contextmanager

import aiohttp
import fsspec.asyn
import pytest

from gcsfs.core import GCSFileSystem


@contextmanager
def running_loop():
    loop = asyncio.new_event_loop()
    ready = threading.Event()
    loop.call_soon(ready.set)
    thread = threading.Thread(target=loop.run_forever, daemon=True)
    thread.start()
    assert ready.wait(2)
    try:
        yield loop
    finally:
        loop.call_soon_threadsafe(loop.stop)
        thread.join(2)
        assert not thread.is_alive()
        loop.close()


def on(loop, fn):
    async def invoke():
        return await fn()

    return asyncio.run_coroutine_threadsafe(invoke(), loop).result(2)


@pytest.mark.parametrize("caller", ["ordinary-thread", "foreign-loop", "owner-loop"])
def test_finalizer_closes_real_session_on_owner_not_other_loops(monkeypatch, caller):
    seen = queue.Queue()
    original = aiohttp.ClientSession.close

    async def close(session):
        running = asyncio.get_running_loop()
        await original(session)
        seen.put(running)

    monkeypatch.setattr(aiohttp.ClientSession, "close", close)
    shared = fsspec.asyn.get_loop()
    with running_loop() as owner, running_loop() as foreign:

        async def start():
            fs = GCSFileSystem(
                asynchronous=True, token="anon", skip_instance_cache=True
            )
            return fs, await fs._set_session()

        fs, session = on(owner, start)
        assert owner is not shared and owner is not foreign
        assert session._loop is owner and fs.loop is None and not session.closed
        fs_ref = weakref.ref(fs)
        holder = [fs]
        del fs

        def collect():
            holder.clear()
            gc.collect()

        async def async_collect():
            collect()

        if caller == "ordinary-thread":
            collect()
        else:
            on(owner if caller == "owner-loop" else foreign, async_collect)
        assert fs_ref() is None
        try:
            actual = seen.get(timeout=2)
            assert actual is owner, "session closed on a foreign event loop"
        finally:
            if not session.closed:
                on(owner, lambda: original(session))
        assert session.closed


def test_already_closed_is_noop_and_default_sync_close_stays_synchronous(monkeypatch):
    seen = []
    original = aiohttp.ClientSession.close

    async def close(session):
        seen.append(asyncio.get_running_loop())
        await original(session)

    monkeypatch.setattr(aiohttp.ClientSession, "close", close)
    shared = fsspec.asyn.get_loop()

    async def create():
        return aiohttp.ClientSession()

    session = on(shared, create)
    GCSFileSystem.close_session(None, session, asynchronous=False)
    assert session.closed
    GCSFileSystem.close_session(None, session, asynchronous=False)
    assert seen == [shared]


@pytest.mark.parametrize("explicit_foreign", [False, True])
def test_dead_owner_uses_shutdown_fallback(monkeypatch, explicit_foreign):
    original = aiohttp.ClientSession.close
    seen = queue.Queue()

    async def close(session):
        seen.put(asyncio.get_running_loop())
        await original(session)

    monkeypatch.setattr(aiohttp.ClientSession, "close", close)

    async def create():
        return aiohttp.ClientSession()

    with running_loop() as dead_owner:
        session = on(dead_owner, create)
    try:
        with running_loop() as foreign:

            async def trigger():
                GCSFileSystem.close_session(
                    foreign if explicit_foreign else None, session, asynchronous=True
                )
                # Give any mistakenly-scheduled task a turn.
                await asyncio.sleep(0)

            on(foreign, trigger)
        assert (
            session.closed and seen.empty()
        ), "should not schedule close on a foreign loop"
    finally:
        if not session.closed:
            session._connector._close()
