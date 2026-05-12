import asyncio

import pytest

from gcsfs.concurrency import parallel_tasks_first_completed


@pytest.mark.asyncio
async def test_parallel_tasks_first_completed_basic():
    async def slow_task():
        await asyncio.sleep(1)
        return "slow"

    async def fast_task():
        await asyncio.sleep(0.1)
        return "fast"

    async with parallel_tasks_first_completed([slow_task(), fast_task()]) as (
        tasks,
        done,
        pending,
    ):
        assert len(done) == 1
        assert len(pending) == 1
        completed_task = done.pop()
        assert completed_task.result() == "fast"
        assert len(tasks) == 2


@pytest.mark.asyncio
async def test_parallel_tasks_first_completed_cancellation():
    task_cancelled = False

    async def slow_task():
        nonlocal task_cancelled
        try:
            await asyncio.sleep(1)
        except asyncio.CancelledError:
            task_cancelled = True
            raise

    async def fast_task():
        await asyncio.sleep(0.1)
        return "fast"

    async with parallel_tasks_first_completed([slow_task(), fast_task()]) as (
        tasks,
        done,
        pending,
    ):
        assert len(done) == 1
        completed_task = done.pop()
        assert completed_task.result() == "fast"

    # After exiting context, slow_task should be cancelled
    await asyncio.sleep(0.1)  # Give it a moment to run cancellation cleanup
    assert task_cancelled


@pytest.mark.asyncio
async def test_parallel_tasks_first_completed_exception():
    async def error_task():
        await asyncio.sleep(0.1)
        raise ValueError("error")

    async def slow_task():
        await asyncio.sleep(1)
        return "slow"

    async with parallel_tasks_first_completed([error_task(), slow_task()]) as (
        tasks,
        done,
        pending,
    ):
        assert len(done) == 1
        completed_task = done.pop()
        with pytest.raises(ValueError, match="error"):
            completed_task.result()


@pytest.mark.asyncio
async def test_parallel_tasks_unretrieved_exception_fix():
    async def get_call():
        await asyncio.sleep(0.5)
        return "ok"

    async def error_task():
        await asyncio.sleep(0.1)
        raise ValueError("error in list")

    async with parallel_tasks_first_completed([get_call(), error_task()]) as (
        tasks,
        done,
        pending,
    ):
        get_object_task, get_directory_info_task = tasks
        try:
            res = await get_object_task
            assert res == "ok"
        except ValueError:
            pytest.fail("Unexpected ValueError")
