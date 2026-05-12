import asyncio
from contextlib import asynccontextmanager


@asynccontextmanager
async def parallel_tasks_first_completed(coros):
    """
    Starts coroutines in parallel and enters the context as soon as
    at least one task has completed. Automatically cancels pending tasks
    when exiting the context.
    """
    tasks = [asyncio.create_task(c) for c in coros]

    try:
        # Suspend until the first task finishes for maximum responsiveness
        done, pending = await asyncio.wait(
            set(tasks), return_when=asyncio.FIRST_COMPLETED
        )
        yield tasks, done, pending
    finally:
        # Ensure 'losing' tasks are cancelled immediately
        for t in tasks:
            if not t.done():
                t.cancel()
        # Await all tasks to ensure exceptions are retrieved and cancellation is processed
        await asyncio.gather(*tasks, return_exceptions=True)
