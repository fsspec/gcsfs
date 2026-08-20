import asyncio
from contextlib import asynccontextmanager


def split_range(size, concurrency, min_chunk_size):
    """Split a byte range into no more chunks than the configured minimum warrants."""
    if size <= 0:
        return []

    min_chunk_size = max(1, min_chunk_size)
    if concurrency <= 1 or size < min_chunk_size:
        return [(0, size)]

    num_chunks = min(concurrency, size // min_chunk_size)
    chunk_size, remainder = divmod(size, num_chunks)
    return [
        (i * chunk_size, chunk_size if i < num_chunks - 1 else chunk_size + remainder)
        for i in range(num_chunks)
    ]


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
