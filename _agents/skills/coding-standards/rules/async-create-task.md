---
title: Proper Background Task Creation
impact: HIGH
impactDescription: Prevent GC issues by holding task references
tags: [async, asyncio, task, background]
---

# Proper Background Task Creation [HIGH]

## Description
Tasks created with `asyncio.create_task()` may be garbage collected and cancelled before completion if no reference is held. Additionally, exceptions in "fire-and-forget" tasks are never raised to your code—they only surface as a `Task exception was never retrieved` message logged when the task object is finalized, which is easy to miss.

## Bad Example
```python
import asyncio

async def main() -> None:
    # Dangerous: no reference to task
    asyncio.create_task(background_work())  # May be GC'd

    # Problem: exceptions are swallowed
    asyncio.create_task(risky_operation())  # Exceptions not reported
```

## Good Example
```python
import asyncio
import logging
from collections.abc import Coroutine
from typing import Any

logger = logging.getLogger(__name__)

# Global task set to hold strong references until each task finishes
background_tasks: set[asyncio.Task[None]] = set()

def spawn_background_task(coro: Coroutine[Any, Any, None]) -> asyncio.Task[None]:
    task = asyncio.create_task(coro)
    background_tasks.add(task)
    task.add_done_callback(_handle_background_task_result)
    return task

def _handle_background_task_result(task: asyncio.Task[None]) -> None:
    background_tasks.discard(task)  # release the strong reference
    if task.cancelled():
        return  # cancellation is already the task's defined terminal state
    try:
        task.result()  # retrieve exceptions so they are not silently logged later
    except Exception:
        logger.exception("Background task failed")

async def main() -> None:
    # Safe: a strong reference is held while the task runs
    spawn_background_task(background_work())

    # Or explicitly await the result when you need it
    task = asyncio.create_task(important_work())
    result = await task  # failures propagate to the caller

# Python 3.11+ : TaskGroup for *structured* concurrency
# Note: this blocks until all child tasks finish, so it is NOT fire-and-forget.
# Use it when the spawning scope should wait for and own the tasks.
async def main_modern() -> None:
    async with asyncio.TaskGroup() as tg:
        tg.create_task(work_a())
        tg.create_task(work_b())
    # Reaching here = all tasks complete; if any raised, an ExceptionGroup
    # propagates and the remaining tasks are cancelled.
```

## Notes
- Annotate the reference holder as `set[asyncio.Task[None]]` (builtin generic, Python 3.9+). Do **not** use `collections.abc.Set` for this—it is the immutable abstract type and does not expose `add()`/`discard()`.
- `task.add_done_callback()` registers handlers for task completion; retrieve `task.result()` or `task.exception()` there so failures are reported deterministically.
- A done callback is an explicit task-supervisor boundary: report unexpected failures there rather than silently discarding the result.
- Use `asyncio.TaskGroup` (3.11+) for tasks the current scope should *wait for*; use the reference-holding pattern above for true fire-and-forget tasks that outlive the call.
- Long-running background tasks should be cancellable via `task.cancel()`. Inside the task, let `asyncio.CancelledError` propagate—don't swallow it in a bare `except`.
- Enable warnings during debug with `asyncio.get_running_loop().set_debug(True)`

## References
- [Python Docs - asyncio.create_task](https://docs.python.org/3/library/asyncio-task.html#asyncio.create_task)
- [PEP 654 - Exception Groups and except*](https://peps.python.org/pep-0654/)
