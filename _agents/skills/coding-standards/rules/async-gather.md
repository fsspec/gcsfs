---
title: Use asyncio.gather for Independent Tasks
impact: HIGH
impactDescription: Significant reduction in I/O wait time
tags: [async, asyncio, concurrency, gather]
---

# Use asyncio.gather for Independent Tasks [HIGH]

## Description
When awaiting multiple independent async operations sequentially, I/O wait times accumulate. Using `asyncio.gather()` runs them concurrently, completing in the time of the slowest operation rather than the sum of all.

## Bad Example
```python
import asyncio

async def fetch_all_data() -> tuple[dict, dict, dict]:
    # Slow: sequential execution accumulates wait time
    users = await fetch_users()       # 1 second
    orders = await fetch_orders()     # 1 second
    products = await fetch_products() # 1 second
    return users, orders, products    # Total: 3 seconds
```

## Good Example
```python
import asyncio

async def fetch_all_data() -> tuple[dict, dict, dict]:
    # Fast: concurrent execution minimizes wait time
    users, orders, products = await asyncio.gather(
        fetch_users(),
        fetch_orders(),
        fetch_products(),
    )
    return users, orders, products  # ~1 second (longest operation only)

# With error handling: you MUST inspect the results for exceptions
async def fetch_all_data_safe() -> dict[str, dict]:
    sources = ("users", "orders", "products")
    results = await asyncio.gather(
        fetch_users(),
        fetch_orders(),
        fetch_products(),
        return_exceptions=True,  # exceptions are returned, not raised
    )

    data: dict[str, dict] = {}
    for name, result in zip(sources, results):
        if isinstance(result, asyncio.CancelledError):
            raise result  # preserve cancellation instead of treating it as data
        if isinstance(result, Exception):
            logger.error("Failed to fetch %s: %s", name, result)
            continue  # or re-raise, retry, use a default...
        data[name] = result
    return data
```

## Notes
- `return_exceptions=True` returns exceptions in the results list instead of raising—you must inspect each item, or failures pass silently.
- `asyncio.CancelledError` is a `BaseException`, not an `Exception`; handle it explicitly before checking ordinary exceptions with `isinstance(result, Exception)`.
- With the default `return_exceptions=False`, the first exception propagates immediately, but the *other* coroutines are **not** cancelled—they keep running in the background and may leak resources. `asyncio.TaskGroup` (3.11+) avoids this by cancelling siblings on failure; prefer it when you need all-or-nothing semantics.
- Don't include dependent tasks in `gather` (await prerequisites separately first)
- For many tasks, combine with `asyncio.Semaphore` to limit concurrency
- Wrap a `gather` in `async with asyncio.timeout(seconds):` (3.11+) to bound total wait time

## References
- [Python Docs - asyncio.gather](https://docs.python.org/3/library/asyncio-task.html#asyncio.gather)
