---
title: Resource Management with async with
impact: HIGH
impactDescription: Prevent resource leaks + guaranteed cleanup
tags: [async, asyncio, context-manager, resource-management]
---

# Resource Management with async with [HIGH]

## Description
Async resources (DB connections, HTTP sessions, files, etc.) should be managed with `async with` to ensure cleanup even when exceptions occur. Manual `close()` calls risk resource leaks on errors.

## Bad Example
```python
import aiohttp

async def fetch_data(url: str) -> dict:
    # Dangerous: session leaks if exception occurs
    session = aiohttp.ClientSession()
    response = await session.get(url)  # If exception here...
    data = await response.json()
    await session.close()  # This never runs
    return data

async def query_db() -> list[dict]:
    conn = await database.connect()
    try:
        result = await conn.fetch("SELECT * FROM users")
    finally:
        await conn.close()  # Works but verbose
    return result
```

## Good Example
```python
import aiohttp

async def fetch_data(url: str) -> dict:
    # Safe: automatic cleanup on exception
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            return await response.json()

async def query_db() -> list[dict]:
    async with database.connect() as conn:
        return await conn.fetch("SELECT * FROM users")

# Managing multiple resources
async def process_with_resources() -> None:
    async with (
        aiohttp.ClientSession() as session,
        database.connect() as conn,
    ):
        # Both resources available
        data = await fetch_and_store(session, conn)
```

## Notes
- Custom classes can implement `__aenter__` and `__aexit__` to become async context managers
- `contextlib.asynccontextmanager` decorator simplifies creation
- Python 3.10+ allows grouping multiple `async with` resources in parentheses
- Can mix with synchronous `with` statements

## References
- [Python Docs - async with](https://docs.python.org/3/reference/compound_stmts.html#async-with)
- [contextlib.asynccontextmanager](https://docs.python.org/3/library/contextlib.html#contextlib.asynccontextmanager)
