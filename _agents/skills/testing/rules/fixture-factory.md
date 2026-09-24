---
title: Use Factory Fixtures for Varied Test Data
impact: HIGH
impactDescription: One fixture serves every variation without duplication
tags: [fixtures, factory, test-data]
---

# Use Factory Fixtures for Varied Test Data [HIGH]

## Description
When tests need the same kind of object with different attributes, return a factory function from the fixture instead of a fixed instance. A fixed fixture forces a new fixture per variation (`user`, `admin_user`, `inactive_user`, ...), while a factory keeps defaults in one place and lets each test state only the attributes it cares about — which also documents what the test depends on.

## Bad Example
```python
@pytest.fixture
def user() -> User:
    return User(name="alice", email="alice@example.com", active=True, role="member")

@pytest.fixture
def admin_user() -> User:
    return User(name="bob", email="bob@example.com", active=True, role="admin")

@pytest.fixture
def inactive_user() -> User:
    return User(name="carol", email="carol@example.com", active=False, role="member")
# a fixture explosion, and tests can't create two users with chosen attributes
```

## Good Example
```python
import itertools
from collections.abc import Callable

import pytest


@pytest.fixture
def make_user() -> Callable[..., User]:
    counter = itertools.count()

    def _make(
        *,
        name: str | None = None,
        active: bool = True,
        role: str = "member",
    ) -> User:
        name = name or f"user{next(counter)}"
        return User(name=name, email=f"{name}@example.com", active=active, role=role)

    return _make


def test_admin_can_delete_posts(make_user: Callable[..., User]) -> None:
    admin = make_user(role="admin")   # only the relevant attribute is stated

    assert can_delete_posts(admin)


def test_users_have_unique_emails(make_user: Callable[..., User]) -> None:
    assert make_user().email != make_user().email
```

## Notes
- Use keyword-only parameters in the factory so call sites stay self-documenting
- If created objects need cleanup, collect them in a list inside the fixture and tear down after `yield`
- For large domain models, the `factory_boy` library provides the same pattern with less boilerplate
- Attributes a test doesn't mention should get sensible valid defaults — tests must not depend on them

## References
- [pytest - Factories as fixtures](https://docs.pytest.org/en/stable/how-to/fixtures.html#factories-as-fixtures)
- [factory_boy](https://factoryboy.readthedocs.io/)
