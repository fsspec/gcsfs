---
title: Use the Narrowest Fixture Scope That Works
impact: HIGH
impactDescription: Prevents cross-test state leakage and order-dependent failures
tags: [fixtures, scope, isolation]
---

# Use the Narrowest Fixture Scope That Works [HIGH]

## Description
Fixtures default to function scope — a fresh instance per test — and that should be your default too. Widen to `module` or `session` scope only for genuinely expensive, effectively immutable resources (a database container, a compiled model). A mutable object shared across tests lets one test's leftovers change another test's result depending on execution order.

## Bad Example
```python
@pytest.fixture(scope="session")   # shared for the whole run
def cart() -> Cart:
    return Cart()


def test_add_item(cart: Cart) -> None:
    cart.add(Item("apple", 100))
    assert cart.total() == 100     # passes


def test_empty_cart_total_is_zero(cart: Cart) -> None:
    assert cart.total() == 0       # fails — apple leaked from previous test
```

## Good Example
```python
@pytest.fixture                    # function scope: fresh per test
def cart() -> Cart:
    return Cart()


# Wide scope reserved for expensive, read-only resources,
# with per-test cleanup restoring isolation
@pytest.fixture(scope="session")
def db_engine() -> Iterator[Engine]:
    engine = create_engine(TEST_DB_URL)
    yield engine
    engine.dispose()


@pytest.fixture
def db_session(db_engine: Engine) -> Iterator[Session]:
    connection = db_engine.connect()
    transaction = connection.begin()
    session = Session(bind=connection)

    yield session

    session.close()
    transaction.rollback()         # each test sees a clean database
    connection.close()
```

## Notes
- Pattern: expensive resource at `session` scope + cheap function-scoped wrapper that resets state (transaction rollback, `clear()`, re-seed)
- Use `yield` fixtures for teardown; code after `yield` runs even when the test fails
- `pytest --setup-show` displays which fixtures are created and torn down where — useful for auditing scopes
- Random-order plugins (`pytest-randomly`) surface scope-leak bugs early

## References
- [pytest - Fixture scopes](https://docs.pytest.org/en/stable/how-to/fixtures.html#scope-sharing-fixtures-across-classes-modules-packages-or-session)
