---
title: Mock External Boundaries, Not Internal Logic
impact: CRITICAL
impactDescription: Over-mocked tests pass while production breaks
tags: [mocking, boundaries, test-quality]
---

# Mock External Boundaries, Not Internal Logic [CRITICAL]

## Description
Mock only what your code does not own: network calls, databases, clocks, file systems, third-party APIs. When internal functions and classes are mocked, the test verifies the mock wiring instead of the behavior — it keeps passing after the real logic breaks, and it breaks after harmless refactoring.

## Bad Example
```python
def test_order_total(mocker: MockerFixture) -> None:
    # Mocks internal logic: the real calculation is never executed
    mocker.patch("app.orders.calculate_subtotal", return_value=100)
    mocker.patch("app.orders.calculate_tax", return_value=10)

    order = Order(items=[Item(price=50, quantity=2)])

    # Only proves that total = subtotal + tax wiring exists
    assert order.total() == 110
```

## Good Example
```python
def test_order_total() -> None:
    # Pure internal logic: no mocks needed at all
    order = Order(items=[Item(price=50, quantity=2)])

    assert order.total() == 110  # 100 + 10% tax


def test_order_is_persisted(mocker: MockerFixture) -> None:
    # Mock the external boundary (database), run the real logic
    save = mocker.patch("app.orders.repository.save", autospec=True)

    place_order(items=[Item(price=50, quantity=2)])

    save.assert_called_once()
    assert save.call_args.args[0].total == 110
```

## Notes
- The `mocker` fixture comes from the pytest-mock plugin (`pip install pytest-mock`); with stdlib only, use `unittest.mock.patch` as a decorator or context manager
- If a test needs many mocks, the code under test likely has too many dependencies — consider refactoring instead of mocking harder
- Prefer fakes (in-memory repository, `tmp_path`) over mocks when the boundary has rich behavior
- Never patch the module under test itself; patch where the dependency is *looked up* (`app.orders.repository`, not `app.db.repository`)
- Clocks are a boundary too: inject time or use libraries like `freezegun` rather than `time.sleep`

## References
- [unittest.mock - Where to patch](https://docs.python.org/3/library/unittest.mock.html#where-to-patch)
- [Mocks Aren't Stubs (Martin Fowler)](https://martinfowler.com/articles/mocksArentStubs.html)
