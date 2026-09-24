---
title: Structure Tests as Arrange-Act-Assert
impact: HIGH
impactDescription: Failures are diagnosed from the test body alone
tags: [structure, aaa, readability]
---

# Structure Tests as Arrange-Act-Assert [HIGH]

## Description
Organize every test into three visible phases: Arrange (set up inputs and state), Act (call the one thing under test), Assert (verify the outcome). A reader should identify what is being tested and why it might fail without reading the implementation. Interleaved setup and assertions hide the subject of the test.

## Bad Example
```python
def test_cart() -> None:
    cart = Cart()
    cart.add(Item("apple", 100))
    assert len(cart.items) == 1          # asserting mid-setup
    cart.add(Item("banana", 50))
    cart.apply_coupon("SAVE10")
    assert cart.total() == 135
    cart.remove("apple")                 # second act phase
    assert cart.total() == 45
```

## Good Example
```python
def test_coupon_discounts_total() -> None:
    # Arrange
    cart = Cart()
    cart.add(Item("apple", 100))
    cart.add(Item("banana", 50))

    # Act
    cart.apply_coupon("SAVE10")

    # Assert
    assert cart.total() == 135


def test_removing_item_recalculates_total() -> None:
    # Arrange
    cart = Cart()
    cart.add(Item("apple", 100))
    cart.add(Item("banana", 50))

    # Act
    cart.remove("apple")

    # Assert
    assert cart.total() == 50
```

## Notes
- The comment markers are optional; blank lines separating the three phases are usually enough
- A long Arrange phase is a signal to extract a fixture or factory (see [fixture-factory](fixture-factory.md))
- Multiple asserts on the *same* action are fine; a second Act phase means a second test (see [struct-one-behavior](struct-one-behavior.md))
- Also known as Given-When-Then

## References
- [Arrange-Act-Assert (Bill Wake)](https://xp123.com/3a-arrange-act-assert/)
- [pytest - Anatomy of a test](https://docs.pytest.org/en/stable/explanation/anatomy.html)
