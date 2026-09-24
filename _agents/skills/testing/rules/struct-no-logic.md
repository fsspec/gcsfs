---
title: Keep Conditionals and Loops Out of Tests
impact: HIGH
impactDescription: Eliminates bugs-in-tests and silently skipped assertions
tags: [structure, simplicity, test-quality]
---

# Keep Conditionals and Loops Out of Tests [HIGH]

## Description
Tests with `if`/`for`/`try` need tests of their own. A conditional can make a whole branch of assertions unreachable, a loop hides which iteration failed, and re-implementing the production formula in the test just duplicates its bugs. Tests should be straight-line code: literal inputs, one path, expected values written as literals.

## Bad Example
```python
def test_discounts() -> None:
    for tier, rate in [("gold", 0.2), ("silver", 0.1), ("bronze", 0.05)]:
        price = discounted_price(1000, tier)
        # re-implements production logic — shares its bugs
        expected = 1000 * (1 - rate)
        if tier == "gold":
            assert price == expected
            assert price < 900
        else:
            assert price == expected
    # when this fails, which tier broke?


def test_optional_feature() -> None:
    result = fetch_settings()
    if result.has_beta_flags:      # if False, test asserts nothing
        assert result.beta_flags == []
```

## Good Example
```python
@pytest.mark.parametrize(
    ("tier", "expected"),
    [
        pytest.param("gold", 800, id="gold-20pct"),
        pytest.param("silver", 900, id="silver-10pct"),
        pytest.param("bronze", 950, id="bronze-5pct"),
    ],
)
def test_tier_discount(tier: str, expected: int) -> None:
    assert discounted_price(1000, tier) == expected


def test_settings_include_empty_beta_flags() -> None:
    result = fetch_settings()

    assert result.beta_flags == []  # unconditional
```

## Notes
- Replace loops over cases with `@pytest.mark.parametrize` (see [param-parametrize](param-parametrize.md))
- Compute expected values by hand and write them as literals; never derive them with the same formula as production code
- Use `pytest.raises(...)` instead of `try/except` + flag variables
- Helper functions/fixtures may contain logic, but keep assertions in the test body where failure output points at them

## References
- [Google Testing Blog - Don't Put Logic in Tests](https://testing.googleblog.com/2014/07/testing-on-toilet-dont-put-logic-in.html)
