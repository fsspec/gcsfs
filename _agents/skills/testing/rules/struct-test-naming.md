---
title: Name Tests After the Behavior They Verify
impact: HIGH
impactDescription: Failure output reads as a spec of what broke
tags: [structure, naming, readability]
---

# Name Tests After the Behavior They Verify [HIGH]

## Description
A test name should state the scenario and the expected outcome, not just the function it calls. When a test fails in CI, the name is often the only thing you see — `test_process` tells you nothing, `test_expired_token_is_rejected` tells you exactly what broke. Well-named tests double as living documentation of the module's behavior.

## Bad Example
```python
def test_validate() -> None: ...

def test_validate_2() -> None: ...

def test_user() -> None: ...

def test_edge_case() -> None: ...

def test_it_works() -> None: ...
```

## Good Example
```python
def test_expired_token_is_rejected() -> None: ...

def test_token_with_future_nbf_is_rejected() -> None: ...

def test_inactive_user_cannot_login() -> None: ...

def test_empty_cart_total_is_zero() -> None: ...

def test_duplicate_email_raises_conflict_error() -> None: ...
```

## Notes
- A useful pattern: `test_<condition>_<expected result>` — e.g. `test_negative_amount_raises_value_error`
- If a behavior is hard to name, the test probably covers more than one behavior (see [struct-one-behavior](struct-one-behavior.md))
- Group related scenarios in a class (`class TestTokenValidation:`) so names stay short without losing context
- Long test names are fine; tests are read far more often in failure reports than they are typed

## References
- [pytest - Test naming conventions](https://docs.pytest.org/en/stable/explanation/goodpractices.html#conventions-for-python-test-discovery)
