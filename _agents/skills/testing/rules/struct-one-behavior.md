---
title: Test One Behavior per Test
impact: HIGH
impactDescription: First failure never masks the remaining defects
tags: [structure, isolation, granularity]
---

# Test One Behavior per Test [HIGH]

## Description
Each test should verify a single behavior. When one test checks several behaviors, the first failing assert aborts the test and hides whether the later behaviors also broke, and the test name can no longer say what failed. One behavior per test yields precise failure reports and lets tests fail independently.

## Bad Example
```python
def test_user_lifecycle() -> None:
    user = register(email="alice@example.com")
    assert user.is_active is False          # behavior 1: starts inactive

    user.activate()
    assert user.is_active is True           # behavior 2: activation

    user.change_email("new@example.com")
    assert user.email == "new@example.com"  # behavior 3: email change
    # if behavior 1 fails, we learn nothing about 2 and 3
```

## Good Example
```python
def test_new_user_starts_inactive() -> None:
    user = register(email="alice@example.com")

    assert user.is_active is False


def test_activate_marks_user_active() -> None:
    user = register(email="alice@example.com")

    user.activate()

    assert user.is_active is True


def test_change_email_updates_address() -> None:
    user = register(email="alice@example.com")

    user.change_email("new@example.com")

    assert user.email == "new@example.com"
```

## Notes
- "One behavior" is not "one assert": several asserts examining the result of a single action belong together
- Shared setup that gets repeated across the split tests should move to a fixture, not back into one mega-test
- End-to-end tests may legitimately walk a full scenario; keep them few and separate from unit tests
- Same-action asserts can be grouped without abort-on-first-failure via `pytest.mark.parametrize` or plugins like `pytest-check` — usually unnecessary

## References
- [pytest - Anatomy of a test](https://docs.pytest.org/en/stable/explanation/anatomy.html)
