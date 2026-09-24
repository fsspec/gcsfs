---
title: Replace Duplicated Tests with parametrize
impact: MEDIUM
impactDescription: Every case runs and reports independently
tags: [parametrize, duplication, coverage]
---

# Replace Duplicated Tests with parametrize [MEDIUM]

## Description
When several tests differ only in input and expected output, collapse them into one `@pytest.mark.parametrize` test. Unlike a loop inside a test, each case runs as an independent test: all cases execute even when one fails, failures name the exact case, and adding a case is one line. It also makes gaps in the case table visible at a glance.

## Bad Example
```python
def test_slugify_spaces() -> None:
    assert slugify("Hello World") == "hello-world"

def test_slugify_unicode() -> None:
    assert slugify("Héllo") == "hello"

def test_slugify_special() -> None:
    assert slugify("a/b?c") == "a-b-c"

# or worse — one test looping, stopping at the first failure:
def test_slugify() -> None:
    for raw, expected in [("Hello World", "hello-world"), ("Héllo", "hello")]:
        assert slugify(raw) == expected
```

## Good Example
```python
@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("Hello World", "hello-world"),
        ("Héllo", "hello"),
        ("a/b?c", "a-b-c"),
        ("  trimmed  ", "trimmed"),
        ("", ""),
    ],
)
def test_slugify(raw: str, expected: str) -> None:
    assert slugify(raw) == expected
```

## Notes
- Parametrize only tests that share the *same* behavior and assertion shape; different behaviors keep separate tests (see [struct-one-behavior](struct-one-behavior.md))
- Expected exceptions can be parametrized by passing context managers as parameters — `pytest.raises(ZeroDivisionError)` for raising cases, `contextlib.nullcontext()` for passing ones — then wrap the Act step in `with expectation:`
- Stacked `@parametrize` decorators produce the cartesian product of cases — handy, but watch the count
- Parametrize a *fixture* (`@pytest.fixture(params=[...])`) when every test using it should run against each variant

## References
- [pytest - How to parametrize fixtures and test functions](https://docs.pytest.org/en/stable/how-to/parametrize.html)
- [pytest - Parametrizing conditional raising](https://docs.pytest.org/en/stable/example/parametrize.html#parametrizing-conditional-raising)
