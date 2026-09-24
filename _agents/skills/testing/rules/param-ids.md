---
title: Give Parametrized Cases Readable ids
impact: MEDIUM
impactDescription: Failure output names the scenario, not object reprs
tags: [parametrize, ids, readability]
---

# Give Parametrized Cases Readable ids [MEDIUM]

## Description
pytest auto-generates parametrize ids from argument values, which works for short strings but degrades to `obj0`, `payload1`, or truncated reprs for dicts, dataclasses, and long inputs. Give complex cases explicit ids with `pytest.param(..., id=...)` so failure reports and `-k` selection use the scenario name instead of an opaque index.

## Bad Example
```python
@pytest.mark.parametrize(
    "payload",
    [
        {"type": "invoice.paid", "data": {"amount": 100}},
        {"type": "invoice.paid", "data": {"amount": 0}},
        {"type": "invoice.void", "data": {"amount": 100}},
    ],
)
def test_webhook_handling(payload: dict) -> None: ...

# Failure report:
#   FAILED test_webhooks.py::test_webhook_handling[payload1]
# Which case is payload1? Open the file and count.
```

## Good Example
```python
@pytest.mark.parametrize(
    "payload",
    [
        pytest.param({"type": "invoice.paid", "data": {"amount": 100}}, id="paid"),
        pytest.param({"type": "invoice.paid", "data": {"amount": 0}}, id="paid-zero-amount"),
        pytest.param({"type": "invoice.void", "data": {"amount": 100}}, id="voided"),
    ],
)
def test_webhook_handling(payload: dict) -> None: ...

# Failure report:
#   FAILED test_webhooks.py::test_webhook_handling[paid-zero-amount]
# Run just that case:
#   pytest -k "paid-zero-amount"
```

## Notes
- Simple scalar parameters (`"gold"`, `42`, `True`) already produce readable auto-ids — explicit ids there are noise
- `pytest.param` also carries per-case marks: `pytest.param(..., id="slow-path", marks=pytest.mark.slow)`
- The `ids=` keyword accepts a list or a callable as an alternative to per-case `pytest.param`
- Use `pytest --collect-only -q` to preview the generated ids

## References
- [pytest - Different options for test IDs](https://docs.pytest.org/en/stable/example/parametrize.html#different-options-for-test-ids)
