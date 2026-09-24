---
title: Use monkeypatch for Environment Variables and Attributes
impact: HIGH
impactDescription: Guaranteed cleanup prevents state leaking between tests
tags: [mocking, monkeypatch, environment, isolation]
---

# Use monkeypatch for Environment Variables and Attributes [HIGH]

## Description
Use pytest's `monkeypatch` fixture to modify environment variables, attributes, dict entries, and the working directory. Every change is automatically undone when the test ends, so tests stay isolated. Mutating `os.environ` or module globals directly leaks state into later tests and causes order-dependent failures.

## Bad Example
```python
import os

def test_reads_api_key() -> None:
    os.environ["API_KEY"] = "test-key"  # leaks into every following test

    config = load_config()

    assert config.api_key == "test-key"
    del os.environ["API_KEY"]  # skipped entirely if the assert fails


def test_debug_mode() -> None:
    settings.DEBUG = True  # permanent for the rest of the session
    assert render_error().shows_traceback
```

## Good Example
```python
def test_reads_api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("API_KEY", "test-key")  # undone automatically

    config = load_config()

    assert config.api_key == "test-key"


def test_missing_api_key_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("API_KEY", raising=False)

    with pytest.raises(ConfigError):
        load_config()


def test_debug_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "DEBUG", True)  # restored after the test
    assert render_error().shows_traceback
```

## Notes
- `monkeypatch.setattr` also replaces functions: `monkeypatch.setattr(app.pay, "charge", fake_charge)` — a lightweight alternative to `mock.patch` when you don't need call assertions
- Use `monkeypatch.chdir(tmp_path)` instead of `os.chdir` for the same cleanup guarantee
- For env vars needed by many tests, wrap monkeypatch in a fixture rather than repeating setenv calls
- The `monkeypatch` fixture is function-scoped; for a session-scoped patch, create `mp = pytest.MonkeyPatch()` inside a session-scoped fixture, `yield`, then call `mp.undo()` in teardown

## References
- [pytest - How to monkeypatch](https://docs.pytest.org/en/stable/how-to/monkeypatch.html)
