---
name: testing
description: Python test-writing best practices covering test structure, fixtures, parametrization, and mocking with pytest. Use when writing, reviewing, or refactoring tests, adding test coverage, or working in tests directories and conftest.py.
paths:
  - "tests/**"
  - "**/test_*.py"
  - "**/*_test.py"
  - "**/conftest.py"
---

# Python Testing

A collection of test-writing best practices for pytest. Designed for AI agents and LLMs to write readable, maintainable, and trustworthy tests.

## Categories

### Mocking [CRITICAL]
Mock external boundaries only, and keep mocks faithful to real interfaces.

| Rule | Description |
|------|-------------|
| [mock-boundaries](rules/mock-boundaries.md) | Mock external boundaries, not internal logic |
| [mock-autospec](rules/mock-autospec.md) | Use autospec to keep mocks faithful to real signatures |
| [mock-monkeypatch](rules/mock-monkeypatch.md) | Use monkeypatch for env vars and attributes |

### Test Structure [HIGH]
Structure tests so failures are easy to diagnose.

| Rule | Description |
|------|-------------|
| [struct-aaa-pattern](rules/struct-aaa-pattern.md) | Structure tests as Arrange-Act-Assert |
| [struct-test-naming](rules/struct-test-naming.md) | Name tests after the behavior they verify |
| [struct-one-behavior](rules/struct-one-behavior.md) | Test one behavior per test |
| [struct-no-logic](rules/struct-no-logic.md) | Keep conditionals and loops out of tests |

### Fixtures [HIGH]
Share setup without hiding it.

| Rule | Description |
|------|-------------|
| [fixture-scope](rules/fixture-scope.md) | Use the narrowest fixture scope that works |
| [fixture-conftest](rules/fixture-conftest.md) | Place fixtures in the nearest conftest.py |
| [fixture-factory](rules/fixture-factory.md) | Use factory fixtures for varied test data |

### Parametrization [MEDIUM]
Eliminate duplicated tests without hiding cases.

| Rule | Description |
|------|-------------|
| [param-parametrize](rules/param-parametrize.md) | Replace duplicated tests with parametrize |
| [param-ids](rules/param-ids.md) | Give parametrized cases readable ids |

## Quick Reference

### Structure Patterns
```python
def test_expired_token_is_rejected() -> None:
    # Arrange
    token = make_token(expires_at=YESTERDAY)

    # Act
    result = validate(token)

    # Assert
    assert result.valid is False
```

### Fixture Patterns
```python
# Narrow scope by default
@pytest.fixture
def user() -> User:
    return User(name="alice", email="alice@example.com")

# Factory fixture for varied data
@pytest.fixture
def make_user() -> Callable[..., User]:
    def _make(name: str = "alice", active: bool = True) -> User:
        return User(name=name, email=f"{name}@example.com", active=active)
    return _make
```

### Parametrize Patterns
```python
@pytest.mark.parametrize(
    ("email", "valid"),
    [
        pytest.param("a@example.com", True, id="simple-address"),
        pytest.param("a@sub.example.com", True, id="subdomain"),
        pytest.param("no-at-sign", False, id="missing-at"),
        pytest.param("", False, id="empty-string"),
    ],
)
def test_email_validation(email: str, valid: bool) -> None:
    assert is_valid_email(email) is valid
```

### Mocking Patterns
```python
# Mock the external boundary with a faithful interface.
# `mocker` requires the pytest-mock plugin; with stdlib only,
# use unittest.mock.patch as a decorator or context manager.
def test_notifies_on_signup(mocker: MockerFixture) -> None:
    send = mocker.patch("app.signup.email_client.send", autospec=True)

    signup(email="alice@example.com")

    send.assert_called_once_with(to="alice@example.com", template="welcome")

# monkeypatch for environment variables
def test_reads_api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("API_KEY", "test-key")
    assert load_config().api_key == "test-key"
```

## See Also

- [tooling / test-pytest](../tooling/rules/test-pytest.md) - pytest configuration, fixtures setup, and coverage
