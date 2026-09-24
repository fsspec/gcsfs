---
title: Use autospec to Keep Mocks Faithful to Real Signatures
impact: CRITICAL
impactDescription: Catches signature drift that plain mocks silently accept
tags: [mocking, autospec, refactoring-safety]
---

# Use autospec to Keep Mocks Faithful to Real Signatures [CRITICAL]

## Description
A plain `Mock` accepts any attribute access and any call signature, so tests keep passing after the real interface changes. Use `autospec=True` (or `spec=`) so the mock rejects calls that the real object would reject, turning silent production bugs into test failures.

## Bad Example
```python
def test_sends_welcome_email(mocker: MockerFixture) -> None:
    send = mocker.patch("app.signup.email_client.send")

    signup(email="alice@example.com")

    # Real signature changed to send(to=..., template=...) long ago;
    # this stale assertion still passes because plain Mock accepts anything
    send.assert_called_once_with("alice@example.com", "welcome")

    # Typo'd assertion methods are also silently swallowed:
    # send.assert_caled_once()  # returns a Mock, never fails
```

## Good Example
```python
def test_sends_welcome_email(mocker: MockerFixture) -> None:
    send = mocker.patch("app.signup.email_client.send", autospec=True)

    signup(email="alice@example.com")

    # Fails with TypeError if the real send() signature drifts
    send.assert_called_once_with(to="alice@example.com", template="welcome")
```

## Notes
- The `mocker` fixture comes from the pytest-mock plugin; `autospec=True` works identically with stdlib `unittest.mock.patch`
- `create_autospec(RealClass)` builds a spec'd standalone mock when you are not patching
- `spec_set=True` additionally forbids setting attributes that don't exist on the real object
- autospec inspects signatures at mock-creation time; it cannot validate return types — keep return values realistic yourself
- autospec on very large classes can be slow; scope such patches narrowly

## References
- [unittest.mock - Autospeccing](https://docs.python.org/3/library/unittest.mock.html#autospeccing)
