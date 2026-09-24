---
title: Require Type Hints for Public APIs
impact: HIGH
impactDescription: Explicit contracts that improve correctness, tooling, and agent reliability
tags: [documentation, typing, type-hints, public-api]
---

# Require Type Hints for Public APIs [HIGH]

## Description
Public API parameters and return values should have precise type annotations. Type hints make contracts machine-checkable, improve editor and agent understanding, and catch integration errors before runtime.

## Bad Example
```python
def find_user(user_id, include_disabled=False):
    row = database.fetch_user(user_id, include_disabled)
    if row is None:
        return None
    return User(row["id"], row["email"], row["enabled"])


class EmailSender:
    def send(self, recipients, subject, body):
        for recipient in recipients:
            smtp.send_message(recipient, subject, body)
```

## Good Example
```python
from collections.abc import Sequence


def find_user(user_id: UserId, include_disabled: bool = False) -> User | None:
    row = database.fetch_user(user_id, include_disabled)
    if row is None:
        return None
    return User(row["id"], row["email"], row["enabled"])


class EmailSender:
    def send(
        self,
        recipients: Sequence[EmailAddress],
        subject: str,
        body: str,
    ) -> None:
        for recipient in recipients:
            smtp.send_message(recipient, subject, body)
```

## Notes
- Annotate all public function and method parameters, including return types.
- Use `None` return annotations for functions called only for side effects.
- Prefer abstract collection types such as `Sequence`, `Mapping`, and `Iterable` for inputs when callers do not need a concrete container.
- Use domain-specific aliases or value objects when they make contracts clearer, such as `UserId` or `EmailAddress`.
- Avoid `Any` in public APIs unless the function truly accepts or returns arbitrary values.

## References
- [Python Docs - typing](https://docs.python.org/3/library/typing.html)
- [PEP 484 - Type Hints](https://peps.python.org/pep-0484/)
