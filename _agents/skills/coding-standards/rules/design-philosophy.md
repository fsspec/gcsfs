---
title: Software Design Philosophy
impact: HIGH
impactDescription: Maintainable, pragmatic code
tags: [design, dry, yagni, kiss, philosophy]
---

# Software Design Philosophy [HIGH]

## Description
Core principles that guide good software design. These principles help avoid over-engineering, reduce maintenance burden, and keep code understandable.

## DRY - Don't Repeat Yourself

Avoid duplicating knowledge or logic. When the same concept exists in multiple places, changes require updating all instances.

```python
# Bad: duplicated validation
def create_user(email: str) -> User:
    if not email or "@" not in email:
        raise ValueError("Invalid email")
    return User(email=email)

def update_email(user: User, email: str) -> None:
    if not email or "@" not in email:  # Duplicated!
        raise ValueError("Invalid email")
    user.email = email

# Good: extracted function
def validate_email(email: str) -> str:
    if not email or "@" not in email:
        raise ValueError("Invalid email")
    return email

def create_user(email: str) -> User:
    return User(email=validate_email(email))

def update_email(user: User, email: str) -> None:
    user.email = validate_email(email)
```

**Note:** DRY applies to knowledge, not just code. Two similar-looking snippets may represent different concepts and shouldn't be merged.

## YAGNI - You Aren't Gonna Need It

Don't build features until they're actually needed. Speculative functionality adds complexity, maintenance burden, and often turns out unnecessary.

```python
# Bad: speculative generalization
class DataProcessor:
    def __init__(
        self,
        format: str = "json",
        compression: str | None = None,  # Not needed yet
        encryption: str | None = None,   # Not needed yet
        retry_count: int = 3,            # Not needed yet
        cache_enabled: bool = False,     # Not needed yet
    ) -> None:
        ...

# Good: only what's needed now
class DataProcessor:
    def __init__(self, format: str = "json") -> None:
        self.format = format
```

**Note:** This doesn't mean ignore architecture—it means don't implement features before requirements exist.

## KISS - Keep It Simple, Stupid

Prefer simple solutions over clever ones. Simple code is easier to read, debug, and modify.

```python
# Bad: over-engineered
def get_user_names(users: list[User]) -> list[str]:
    return list(
        map(
            lambda u: u.name,
            filter(lambda u: u.active, users)
        )
    )

# Good: simple and readable
def get_user_names(users: list[User]) -> list[str]:
    return [u.name for u in users if u.active]
```

```python
# Bad: unnecessary abstraction for one-time use
class StringReverser:
    def reverse(self, s: str) -> str:
        return s[::-1]

reverser = StringReverser()
result = reverser.reverse("hello")

# Good: just do it
result = "hello"[::-1]
```

## How They Work Together

| Situation | Principle |
|-----------|-----------|
| Same logic in 3+ places | DRY - extract it |
| "We might need this later" | YAGNI - don't build it |
| Two similar solutions | KISS - pick the simpler one |
| Premature abstraction | YAGNI + KISS - wait for real need |

## Notes
- These principles can conflict—use judgment
- DRY without need leads to premature abstraction (violates YAGNI)
- Over-simplification can lead to duplication (violates DRY)
- When in doubt, start simple and refactor when patterns emerge

## References
- [The Pragmatic Programmer](https://pragprog.com/titles/tpp20/the-pragmatic-programmer-20th-anniversary-edition/)
- [Martin Fowler - YAGNI](https://martinfowler.com/bliki/Yagni.html)
