---
title: Prefer Protocol Over Abstract Base Classes
impact: MEDIUM
impactDescription: Structural typing without inheritance requirements
tags: [oop, protocol, typing, duck-typing]
---

# Prefer Protocol Over Abstract Base Classes [MEDIUM]

## Description
`Protocol` enables structural subtyping (duck typing with static type checking). Unlike abstract base classes (ABC), classes don't need to explicitly inherit from a Protocol—they just need to implement the required methods. This provides flexibility while maintaining type safety.

## Bad Example
```python
from abc import ABC, abstractmethod

# Forces explicit inheritance
class Serializable(ABC):
    @abstractmethod
    def to_json(self) -> str: ...

# Every class must explicitly inherit
class User(Serializable):  # Must inherit
    def __init__(self, name: str) -> None:
        self.name = name

    def to_json(self) -> str:
        return f'{{"name": "{self.name}"}}'

# Can't use third-party classes that happen to have to_json()
def save(obj: Serializable) -> None:  # Only accepts Serializable subclasses
    data = obj.to_json()
    # ...
```

## Good Example
```python
from typing import Protocol

# No inheritance required
class Serializable(Protocol):
    def to_json(self) -> str: ...

# Works without inheriting from Serializable
class User:
    def __init__(self, name: str) -> None:
        self.name = name

    def to_json(self) -> str:
        return f'{{"name": "{self.name}"}}'

# Any object with to_json() works
def save(obj: Serializable) -> None:
    data = obj.to_json()
    # ...

# Third-party classes work if they have the method
save(User("Alice"))  # Works
save(some_third_party_object_with_to_json)  # Also works!

# Protocols can have multiple methods
class Repository(Protocol):
    def get(self, id: str) -> Entity: ...
    def save(self, entity: Entity) -> None: ...
    def delete(self, id: str) -> None: ...

# Runtime checking with @runtime_checkable
from typing import runtime_checkable

@runtime_checkable
class Closable(Protocol):
    def close(self) -> None: ...

if isinstance(resource, Closable):
    resource.close()
```

## When ABC Is the Right Choice
Protocol is not a universal replacement for ABC. Reach for an ABC when you want to
**share concrete implementation** with subclasses or **force** them to override specific
methods (the Template Method pattern):

```python
from abc import ABC, abstractmethod

class Exporter(ABC):
    # Shared implementation all subclasses inherit
    def export(self, rows: list[dict]) -> str:
        header = self._format_header(rows)
        body = "\n".join(self._format_row(r) for r in rows)
        return f"{header}\n{body}"

    # Subclasses MUST provide these; instantiating without them is a TypeError
    @abstractmethod
    def _format_header(self, rows: list[dict]) -> str: ...

    @abstractmethod
    def _format_row(self, row: dict) -> str: ...

class CsvExporter(Exporter):
    def _format_header(self, rows: list[dict]) -> str:
        return ",".join(rows[0].keys())

    def _format_row(self, row: dict) -> str:
        return ",".join(map(str, row.values()))
```

Rule of thumb: **Protocol** for "any type with this shape qualifies" (no inherited code);
**ABC** for "share this base behavior and require these overrides".

## Notes
- Protocols work at type-check time; use `@runtime_checkable` for `isinstance()` checks
- `@runtime_checkable` `isinstance()` only verifies that the **methods exist by name**—it does not check signatures or return types, so it can report a false match. Use it as a coarse guard, not a guarantee.
- Prefer Protocol for interfaces that external code might implement
- Use ABC when you need shared implementation or want to enforce inheritance (see "When ABC Is the Right Choice")
- Protocol supports properties, class methods, and attributes

## References
- [Python Docs - typing.Protocol](https://docs.python.org/3/library/typing.html#typing.Protocol)
- [PEP 544 - Protocols: Structural subtyping](https://peps.python.org/pep-0544/)
