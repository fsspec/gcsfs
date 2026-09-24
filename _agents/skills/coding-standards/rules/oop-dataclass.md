---
title: Use Dataclass for Data Containers
impact: MEDIUM
impactDescription: Less boilerplate, better semantics
tags: [oop, dataclass, data-structures, boilerplate]
---

# Use Dataclass for Data Containers [MEDIUM]

## Description
For classes that primarily hold data, `@dataclass` automatically generates `__init__`, `__repr__`, `__eq__`, and other methods. This reduces boilerplate, prevents bugs, and makes intent clear.

## Bad Example
```python
class User:
    def __init__(self, name: str, email: str, age: int) -> None:
        self.name = name
        self.email = email
        self.age = age

    def __repr__(self) -> str:
        return f"User(name={self.name!r}, email={self.email!r}, age={self.age})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, User):
            return NotImplemented
        return self.name == other.name and self.email == other.email and self.age == other.age

    def __hash__(self) -> int:
        return hash((self.name, self.email, self.age))
```

## Good Example
```python
from dataclasses import dataclass, field

@dataclass
class User:
    name: str
    email: str
    age: int

# Mutable defaults: use default_factory, NEVER a literal
@dataclass
class Cart:
    # items: list[Item] = []        # raises ValueError at class definition time
    items: list[Item] = field(default_factory=list)  # correct: fresh list per instance

# With defaults and computed fields
@dataclass
class Order:
    items: list[Item]
    customer_id: str
    discount: float = 0.0
    # init=False keeps it out of __init__; repr=False hides the internal field
    _total: float = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self._total = sum(item.price for item in self.items) * (1 - self.discount)

# Immutable dataclass
@dataclass(frozen=True)
class Point:
    x: float
    y: float

# With slots for memory efficiency (Python 3.10+)
@dataclass(slots=True)
class Coordinate:
    lat: float
    lon: float
```

## Notes
- A mutable literal default (`items: list = []`) raises `ValueError: mutable default ... is not allowed` at class-definition time. Always use `field(default_factory=list)`—this also avoids the shared-mutable-default bug that plagues plain functions.
- Use `frozen=True` for immutable objects: it makes instances read-only and auto-generates `__hash__`, so they work as dict keys and set members without writing `__hash__` by hand.
- Use `slots=True` (passing `slots=True` to `@dataclass` is Python 3.10+) for memory efficiency with many instances
- Use `Decimal`, not `float`, for money fields like `discount`/`price` to avoid rounding errors
- Use `@dataclass` for internal data structures; use Pydantic at trust boundaries (see `validation-pydantic`)
- Consider `attrs` for more features beyond what dataclasses provide

## References
- [Python Docs - dataclasses](https://docs.python.org/3/library/dataclasses.html)
- [PEP 557 - Data Classes](https://peps.python.org/pep-0557/)
