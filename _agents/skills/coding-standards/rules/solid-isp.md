---
title: Interface Segregation Principle
impact: HIGH
impactDescription: Small, focused interfaces clients actually use
tags: [design, solid, isp, protocol, typing]
---

# Interface Segregation Principle [HIGH]

## Description
Clients should not be forced to depend on methods they do not use. Prefer several small, purpose-specific `Protocol` definitions over one large interface that bundles unrelated capabilities. This reduces coupling, simplifies testing, and aligns with Python's structural typing—implementations only need the methods they actually provide.

## Bad Example
```python
from typing import Protocol

class Worker(Protocol):
    def work(self) -> None: ...
    def eat(self) -> None: ...
    def sleep(self) -> None: ...

class Robot:
    def work(self) -> None:
        ...

    def eat(self) -> None:
        # Robots don't eat—empty stub to satisfy Worker
        raise NotImplementedError

    def sleep(self) -> None:
        raise NotImplementedError

def assign_task(worker: Worker) -> None:
    worker.work()
```

## Good Example
```python
from typing import Protocol

class Workable(Protocol):
    def work(self) -> None: ...

class Feedable(Protocol):
    def eat(self) -> None: ...

class Robot:
    def work(self) -> None:
        ...

class Human:
    def work(self) -> None:
        ...

    def eat(self) -> None:
        ...

def assign_task(worker: Workable) -> None:
    worker.work()  # Depends only on what it needs

def lunch_break(worker: Feedable) -> None:
    worker.eat()

assign_task(Robot())
assign_task(Human())
lunch_break(Human())
```

## Notes
- Split fat ABCs or `Protocol`s when implementations leave methods as stubs or `raise NotImplementedError`
- ISP complements `oop-protocol`: structural typing makes small interfaces cheap—no inheritance ceremony required
- Name protocols by capability (`Readable`, `Writable`) rather than by entity (`Database`) when roles differ
- A class may implement multiple small protocols; callers depend only on the one they need
- Do not over-segregate: split when clients genuinely use different subsets, not preemptively for every method

## References
- [SOLID - Interface Segregation Principle](https://en.wikipedia.org/wiki/Interface_segregation_principle)
- [PEP 544 - Protocols: Structural subtyping](https://peps.python.org/pep-0544/)