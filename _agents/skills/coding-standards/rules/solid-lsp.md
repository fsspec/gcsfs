---
title: Liskov Substitution Principle
impact: HIGH
impactDescription: Subtypes honor the contract of their abstractions
tags: [design, solid, lsp, inheritance, protocol]
---

# Liskov Substitution Principle [HIGH]

## Description
Objects of a subtype must be substitutable for objects of the base type without breaking program correctness. If code works with an abstraction, every concrete implementation must honor the same behavioral contract—same preconditions, same postconditions, and no surprising exceptions. Violations often appear as subclasses that weaken guarantees or strengthen requirements.

## Bad Example
```python
class Bird:
    def fly(self) -> None:
        ...

class Penguin(Bird):
    def fly(self) -> None:
        # Violates the Bird contract—callers expect all Birds to fly
        raise NotImplementedError("Penguins cannot fly")

def migrate(birds: list[Bird]) -> None:
    for bird in birds:
        bird.fly()  # Breaks when a Penguin is in the list

migrate([Sparrow(), Penguin()])  # Runtime error
```

## Good Example
```python
from typing import Protocol

class Bird(Protocol):
    def move(self) -> None: ...

class FlyingBird:
    def move(self) -> None:
        self._fly()

    def _fly(self) -> None: ...

class Penguin:
    def move(self) -> None:
        self._swim()

    def _swim(self) -> None: ...

def relocate(birds: list[Bird]) -> None:
    for bird in birds:
        bird.move()  # Works for any Bird implementation

relocate([FlyingBird(), Penguin()])
```

## Notes
- Subclasses must not raise exceptions that the base contract does not allow
- Tightening input validation (e.g., rejecting values the parent accepts) is an LSP violation
- Prefer composition and role-specific `Protocol`s over inheritance when subtypes diverge in behavior
- `@runtime_checkable` `isinstance()` checks do not verify behavioral contracts—design interfaces around what callers actually need
- The classic Rectangle/Square problem is another LSP violation: a Square subclass that rejects independent width/height changes breaks Rectangle callers

## References
- [SOLID - Liskov Substitution Principle](https://en.wikipedia.org/wiki/Liskov_substitution_principle)
- [Liskov Substitution Principle (Robert C. Martin)](https://objectmentor.com/resources/articles/lsp.pdf)