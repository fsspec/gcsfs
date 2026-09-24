---
title: Prefer Composition Over Inheritance
impact: MEDIUM
impactDescription: More flexible and maintainable class design
tags: [oop, composition, inheritance, flexibility]
---

# Prefer Composition Over Inheritance [MEDIUM]

## Description
Inheritance creates tight coupling between classes and can lead to fragile hierarchies. Composition (having objects contain other objects) provides more flexibility, allows combining behaviors, and makes changes easier without affecting other classes.

## Bad Example
```python
class Animal:
    def move(self) -> None:
        print("Moving")

class Bird(Animal):
    def move(self) -> None:
        print("Flying")

class Penguin(Bird):  # Problem: penguins can't fly!
    def move(self) -> None:
        print("Swimming")  # Overrides flying, but still "is-a" Bird

# Deep hierarchy becomes rigid
class Employee:
    def work(self) -> None: ...

class Manager(Employee):
    def manage(self) -> None: ...

class SeniorManager(Manager):
    def strategize(self) -> None: ...

class TechnicalSeniorManager(SeniorManager):  # Explosion of classes
    def code(self) -> None: ...
```

## Good Example
```python
from typing import Protocol

# Define behaviors as protocols
class Movable(Protocol):
    def move(self) -> None: ...

# Implement behaviors as composable components
class FlyingBehavior:
    def move(self) -> None:
        print("Flying")

class SwimmingBehavior:
    def move(self) -> None:
        print("Swimming")

class WalkingBehavior:
    def move(self) -> None:
        print("Walking")

# Compose behaviors
class Bird:
    def __init__(self, movement: Movable) -> None:
        self._movement = movement

    def move(self) -> None:
        self._movement.move()

# Usage: behavior is injected
sparrow = Bird(FlyingBehavior())
penguin = Bird(SwimmingBehavior())

# Flexible employee with roles
@dataclass
class Employee:
    name: str
    roles: list[Role]

    def can_do(self, action: str) -> bool:
        return any(role.can_do(action) for role in self.roles)

# Compose capabilities
manager = Employee("Alice", [ManagementRole(), TechnicalRole()])
```

## Notes
- Use inheritance for true "is-a" relationships with shared implementation
- Use composition for "has-a" relationships and when behaviors might change
- Prefer small, focused classes that do one thing well
- Mixins can be a middle ground but use sparingly

## References
- [Design Patterns: Favor Composition Over Inheritance](https://en.wikipedia.org/wiki/Composition_over_inheritance)
