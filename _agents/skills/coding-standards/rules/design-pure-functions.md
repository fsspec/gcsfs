---
title: Prefer Pure Functions Without Side Effects
impact: HIGH
impactDescription: Predictable, testable, and parallelizable code
tags: [design, functional, pure-functions, testing]
---

# Prefer Pure Functions Without Side Effects [HIGH]

## Description
Pure functions always return the same output for the same input and have no side effects (no mutation of external state, no I/O). They are easier to test, reason about, and can be safely parallelized or cached.

## Bad Example
```python
# Impure: modifies external state
total_processed = 0

def process_item(item: Item) -> Result:
    global total_processed
    total_processed += 1  # Side effect: modifies global state

    item.status = "processed"  # Side effect: mutates input
    log_to_file(item)  # Side effect: I/O

    return calculate_result(item)

# Impure: depends on external state
def get_discount(price: float) -> float:
    if datetime.now().weekday() == 5:  # Non-deterministic
        return price * 0.1
    return 0
```

## Good Example
```python
from dataclasses import dataclass
from datetime import date

# Pure: same input always produces same output
def calculate_result(item: Item) -> Result:
    return Result(
        value=item.quantity * item.price,
        category=categorize(item.type),
    )

# Pure: explicit dependencies, no hidden state
def get_discount(price: float, current_date: date) -> float:
    if current_date.weekday() == 5:
        return price * 0.1
    return 0

# Pure: returns new object instead of mutating
@dataclass(frozen=True)
class Item:
    status: str
    quantity: int

def mark_processed(item: Item) -> Item:
    return Item(status="processed", quantity=item.quantity)

# Separate pure logic from impure orchestration
def process_items(items: list[Item], current_date: date) -> ProcessingResult:
    # Pure transformations
    results = [calculate_result(item) for item in items]
    discounts = [get_discount(r.value, current_date) for r in results]
    return ProcessingResult(results=results, discounts=discounts)

# Impure shell handles I/O
def main() -> None:
    items = load_from_db()  # Impure: I/O
    result = process_items(items, date.today())  # Pure: computation
    save_to_db(result)  # Impure: I/O
```

## Notes
- "Functional core, imperative shell" pattern: pure logic wrapped by impure I/O handlers
- Use `frozen=True` in dataclasses to prevent mutation
- Pass time, random seeds, and other non-deterministic values as parameters
- Pure functions are trivially testable: just assert output equals expected

## References
- [Functional Core, Imperative Shell](https://www.destroyallsoftware.com/screencasts/catalog/functional-core-imperative-shell)
