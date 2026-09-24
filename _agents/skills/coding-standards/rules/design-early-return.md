---
title: Reduce Nesting with Early Returns
impact: HIGH
impactDescription: Improved readability and reduced cognitive load
tags: [design, readability, control-flow, guard-clause]
---

# Reduce Nesting with Early Returns [HIGH]

## Description
Deeply nested code increases cognitive load and makes logic harder to follow. Using early returns (guard clauses) handles edge cases upfront, keeping the main logic flat and readable.

## Bad Example
```python
def process_order(order: Order | None) -> Result:
    if order is not None:
        if order.is_valid:
            if order.items:
                if order.customer.is_active:
                    # Main logic buried deep
                    total = sum(item.price for item in order.items)
                    discount = calculate_discount(order.customer)
                    return Result(total=total - discount, status="success")
                else:
                    return Result(total=0, status="inactive_customer")
            else:
                return Result(total=0, status="no_items")
        else:
            return Result(total=0, status="invalid_order")
    else:
        return Result(total=0, status="no_order")
```

## Good Example
```python
def process_order(order: Order | None) -> Result:
    # Guard clauses handle edge cases first
    if order is None:
        return Result(total=0, status="no_order")

    if not order.is_valid:
        return Result(total=0, status="invalid_order")

    if not order.items:
        return Result(total=0, status="no_items")

    if not order.customer.is_active:
        return Result(total=0, status="inactive_customer")

    # Main logic is flat and clear
    total = sum(item.price for item in order.items)
    discount = calculate_discount(order.customer)
    return Result(total=total - discount, status="success")

# Also applies to loops with continue
def process_items(items: list[Item]) -> list[Result]:
    results: list[Result] = []
    for item in items:
        if item.is_deleted:
            continue  # Skip early

        if not item.is_valid:
            continue  # Skip early

        # Main processing logic
        results.append(process_valid_item(item))

    return results
```

## Notes
- Each guard clause should handle one specific condition
- Order guards from most likely to least likely for minor performance gains
- Use `continue` in loops for the same flat structure
- Consider extracting complex guard conditions into well-named functions

## References
- [Refactoring: Replace Nested Conditional with Guard Clauses](https://refactoring.com/catalog/replaceNestedConditionalWithGuardClauses.html)
