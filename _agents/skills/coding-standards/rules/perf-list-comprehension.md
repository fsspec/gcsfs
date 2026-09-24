---
title: Prefer List Comprehensions Over Loops
impact: CRITICAL
impactDescription: 1.5-2x faster execution
tags: [performance, loops, comprehension]
---

# Prefer List Comprehensions Over Loops [CRITICAL]

## Description
Instead of building lists with for loops, use list comprehensions for more concise code and better performance. Comprehensions are optimized at the C level and avoid repeated `append` method calls.

## Bad Example
```python
# Slow: repeated append calls
result: list[int] = []
for i in range(1000):
    result.append(i * 2)

# Verbose filtering
filtered: list[str] = []
for item in items:
    if item.startswith("prefix_"):
        filtered.append(item.upper())
```

## Good Example
```python
# Fast: list comprehension
result: list[int] = [i * 2 for i in range(1000)]

# Comprehension with filtering
filtered: list[str] = [
    item.upper() for item in items if item.startswith("prefix_")
]
```

## Notes
- If a comprehension exceeds 3 lines, consider using a regular loop for readability
- Use regular loops when side effects are needed (print, file writes, etc.)
- Dict comprehensions `{k: v for k, v in items}` and set comprehensions `{x for x in items}` follow the same pattern

## References
- [Python Docs - List Comprehensions](https://docs.python.org/3/tutorial/datastructures.html#list-comprehensions)
