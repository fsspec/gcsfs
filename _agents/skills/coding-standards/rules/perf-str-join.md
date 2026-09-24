---
title: Use join for String Concatenation
impact: CRITICAL
impactDescription: O(n²) → O(n) string building
tags: [performance, string, join]
---

# Use join for String Concatenation [CRITICAL]

## Description
String concatenation with `+` creates a new string object each time, resulting in O(n²) complexity. The `str.join()` method pre-calculates the final size and allocates memory once, achieving O(n) efficiency.

## Bad Example
```python
# Slow: creates new string object each iteration
def build_csv_line(values: list[str]) -> str:
    result = ""
    for value in values:
        result += value + ","  # O(n²)
    return result[:-1]

# String concatenation in loop
log_message = ""
for event in events:
    log_message += f"[{event.time}] {event.message}\n"
```

## Good Example
```python
# Fast: join concatenates at once
def build_csv_line(values: list[str]) -> str:
    return ",".join(values)

# Combine with generator expression
log_message = "\n".join(
    f"[{event.time}] {event.message}" for event in events
)

# Filtering with conditions
result = ", ".join(name for name in names if name)
```

## Notes
- f-strings are fine for small concatenations (2-3 items) and equally performant
- `io.StringIO` is an alternative but `join()` is usually sufficient
- For bytes, use `b"".join()`
- For paths, use `pathlib.Path` with `/` operator: `Path("dir") / "file.txt"`

## References
- [Python Docs - str.join()](https://docs.python.org/3/library/stdtypes.html#str.join)
