---
title: Use dict.get() for Efficient Default Values
impact: CRITICAL
impactDescription: Avoid KeyError + cleaner code
tags: [performance, dict, defensive-coding]
---

# Use dict.get() for Efficient Default Values [CRITICAL]

## Description
When retrieving keys from dictionaries, `dict.get(key, default)` provides a concise way to specify default values when keys don't exist. More efficient and readable than `try/except` or `if key in dict`.

## Bad Example
```python
# Verbose: existence check + retrieval
config: dict[str, int] = {"timeout": 30}

if "retries" in config:
    retries = config["retries"]
else:
    retries = 3

# Exception handling is expensive
try:
    retries = config["retries"]
except KeyError:
    retries = 3
```

## Good Example
```python
config: dict[str, int] = {"timeout": 30}

# Simple: get() with default value
retries = config.get("retries", 3)

# setdefault() sets value if missing and returns it
cache: dict[str, list[int]] = {}
cache.setdefault("results", []).append(42)
```

## Notes
- Omitting the second argument to `get()` returns `None`
- Chain for nested dicts: `dict.get("key1", {}).get("key2", default)`
- Use `setdefault()` when the default is a mutable object (like list)
- Python 3.8+ allows combining with `:=` (walrus operator)

## References
- [Python Docs - dict.get()](https://docs.python.org/3/library/stdtypes.html#dict.get)
