---
title: Use Set for Fast Lookups
impact: CRITICAL
impactDescription: O(n) → O(1) lookup performance
tags: [performance, data-structures, set, lookup]
---

# Use Set for Fast Lookups [CRITICAL]

## Description
The `in` operator on lists performs O(n) linear search, while sets use O(1) hash-based lookup. For repeated membership checks, converting to a set provides significant speedup even accounting for conversion cost.

## Bad Example
```python
# Slow: repeated list search is O(n * m)
allowed_users: list[str] = ["alice", "bob", "charlie", ...]  # large list

def is_allowed(user: str) -> bool:
    return user in allowed_users  # O(n) search every time

# Filtering is also slow
valid_ids: list[int] = [1, 2, 3, 4, 5, ...]  # large list
result = [item for item in items if item.id in valid_ids]  # O(n * m)
```

## Good Example
```python
# Fast: set lookup is O(1)
allowed_users: set[str] = {"alice", "bob", "charlie", ...}

def is_allowed(user: str) -> bool:
    return user in allowed_users  # O(1) lookup

# Filtering is fast
valid_ids: set[int] = {1, 2, 3, 4, 5, ...}
result = [item for item in items if item.id in valid_ids]  # O(n)

# Converting from list
user_list: list[str] = get_users_from_db()
user_set: set[str] = set(user_list)  # O(n) conversion, then O(1) lookups
```

## Notes
- Set elements must be hashable (immutable)
- If order matters, use `dict.fromkeys()` or `dict` in Python 3.7+
- `frozenset` is immutable and can be used as dict keys or set elements
- Sets also deduplicate: `unique = list(set(items))`

## References
- [Python Docs - Set Types](https://docs.python.org/3/library/stdtypes.html#set)
