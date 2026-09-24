# Python Testing

Test-writing best practices for pytest, designed for AI agents and LLMs to write readable, maintainable, and trustworthy tests.

## Overview

This skill provides 12 rules across 4 categories:

| Category | Prefix | Impact | Rules |
|----------|--------|--------|-------|
| Mocking | `mock-` | CRITICAL | 3 |
| Test Structure | `struct-` | HIGH | 4 |
| Fixtures | `fixture-` | HIGH | 3 |
| Parametrization | `param-` | MEDIUM | 2 |

## Structure

```
skills/testing/
├── SKILL.md                  # Skill overview with quick reference
├── metadata.json             # Metadata (version, description)
├── README.md                 # This file
└── rules/
    ├── _sections.md          # Section definitions
    ├── _template.md          # Rule template
    ├── mock-boundaries.md    # Mock external boundaries only
    ├── mock-autospec.md      # Faithful mock signatures
    ├── mock-monkeypatch.md   # Env vars and attributes
    ├── struct-aaa-pattern.md # Arrange-Act-Assert
    ├── struct-test-naming.md # Behavior-based names
    ├── struct-one-behavior.md# One behavior per test
    ├── struct-no-logic.md    # No conditionals/loops in tests
    ├── fixture-scope.md      # Narrowest scope that works
    ├── fixture-conftest.md   # conftest.py placement
    ├── fixture-factory.md    # Factory fixtures
    ├── param-parametrize.md  # Case tables with parametrize
    └── param-ids.md          # Readable case ids
```

## Rules

### Mocking (CRITICAL)
- `mock-boundaries` - Mock external boundaries, not internal logic
- `mock-autospec` - Use autospec to keep mocks faithful to real signatures
- `mock-monkeypatch` - Use monkeypatch for env vars and attributes

### Test Structure (HIGH)
- `struct-aaa-pattern` - Structure tests as Arrange-Act-Assert
- `struct-test-naming` - Name tests after the behavior they verify
- `struct-one-behavior` - Test one behavior per test
- `struct-no-logic` - Keep conditionals and loops out of tests

### Fixtures (HIGH)
- `fixture-scope` - Use the narrowest fixture scope that works
- `fixture-conftest` - Place fixtures in the nearest conftest.py
- `fixture-factory` - Use factory fixtures for varied test data

### Parametrization (MEDIUM)
- `param-parametrize` - Replace duplicated tests with parametrize
- `param-ids` - Give parametrized cases readable ids

## Related

- [tooling / test-pytest](../tooling/rules/test-pytest.md) - pytest configuration, plugins, and coverage setup

## Usage

This skill is automatically applied when working with test files (`tests/**`, `test_*.py`, `*_test.py`, `conftest.py`).
