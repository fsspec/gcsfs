# Section Definitions

## Mocking (mock)
**Impact:** CRITICAL

Mock external boundaries only, and keep mocks faithful to real interfaces.
Covers what to mock and what not to, autospec for signature safety, and monkeypatch for environment and attribute isolation.

**Rules:**
- `mock-boundaries` - Mock external boundaries, not internal logic
- `mock-autospec` - Use autospec to keep mocks faithful to real signatures
- `mock-monkeypatch` - Use monkeypatch for env vars and attributes

## Test Structure (struct)
**Impact:** HIGH

Structure tests so failures are easy to diagnose.
Covers the Arrange-Act-Assert pattern, behavior-based naming, test granularity, and keeping logic out of test bodies.

**Rules:**
- `struct-aaa-pattern` - Structure tests as Arrange-Act-Assert
- `struct-test-naming` - Name tests after the behavior they verify
- `struct-one-behavior` - Test one behavior per test
- `struct-no-logic` - Keep conditionals and loops out of tests

## Fixtures (fixture)
**Impact:** HIGH

Share setup without hiding it.
Covers fixture scoping for isolation, conftest.py placement, and factory fixtures for varied test data.

**Rules:**
- `fixture-scope` - Use the narrowest fixture scope that works
- `fixture-conftest` - Place fixtures in the nearest conftest.py
- `fixture-factory` - Use factory fixtures for varied test data

## Parametrization (param)
**Impact:** MEDIUM

Eliminate duplicated tests without hiding cases.
Covers pytest.mark.parametrize for case tables and readable ids for failure reports.

**Rules:**
- `param-parametrize` - Replace duplicated tests with parametrize
- `param-ids` - Give parametrized cases readable ids
