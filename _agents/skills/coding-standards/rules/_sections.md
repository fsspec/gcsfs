# Section Definitions

## Error Handling (error)
**Impact:** CRITICAL

Prevent failures from being hidden or reported as successful outcomes.
Covers exception propagation, translation, recovery, and application-boundary handling.

**Rules:**
- `error-no-silent-exceptions` - Never swallow exceptions

## Performance Optimization (perf)
**Impact:** CRITICAL

Apply Python optimization patterns to improve processing speed and memory efficiency.
Covers list comprehensions, generators, efficient data structure selection, and other techniques that leverage Python's characteristics.

**Rules:**
- `perf-list-comprehension` - Prefer list comprehensions over loops
- `perf-generator-expression` - Use generators for large datasets
- `perf-dict-get` - Use dict.get() for efficient default values
- `perf-set-lookup` - Use set for fast lookups instead of list
- `perf-str-join` - Use join for string concatenation

## Async Processing (async)
**Impact:** HIGH

Efficient asynchronous programming patterns using asyncio.
Covers concurrent execution of I/O-bound operations, resource management, and concurrency control.

**Rules:**
- `async-gather` - Use asyncio.gather for independent tasks
- `async-create-task` - Proper background task creation
- `async-context-manager` - Resource management with async with
- `async-semaphore` - Limit concurrency with semaphores

## Design Principles (design)
**Impact:** HIGH

Software design principles for maintainability and extensibility.
Covers SOLID principles, dependency injection, pure functions, and other principles for building robust codebases.

**Rules:**
- `design-philosophy` - DRY, YAGNI, KISS principles
- `design-single-responsibility` - Single Responsibility Principle
- `design-dependency-injection` - Loose coupling with dependency injection
- `design-no-global-singleton` - Avoid global singletons and module-level shared instances
- `solid-ocp` - Open/Closed Principle
- `solid-lsp` - Liskov Substitution Principle
- `solid-isp` - Interface Segregation Principle
- `design-pure-functions` - Prefer pure functions without side effects
- `design-early-return` - Reduce nesting with early returns

## Documentation (doc)
**Impact:** HIGH

Documentation standards for public API clarity and machine-checkable contracts.
Covers docstrings, type annotations, and API documentation practices that help humans, tools, and AI agents understand code behavior.

**Rules:**
- `doc-docstring` - Document public APIs with Google style docstrings
- `doc-type-hints` - Require type hints for public APIs

## Data Validation (validation)
**Impact:** HIGH

Validation patterns for data crossing trust boundaries.
Covers schema-based validation, coercion, and clear error handling for external input.

**Rules:**
- `validation-pydantic` - Use Pydantic for boundary data validation

## Object-Oriented Programming (oop)
**Impact:** MEDIUM

Best practices for Pythonic object-oriented programming.
Covers class design, composition vs inheritance, dataclasses, and protocols.

**Rules:**
- `oop-composition-over-inheritance` - Prefer composition over inheritance
- `oop-dataclass` - Use dataclass for data containers
- `oop-protocol` - Prefer Protocol over abstract base classes
- `oop-property` - Use property instead of getters
