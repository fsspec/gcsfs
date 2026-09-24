# Python Coding Standards

A comprehensive collection of Python coding standards and best practices, designed for AI agents and LLMs to generate high-quality Python code.

## Overview

This skill provides 26 rules across 7 categories:

| Category | Prefix | Impact | Rules |
|----------|--------|--------|-------|
| Error Handling | `error-` | CRITICAL | 1 |
| Performance Optimization | `perf-` | CRITICAL | 5 |
| Async Processing | `async-` | HIGH | 4 |
| Design Principles | `design-`, `solid-` | HIGH | 9 |
| Documentation | `doc-` | HIGH | 2 |
| Data Validation | `validation-` | HIGH | 1 |
| Object-Oriented Programming | `oop-` | MEDIUM | 4 |

## Structure

```
skills/coding-standards/
├── SKILL.md              # Skill overview with all rule summaries
├── metadata.json         # Metadata (version, description)
├── README.md             # This file
└── rules/
    ├── _sections.md      # Section definitions
    ├── _template.md      # Rule template
    ├── error-*.md        # Error handling rules
    ├── perf-*.md         # Performance rules
    ├── async-*.md        # Async processing rules
    ├── design-*.md       # Design principles rules
    ├── solid-*.md        # SOLID principles rules
    ├── doc-*.md          # Documentation rules
    ├── validation-*.md   # Data validation rules
    └── oop-*.md          # OOP rules
```

## Rules

### Error Handling (CRITICAL)
- `error-no-silent-exceptions` - Never swallow exceptions

### Performance Optimization (CRITICAL)
- `perf-list-comprehension` - Prefer list comprehensions over loops
- `perf-generator-expression` - Use generators for large datasets
- `perf-dict-get` - Use dict.get() for efficient default values
- `perf-set-lookup` - Use set for fast lookups
- `perf-str-join` - Use join for string concatenation

### Async Processing (HIGH)
- `async-gather` - Use asyncio.gather for independent tasks
- `async-create-task` - Proper background task creation
- `async-context-manager` - Resource management with async with
- `async-semaphore` - Limit concurrency with semaphores

### Design Principles (HIGH)
- `design-philosophy` - DRY, YAGNI, KISS principles
- `design-single-responsibility` - Single Responsibility Principle
- `design-dependency-injection` - Loose coupling with dependency injection
- `design-no-global-singleton` - Avoid global singletons and module-level shared instances
- `solid-ocp` - Open/Closed Principle
- `solid-lsp` - Liskov Substitution Principle
- `solid-isp` - Interface Segregation Principle
- `design-pure-functions` - Prefer pure functions without side effects
- `design-early-return` - Reduce nesting with early returns

### Documentation (HIGH)
- `doc-docstring` - Document public APIs with Google style docstrings
- `doc-type-hints` - Require type hints for public APIs

### Data Validation (HIGH)
- `validation-pydantic` - Use Pydantic for boundary data validation

### Object-Oriented Programming (MEDIUM)
- `oop-composition-over-inheritance` - Prefer composition over inheritance
- `oop-dataclass` - Use dataclass for data containers
- `oop-protocol` - Prefer Protocol over abstract base classes
- `oop-property` - Use property instead of getters

## Usage

This skill is automatically applied when working with Python files (`**/*.py`) and Python configuration files (`pyproject.toml`, `setup.py`, `requirements*.txt`).

## Rule Format

Each rule follows a consistent format:
- **Frontmatter**: title, impact level, tags
- **Description**: Why this rule matters
- **Bad Example**: Code to avoid
- **Good Example**: Recommended approach
- **Notes**: Additional tips and edge cases
- **References**: Documentation links

## Contributing

To add a new rule:
1. Copy `rules/_template.md`
2. Use the appropriate prefix (`error-`, `perf-`, `async-`, `design-`, `solid-`, `doc-`, `validation-`, `oop-`)
3. Follow the existing format
4. Update `_sections.md` and `SKILL.md`
