---
title: Recommended Tooling
impact: HIGH
impactDescription: Automated quality enforcement
tags: [tooling, ruff, mypy, pytest, pyscn, uv]
---

# Recommended Tooling [HIGH]

## Description
To write high-quality Python code, we recommend adopting these tools. They automate quality checks, catch errors early, and maintain consistency across the codebase.

## Recommended Tools

| Tool | Purpose | Why |
|------|---------|-----|
| [pyscn](https://github.com/ludo-technologies/pyscn) | Code Analysis | Detect dead code, duplicates, circular dependencies |
| [ruff](https://docs.astral.sh/ruff/) | Linting & Formatting | Fast static analysis, catch bugs and style issues |
| [mypy](https://mypy.readthedocs.io/) | Type Checking | Find errors before runtime, improve IDE support |
| [pytest](https://docs.pytest.org/) | Testing | Build confidence with reliable tests |
| [uv](https://docs.astral.sh/uv/) | Package Management | Fast dependency management |

## Minimal Setup

```toml
# pyproject.toml
[project]
name = "myproject"
version = "0.1.0"
requires-python = ">=3.11"

[project.optional-dependencies]
dev = ["ruff", "mypy", "pytest", "pytest-cov", "pyscn"]

[tool.ruff]
target-version = "py311"
line-length = 88

[tool.ruff.lint]
select = ["E", "F", "W", "I", "UP", "B", "BLE", "SIM", "PTH", "S110", "S112"]

[tool.ruff.lint.flake8-bandit]
check-typed-exception = true

[tool.mypy]
python_version = "3.11"
strict = true

[tool.pytest.ini_options]
testpaths = ["tests"]
addopts = ["-v", "--tb=short"]

[tool.pyscn]
max_complexity = 15
```

## Quick Commands

```bash
# Install dev dependencies
uv pip install -e ".[dev]"

# Run all checks
ruff check . --fix      # Lint and auto-fix
ruff format .           # Format code
mypy .                  # Type check
pytest                  # Run tests
pyscn check .           # Quality gate
```

## Notes
- Integrate these into CI/CD to catch issues before merge
- See the [tooling skill](../tooling/SKILL.md) for detailed configuration
- Start with ruff and mypy, add others as needed

## References
- [Python Tooling Skill](../tooling/SKILL.md)
