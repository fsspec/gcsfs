---
name: run-pre-commit
description: >-
  Run pre-commit hooks (black, flake8, isort, trailing-whitespace, end-of-file-fixer) for gcsfs matching CI.
  Use this skill to format code, fix whitespace/import issues, and ensure all pre-commit hooks pass.
---

# Run Pre-Commit Hooks

This skill guides the agent and developers on how to run and manage `pre-commit` hooks configured in [`.pre-commit-config.yaml`](file:///usr/local/google/home/princer/code/ws1/gcsfs/.pre-commit-config.yaml).

---

## 1. Quick Execution (Helper Script)

Run all pre-commit hooks across the repository:

```bash
_agents/skills/run-pre-commit/scripts/run_pre_commit.sh
```

### Common Variations
- **Run on staged files only:**
  `_agents/skills/run-pre-commit/scripts/run_pre_commit.sh run`
- **Run on specific files:**
  `_agents/skills/run-pre-commit/scripts/run_pre_commit.sh run --files gcsfs/concurrency.py gcsfs/core.py`
- **Run a single hook:**
  `_agents/skills/run-pre-commit/scripts/run_pre_commit.sh run black --all-files`
  `_agents/skills/run-pre-commit/scripts/run_pre_commit.sh run flake8 --all-files`
  `_agents/skills/run-pre-commit/scripts/run_pre_commit.sh run isort --all-files`

---

## 2. Direct `pre-commit` Commands

If `pre-commit` is installed in your active environment:

```bash
# Run all hooks on all files
pre-commit run --all-files

# Run only on modified/staged files
pre-commit run

# Install git hook scripts so they run automatically on `git commit`
pre-commit install

# Update hook repositories to latest versions
pre-commit autoupdate
```

---

## 3. Configured Hooks Reference

The hooks configured in [`.pre-commit-config.yaml`](file:///usr/local/google/home/princer/code/ws1/gcsfs/.pre-commit-config.yaml) include:

| Hook | Purpose |
| :--- | :--- |
| **`black`** | Code formatting (target Python 3.10+) |
| **`flake8`** | PEP8 style and syntax enforcement (configured in [setup.cfg](file:///usr/local/google/home/princer/code/ws1/gcsfs/setup.cfg)) |
| **`isort`** | Import order and grouping (configured in [pyproject.toml](file:///usr/local/google/home/princer/code/ws1/gcsfs/pyproject.toml)) |
| **`trailing-whitespace`** | Strips trailing whitespace |
| **`end-of-file-fixer`** | Ensures files end with a newline |
| **`requirements-txt-fixer`** | Sorts entries in `requirements.txt` |
