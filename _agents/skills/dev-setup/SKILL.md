---
name: dev-setup
description: >-
  Set up the local development environment for gcsfs.
  Creates a virtual environment, installs gcsfs in editable mode with development dependencies, and registers git pre-commit hooks.
  Use this skill when onboarding a new environment or setting up tools.
---

# Development Environment Setup

This skill guides the agent and developers on how to bootstrap a complete development environment for `gcsfs`.

---

## 1. Quick Automated Setup

Run the helper script to create `.venv`, install all dev dependencies, and configure git hooks:

```bash
_agents/skills/dev-setup/scripts/setup_dev.sh
```

After completion, activate the virtual environment:
```bash
source .venv/bin/activate
```

---

## 2. Manual Setup Steps

### Option A: Standard `venv`
```bash
# 1. Create virtual environment
python3 -m venv .venv
source .venv/bin/activate

# 2. Upgrade pip and install editable package + dev dependencies
pip install --upgrade pip
pip install -e ".[dev]"
pip install pre-commit

# 3. Register git pre-commit hooks
pre-commit install
```

### Option B: Fast setup with `uv`
```bash
uv venv
source .venv/bin/activate
uv pip install -e ".[dev]" pre-commit
pre-commit install
```

### Option C: Conda / Miniconda (Matches CI)
```bash
conda env create -f environment_gcsfs.yaml
conda activate gcsfs_test
pip install -e . pre-commit
pre-commit install
```

---

## 3. Verification

Verify that all essential tools are installed and working:

```bash
pytest --version
flake8 --version
isort --version
pre-commit --version
```
