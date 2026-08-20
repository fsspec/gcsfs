#!/usr/bin/env bash
# Sets up the local development environment for gcsfs.
set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
cd "$REPO_ROOT"

echo "==> Setting up development environment for gcsfs..."

# 1. Create virtual environment if one doesn't exist
VENV_DIR="${REPO_ROOT}/.venv"
if [[ ! -d "$VENV_DIR" && -z "${VIRTUAL_ENV:-}" && -z "${CONDA_PREFIX:-}" ]]; then
    echo "==> Creating Python virtual environment in ${VENV_DIR}..."
    if command -v uv >/dev/null 2>&1; then
        uv venv "$VENV_DIR"
    else
        python3 -m venv "$VENV_DIR"
    fi
fi

# Determine python/pip binary paths
if [[ -n "${VIRTUAL_ENV:-}" ]]; then
    PIP="${VIRTUAL_ENV}/bin/pip"
    PYTHON="${VIRTUAL_ENV}/bin/python"
elif [[ -d "$VENV_DIR" ]]; then
    PIP="${VENV_DIR}/bin/pip"
    PYTHON="${VENV_DIR}/bin/python"
elif command -v pip >/dev/null 2>&1; then
    PIP="pip"
    PYTHON="python3"
else
    echo "Error: Python/pip not found." >&2
    exit 1
fi

# 2. Install development dependencies and gcsfs in editable mode
echo "==> Installing gcsfs with [dev] dependencies and pre-commit..."
"$PIP" install --upgrade pip
"$PIP" install -e ".[dev]"
"$PIP" install pre-commit

# 3. Install git pre-commit hooks
if command -v git >/dev/null 2>&1 && [[ -d ".git" ]]; then
    PRE_COMMIT="$(dirname "$PIP")/pre-commit"
    if [[ -x "$PRE_COMMIT" ]]; then
        echo "==> Registering git pre-commit hooks..."
        "$PRE_COMMIT" install
    fi
fi

echo "==> Development environment setup complete!"
echo "    Activate with: source .venv/bin/activate"
