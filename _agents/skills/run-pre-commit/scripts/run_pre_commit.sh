#!/usr/bin/env bash
# Runs pre-commit checks matching repository config (.pre-commit-config.yaml) and CI.
set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
cd "$REPO_ROOT"

# Dynamically find pre-commit binary
find_pre_commit() {
    for d in "${VIRTUAL_ENV:-}" "${CONDA_PREFIX:-}" \
             "$REPO_ROOT/.venv" "$REPO_ROOT/venv" "$REPO_ROOT/env" \
             "$REPO_ROOT/../.venv" "$REPO_ROOT/../../gcsfs/.venv"; do
        if [[ -n "$d" && -x "$d/bin/pre-commit" ]]; then
            echo "$d/bin/pre-commit"
            return 0
        fi
    done
    if command -v pre-commit >/dev/null 2>&1; then
        command -v pre-commit
        return 0
    fi
    return 1
}

PRE_COMMIT="$(find_pre_commit 2>/dev/null || true)"

if [[ -z "$PRE_COMMIT" ]]; then
    echo "Error: 'pre-commit' command not found." >&2
    echo "Install it with: pip install pre-commit" >&2
    exit 1
fi

echo "==> Running pre-commit hooks..."

if [[ $# -eq 0 ]]; then
    # Default: Run all hooks across all files
    "$PRE_COMMIT" run --all-files
else
    # Forward any passed arguments (e.g. --files, hook names, install, autoupdate)
    "$PRE_COMMIT" "$@"
fi
