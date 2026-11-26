#!/usr/bin/env bash
set -euo pipefail

_INSTALL_GROUP_REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)

_python_bin() {
  if command -v python >/dev/null 2>&1; then
    echo python
  else
    echo python3
  fi
}

# Probe sysfs PCI + /dev/vfio via accelerator.py (single source of truth, stdlib-only).
is_tpu_host() {
  local py
  py=$(_python_bin)
  "$py" "${_INSTALL_GROUP_REPO_ROOT}/gcsfs/tests/perf/subsystembenchmarks/_common/accelerator.py" --has-tpu-hardware
}

# Inspect installed accelerator packages once via importlib.metadata (never `import torch`,
# so TPU runtime autoload is not triggered). Writes constraints for preinstalled
# torch/torch_tpu/libtpu/jax/jaxlib and optionally filters requirements.txt.
write_accelerator_constraints() {
  local constraint_out="$1"
  local req_in="${2:-}"
  local req_out="${3:-}"
  local py
  py=$(_python_bin)
  "$py" - "$constraint_out" "$req_in" "$req_out" <<'PY'
import importlib.metadata
import os
import re
import sys

constraint_path, req_path, out_path = sys.argv[1:4]


def _norm(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


_PEP440_RE = re.compile(
    r"^\d+(?:\.\d+)*(?:(?:a|b|rc)\d+)?(?:\.post\d+)?(?:\.dev\d+)?(?:\+[a-z0-9.]+)?$",
    re.IGNORECASE,
)
_REQ_NAME_RE = re.compile(r"^\s*([A-Za-z0-9][A-Za-z0-9._-]*)")

constraints = []
pinned = set()
has_torch = False
for pkg in ("torch", "torch_tpu", "torch-tpu", "libtpu", "jax", "jaxlib"):
    try:
        ver = importlib.metadata.version(pkg)
        constraints.append(f"{pkg}=={ver}")
        pinned.add(_norm(pkg))
        if pkg == "torch":
            has_torch = True
    except importlib.metadata.PackageNotFoundError:
        pass

# Outside a venv, Debian/distutils-installed packages lack a RECORD file so pip
# refuses to uninstall/upgrade them (e.g. PyYAML 6.0.1 in NGC PyTorch images).
no_record_names = set()
if has_torch and sys.prefix == sys.base_prefix:
    for dist in importlib.metadata.distributions():
        name = dist.metadata.get("Name")
        if not name:
            continue
        if (
            dist.read_text("RECORD") is None
            and dist.read_text("installed-files.txt") is None
        ):
            norm = _norm(name)
            no_record_names.add(norm)
            ver = dist.version or ""
            if norm not in pinned and _PEP440_RE.match(ver):
                constraints.append(f"{name}=={ver}")
                pinned.add(norm)

with open(constraint_path, "w") as f:
    f.write(("\n".join(constraints) + "\n") if constraints else "")

if req_path and out_path:
    skip_names = (
        ({"torch", "torchvision", "torchaudio"} | no_record_names)
        if has_torch
        else set()
    )
    out_lines = []

    def flatten(path: str) -> None:
        base = os.path.dirname(os.path.abspath(path))
        with open(path) as fh:
            for raw in fh:
                stripped = raw.strip()
                if not stripped or stripped.startswith("#"):
                    continue
                if stripped.startswith(("-r ", "--requirement ")):
                    rel = stripped.split(None, 1)[1].strip()
                    flatten(os.path.join(base, rel))
                    continue
                m = _REQ_NAME_RE.match(stripped)
                if m and _norm(m.group(1)) in skip_names:
                    continue
                out_lines.append(stripped)

    flatten(req_path)
    with open(out_path, "w") as f:
        f.write("\n".join(out_lines) + "\n")
PY
}

install_group_requirements() {
  local req_file="gcsfs/tests/perf/subsystembenchmarks/${GROUP}/requirements.txt"
  local tmp_dir constraint_file filtered_req
  tmp_dir=$(mktemp -d)
  constraint_file="${tmp_dir}/constraints.txt"
  filtered_req="${tmp_dir}/requirements.txt"
  write_accelerator_constraints "$constraint_file" "$req_file" "$filtered_req"
  if [[ -s "$constraint_file" ]]; then
    PIP_CONSTRAINT="$constraint_file" pip install -r "$filtered_req" >/dev/null
  else
    pip install -r "$req_file" >/dev/null
  fi
  rm -rf "$tmp_dir"
}

install_editable_gcsfs() {
  local constraint_file="${1:-${TORCH_CONSTRAINT:-}}"
  if [[ -n "$constraint_file" && -s "$constraint_file" ]]; then
    PIP_CONSTRAINT="$constraint_file" pip install -e . >/dev/null
  else
    pip install -e . >/dev/null
  fi
}

install_requirements_override() {
  local raw="${1:-}"
  local constraint_file="${2:-${TORCH_CONSTRAINT:-}}"
  local -a specs=()
  read -r -a specs <<< "$raw"
  if ((${#specs[@]})); then
    # Resolve any dependencies needed by the override, then reinstall only the
    # requested packages in case an existing installation has the same version.
    if [[ -n "$constraint_file" && -s "$constraint_file" ]]; then
      PIP_CONSTRAINT="$constraint_file" pip install -- "${specs[@]}"
      PIP_CONSTRAINT="$constraint_file" pip install --no-deps --force-reinstall -- "${specs[@]}"
    else
      pip install -- "${specs[@]}"
      pip install --no-deps --force-reinstall -- "${specs[@]}"
    fi
  fi
  export GCSFS_SUBSYSTEM_REQUIREMENTS_OVERRIDE="${specs[*]}"
}

install_accelerator_packages() {
  if ! is_tpu_host; then
    return 0
  fi
  local py has_torch_tpu
  py=$(_python_bin)
  has_torch_tpu=$("$py" -c '
import importlib.metadata
for name in ("torch_tpu", "torch-tpu"):
    try:
        importlib.metadata.version(name)
        print("yes")
        break
    except importlib.metadata.PackageNotFoundError:
        pass
' 2>/dev/null || true)
  if [[ "$has_torch_tpu" != "yes" ]]; then
    pip install torch_tpu --extra-index-url https://download.pytorch.org/whl/cpu
  fi
}

main() {
  install_group_requirements
  install_accelerator_packages
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  main "$@"
fi
