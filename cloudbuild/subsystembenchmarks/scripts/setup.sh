#!/usr/bin/env bash
set -euo pipefail

source "$HOME/ssb_cloudbuild.env"
cd "$HOME/gcsfs"

sudo apt-get update >/dev/null
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y \
  python3-pip python3-venv git >/dev/null
python3 -m venv env
source env/bin/activate
pip install --upgrade pip >/dev/null

# Manual gcloud builds submit omits .git, which leaves hatch-vcs without a version.
if [[ ! -d .git ]]; then
  export SETUPTOOLS_SCM_PRETEND_VERSION="0.0.0"
fi
pip install -e . >/dev/null
pip install -r "gcsfs/tests/perf/subsystembenchmarks/$GROUP/requirements.txt" >/dev/null

read -r -a REQUIREMENT_SPECS <<< "$REQUIREMENTS_OVERRIDE"
if ((${#REQUIREMENT_SPECS[@]})); then
  pip install -- "${REQUIREMENT_SPECS[@]}"
fi
REQUIREMENTS_OVERRIDE="${REQUIREMENT_SPECS[*]}"
REQUIREMENTS_RESOLVED=$(pip list --format=json)
{
  printf 'export GCSFS_SUBSYSTEM_REQUIREMENTS_OVERRIDE=%q\n' "$REQUIREMENTS_OVERRIDE"
  printf 'export GCSFS_SUBSYSTEM_REQUIREMENTS_RESOLVED=%q\n' "$REQUIREMENTS_RESOLVED"
} >> "$HOME/ssb_cloudbuild.env"
