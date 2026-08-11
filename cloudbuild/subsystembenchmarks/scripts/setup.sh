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

if [[ "${MODEL_ID:-}" == gs://* ]]; then
  echo "MODEL_ID is a GCS path: $MODEL_ID"
  DIR_NAME=$(basename "${MODEL_ID%/}")
  LOCAL_MODEL_PATH="/tmp/$DIR_NAME"
  if [[ ! -d "$LOCAL_MODEL_PATH" ]]; then
    echo "Installing standalone gcloud CLI..."
    # Install in /tmp/gcloud-install
    mkdir -p /tmp/gcloud-install
    (
      cd /tmp/gcloud-install
      curl -sSO https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-cli-linux-x86_64.tar.gz
      tar -xf google-cloud-cli-linux-x86_64.tar.gz
    )
    GCLOUD="/tmp/gcloud-install/google-cloud-sdk/bin/gcloud"

    echo "Downloading model from GCS to $LOCAL_MODEL_PATH..."
    # gcloud storage cp -r gs://bucket/dir /tmp/ will create /tmp/dir/
    $GCLOUD storage cp -r "${MODEL_ID%/}" /tmp/
    echo "Download complete."

    # Clean up gcloud installation to free space
    rm -rf /tmp/gcloud-install
  else
    echo "Model already exists at $LOCAL_MODEL_PATH, skipping download."
  fi
fi

read -r -a REQUIREMENT_SPECS <<< "$REQUIREMENTS_OVERRIDE"
if ((${#REQUIREMENT_SPECS[@]})); then
  # Resolve any dependencies needed by the override, then reinstall only the
  # requested packages in case an existing installation has the same version.
  pip install -- "${REQUIREMENT_SPECS[@]}"
  pip install --no-deps --force-reinstall -- "${REQUIREMENT_SPECS[@]}"
fi
REQUIREMENTS_OVERRIDE="${REQUIREMENT_SPECS[*]}"
REQUIREMENTS_RESOLVED=$(pip list --format=json)
{
  printf 'export GCSFS_SUBSYSTEM_REQUIREMENTS_OVERRIDE=%q\n' "$REQUIREMENTS_OVERRIDE"
  printf 'export GCSFS_SUBSYSTEM_REQUIREMENTS_RESOLVED=%q\n' "$REQUIREMENTS_RESOLVED"
} >> "$HOME/ssb_cloudbuild.env"
