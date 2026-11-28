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
pip install -r "gcsfs/tests/perf/subsystembenchmarks/requirements-cpu.txt" \
  -r "gcsfs/tests/perf/subsystembenchmarks/$GROUP/requirements.txt" >/dev/null
# Install local editable gcsfs last so its fsspec floor wins over framework caps.
pip install -e . >/dev/null

if [[ "${GROUP}" == "checkpointing/pytorch_lightning" || "${GROUP}" == "checkpointing/ray_pytorch" ]]; then
  STAGE_MODEL_ID="${MODEL_ID:-}"
  if [[ -z "${STAGE_MODEL_ID}" ]]; then
    STAGE_MODEL_ID=$(python -c 'import sys, yaml
with open(sys.argv[1]) as f:
    print((yaml.safe_load(f) or {}).get("common", {}).get("model_id", ""))' \
      "gcsfs/tests/perf/subsystembenchmarks/${GROUP}/configs.yaml")
  fi
  if [[ "${STAGE_MODEL_ID}" == gs://* ]]; then
    echo "Staging legacy checkpointing model from GCS: $STAGE_MODEL_ID"
    DIR_NAME=$(basename "${STAGE_MODEL_ID%/}")
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
      $GCLOUD storage cp -r "${STAGE_MODEL_ID%/}" /tmp/
      echo "Download complete."

      # Clean up gcloud installation to free space
      rm -rf /tmp/gcloud-install
    else
      echo "Model already exists at $LOCAL_MODEL_PATH, skipping download."
    fi
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
