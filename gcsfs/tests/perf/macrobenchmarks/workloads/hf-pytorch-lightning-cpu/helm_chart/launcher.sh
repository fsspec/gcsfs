#!/bin/bash

# CPU emulator launcher. Mirrors a4_v1/launcher.sh but drops the GPU-specific
# bits (NCCL plugin, nvidia.com/gpu, RDMA NICs). Each pod on c4-standard-192
# runs this; torchrun then forks 8 worker processes per pod that stand in for
# the 8 GPU chips on an A4 node.

set -euo pipefail

export PYTHONUNBUFFERED=1

# The default workload image is nvcr.io/nvidia/pytorch:25.01-py3 (see
# values_base.yaml), which already ships curl/ca-certificates, so this guard is
# a no-op there. It exists for minimal Debian-based fallback images (e.g.
# python:3.11-slim) that omit curl/ca-certificates, which the gcloud install +
# model download below both need. Install once per pod; subsequent pip steps
# fail clearly if this step fails.
if ! command -v curl >/dev/null 2>&1; then
  echo "Installing curl + ca-certificates (needed for gcloud download)..."
  apt-get update -qq
  apt-get install -y --no-install-recommends curl ca-certificates
  rm -rf /var/lib/apt/lists/*
fi

echo "Installing standalone gcloud CLI..."
cd /tmp
curl -sSO https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-cli-linux-x86_64.tar.gz
tar -xf google-cloud-cli-linux-x86_64.tar.gz
rm google-cloud-cli-linux-x86_64.tar.gz
export PATH=$PATH:/tmp/google-cloud-sdk/bin
cd -

# If MODEL_ID is a GCS path, pull the weights once per pod. cpu_sim.py will
# then load from /tmp/<basename> with local_files_only=True, so the 8 ranks
# on this node do not race on the HuggingFace API. Skipping the download if
# the directory already exists keeps pod restarts cheap.
if [[ "${MODEL_ID:-}" == gs://* ]]; then
  echo "MODEL_ID is a GCS path: $MODEL_ID"
  DIR_NAME=$(basename "${MODEL_ID%/}")
  LOCAL_MODEL_PATH="/tmp/$DIR_NAME"

  if [[ ! -d "$LOCAL_MODEL_PATH" ]]; then
    echo "Downloading model from GCS to $LOCAL_MODEL_PATH..."
    # Strip trailing slash: `gcloud storage cp -r gs://bucket/dir/ /tmp/` would
    # copy the *contents* of dir into /tmp (rsync-style), so the files would
    # land at /tmp/config.json instead of /tmp/<basename>/config.json. cpu_sim
    # looks for the latter via local_files_only on $LOCAL_MODEL_PATH.
    /tmp/google-cloud-sdk/bin/gcloud storage cp -r "${MODEL_ID%/}" /tmp/
    echo "Download complete."
  else
    echo "Model already exists at $LOCAL_MODEL_PATH, skipping download."
  fi
fi

# Install workload deps. requirements.txt is mounted alongside the .py via
# the workload-configuration ConfigMap (see workload-config-configmap.yaml +
# workload-job.yaml items).
#
# Two pip invocations:
#   1. torch from PyTorch's CPU index (default PyPI wheel pulls the GPU
#      variant which is ~2 GB and refuses to import without libcuda).
#   2. everything else from requirements.txt via standard PyPI.
#
# Dual-mode behavior:
#   - On an image that already ships these deps (e.g. a custom pre-built
#     image): both pip calls become fast "Requirement already satisfied"
#     no-ops (~5s total for resolver pass).
#   - On the bare python:3.11-slim fallback: actually installs everything
#     (~3 min). The version pins in requirements.txt are the canonical ones.
pip3 install --no-cache-dir --index-url https://download.pytorch.org/whl/cpu torch
pip3 install --no-cache-dir -r /workload/configs/requirements.txt

if [[ -n "${REQUIREMENTS:-}" ]]; then
  # Optional escape hatch: REQUIREMENTS lets a run install/override arbitrary
  # packages (the gcsfs under test and/or a custom lightning build, etc.)
  # without rebuilding the image or editing requirements.txt. It runs AFTER
  # requirements.txt, so a spec here overrides the pinned versions there.
  # Word-split intentional.
  # shellcheck disable=SC2086
  pip3 install $REQUIREMENTS
fi

# JOB_COMPLETION_INDEX is set by the K8s Indexed Job (one value per pod,
# 0..NNODES-1). torchrun consumes it as --node_rank.
export NODE_RANK=$JOB_COMPLETION_INDEX
export HYDRA_FULL_ERROR=1

echo "Launching Torch distributed as node rank $NODE_RANK out of $NNODES nodes"

# Gloo (the CPU collective backend used by DDPStrategy in cpu_sim.py) does not
# auto-discover the right NIC across pods reliably; pin it to the pod's
# primary interface. With hostNetwork: false this is always eth0 inside the
# pod regardless of the c4 host's underlying NIC name (ens4/etc.).
export GLOO_SOCKET_IFNAME=${GLOO_SOCKET_IFNAME:-eth0}
export TOKENIZERS_PARALLELISM=false

# Parallel training strategy for cpu_sim.py (ddp default).
export TRAINING_STRATEGY=${TRAINING_STRATEGY:-ddp}

# Training parameters -- same defaults as a4_v1/launcher.sh so step time and
# checkpoint cadence are directly comparable between the GPU and CPU runs.
export NUM_TRAIN_EPOCHS=1
export PER_DEVICE_TRAIN_BATCH_SIZE=${PER_DEVICE_TRAIN_BATCH_SIZE:-8}
export GRADIENT_ACCUMULATION_STEPS=${GRADIENT_ACCUMULATION_STEPS:-1}

# Enable Python fault handler so a segfault in any of the 16 ranks dumps
# a stack trace into pod logs.
export PYTHONFAULTHANDLER=1
# DataLoader workers do their own tokenization; cap BLAS threads per worker
# so 8 ranks * 16 workers stay within the 192 vCPUs on c4-standard-192.
# (Lower DATALOADER_NUM_WORKERS if step-time IO timing looks CPU-bound:
# 8 ranks * 16 workers = 128 procs vs 192 vCPUs.)
export OMP_NUM_THREADS=${OMP_NUM_THREADS:-1}
export MKL_NUM_THREADS=${MKL_NUM_THREADS:-1}
export PYTHONPATH=${PYTHONPATH:-}:/workload/configs

torchrun \
  --nproc_per_node="${GPUS_PER_NODE:-8}" \
  --nnodes="$NNODES" \
  --node_rank="$NODE_RANK" \
  --master_addr="$MASTER_ADDR" \
  --master_port="$MASTER_PORT" \
  "$PYTHON_MAIN"
