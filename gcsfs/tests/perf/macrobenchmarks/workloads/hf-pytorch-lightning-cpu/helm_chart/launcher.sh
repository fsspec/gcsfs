#!/bin/bash

# CPU emulator launcher. Each pod on c4-standard-192
# runs this; torchrun then forks GPUS_PER_NODE worker processes per pod (4 by
# default), so 2 nodes x 4 = 8 ranks total. Per-node ranks are capped at 4 (not
# 8) so a checkpoint-restoring run fits the 720GB c4-standard-192 host RAM; see
# values_base.yaml.

set -euo pipefail

export PYTHONUNBUFFERED=1

# The default workload image is nvcr.io/nvidia/pytorch:26.05-py3 (see
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

# --- node-local bootstrap cache ---------------------------------------------
# HOST_CACHE_PATH is a hostPath volume (chart value workload.hostCachePath) that
# outlives the pod. The seed-checkpoint release and the measured release land on
# the same nodes, so whatever is staged here once is reused by the second
# generation instead of being fetched again. Unset/empty keeps the previous
# per-pod behaviour: every path below still works, it just always misses.
CACHE_ROOT="${HOST_CACHE_PATH:-}"
if [[ -n "$CACHE_ROOT" ]]; then
  mkdir -p "$CACHE_ROOT"
  echo "Bootstrap cache: $CACHE_ROOT"
else
  echo "Bootstrap cache disabled; staging into the pod filesystem."
fi

# Publish a directory into the cache only once it is provably whole: populate a
# unique staging sibling, drop a marker, then rename. The rename is atomic, so a
# pod killed mid-download cannot leave a partial directory that the next pod
# mistakes for a hit -- the failure the old "does the directory exist" guard
# would have made permanent now that the cache survives pod deletion.
#
# Contract: the command is invoked with a staging directory as its final
# argument and must produce exactly one entry inside it named after $dest. Both
# `gcloud storage cp -r SRC dir/` and `tar -C dir -xf` do that naturally.
stage_once() {
  local dest="$1"; shift
  local name staging
  name=$(basename "$dest")
  if [[ -f "$dest/.complete" ]]; then
    echo "Cache hit: $dest"
    return 0
  fi
  staging="${dest}.staging.$$"
  rm -rf "$staging"
  mkdir -p "$staging"
  # Guarded rather than bare so a failed download cleans up after itself; the
  # `return 1` still aborts the launcher under `set -e`, which is what should
  # happen -- a pod that cannot stage its model must fail, not train on nothing.
  if ! "$@" "$staging"; then
    echo "stage_once: population failed for $dest" >&2
    rm -rf "$staging"
    return 1
  fi
  if [[ ! -d "$staging/$name" ]]; then
    echo "stage_once: command did not produce $staging/$name" >&2
    rm -rf "$staging"
    return 1
  fi
  touch "$staging/$name/.complete"
  # Only an incomplete leftover from an earlier crash can be at $dest here; a
  # complete one returned above. Clear it so the rename lands.
  rm -rf "$dest"
  # Plain `mv`, not `mv -T`: -T is GNU-only and the images are not guaranteed to
  # ship it. $dest was just removed, so this is a rename within one directory
  # (same filesystem => atomic), not a move-into-directory. The marker check
  # catches the move-into-directory shape if something recreated $dest in the
  # meantime -- only reachable when singlePodPerNode is off and two pods on one
  # node race, which this chart does not do.
  if ! mv "$staging/$name" "$dest" || [[ ! -f "$dest/.complete" ]]; then
    rm -rf "$staging"
    # The other pod's copy is equally valid, so a lost race is not an error.
    if [[ -f "$dest/.complete" ]]; then
      echo "Cache populated concurrently: $dest"
      return 0
    fi
    echo "Failed to publish $dest" >&2
    return 1
  fi
  rm -rf "$staging"
  echo "Cached: $dest"
}

fetch_gcloud_sdk() {
  local staging="$1"
  local archive="$staging/google-cloud-cli-linux-x86_64.tar.gz"
  curl -fsSL \
    https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-cli-linux-x86_64.tar.gz \
    -o "$archive"
  tar -C "$staging" -xf "$archive"
  rm -f "$archive"
}

if ! command -v gcloud >/dev/null 2>&1; then
  echo "Installing standalone gcloud CLI..."
  GCLOUD_PARENT="${CACHE_ROOT:-/tmp}"
  mkdir -p "$GCLOUD_PARENT"
  stage_once "$GCLOUD_PARENT/google-cloud-sdk" fetch_gcloud_sdk
  export PATH="$PATH:$GCLOUD_PARENT/google-cloud-sdk/bin"
fi

# Wheels are immutable per (name, version, url), so a shared pip cache is safe
# and saves the second pod generation the whole download+build pass. The
# artifact under test is deliberately excluded below.
if [[ -n "$CACHE_ROOT" ]]; then
  export PIP_CACHE_DIR="$CACHE_ROOT/pip"
  mkdir -p "$PIP_CACHE_DIR"
  PIP_ARGS=()
else
  PIP_ARGS=(--no-cache-dir)
fi

# If MODEL_ID is a GCS path, pull the ~16GB of weights once per *node* (not
# once per pod) into the bootstrap cache, so the measured release reuses what
# the seed release already staged. cpu_sim.py loads it from $LOCAL_MODEL_PATH
# with local_files_only=True, so the ranks on this node do not race on the
# HuggingFace API. This download is deliberately outside the measurement
# boundary (gcloud, not gcsfs) so caching it moves no metric -- it only removes
# idle time from the run.
fetch_model_from_gcs() {
  # Strip trailing slash: `gcloud storage cp -r gs://bucket/dir/ dest/` would
  # copy the *contents* of dir into dest (rsync-style), so the files would land
  # at dest/config.json instead of dest/<basename>/config.json. stage_once
  # requires the latter.
  gcloud storage cp -r "${MODEL_ID%/}" "$1/"
}
if [[ "${MODEL_ID:-}" == gs://* ]]; then
  echo "MODEL_ID is a GCS path: $MODEL_ID"
  DIR_NAME=$(basename "${MODEL_ID%/}")
  MODEL_ROOT="${CACHE_ROOT:-/tmp}"
  mkdir -p "$MODEL_ROOT"
  LOCAL_MODEL_PATH="$MODEL_ROOT/$DIR_NAME"
  stage_once "$LOCAL_MODEL_PATH" fetch_model_from_gcs
  # cpu_sim.py reads this to find the staged weights; it falls back to
  # /tmp/<basename> when unset, which is where they used to land.
  export LOCAL_MODEL_PATH
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
pip3 install "${PIP_ARGS[@]}" --index-url https://download.pytorch.org/whl/cpu torch
pip3 install "${PIP_ARGS[@]}" -r /workload/configs/requirements.txt

if [[ -n "${REQUIREMENTS:-}" ]]; then
  # Optional escape hatch: REQUIREMENTS lets a run install/override arbitrary
  # packages (the gcsfs under test and/or a custom lightning build, etc.)
  # without rebuilding the image or editing requirements.txt. It runs AFTER
  # requirements.txt, so a spec here overrides the pinned versions there.
  # Word-split intentional.
  # --no-cache-dir regardless of the shared pip cache: the build under test is
  # the one thing that must never be served from a previous pod's download, so a
  # re-pushed artifact at an unchanged URL can never go stale here.
  # shellcheck disable=SC2086
  pip3 install --no-cache-dir $REQUIREMENTS
  # Reinstall only the requested packages so their dependency graph is not
  # unnecessarily reinstalled after the normal resolution pass above.
  # shellcheck disable=SC2086
  pip3 install --no-cache-dir --no-deps --force-reinstall $REQUIREMENTS
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

# Parallel training strategy: ddp (default), fsdp_sharded, fsdp_full,
# model_parallel_sharded, or model_parallel_full.
export TRAINING_STRATEGY=${TRAINING_STRATEGY:-ddp}
# ModelParallelStrategy mesh (model_parallel_* only); TP x DP must equal world.
export TENSOR_PARALLEL_SIZE=${TENSOR_PARALLEL_SIZE:-4}
export DATA_PARALLEL_SIZE=${DATA_PARALLEL_SIZE:-2}

# Training parameters. Epochs bind only if MAX_STEPS=-1.
export NUM_TRAIN_EPOCHS=${NUM_TRAIN_EPOCHS:-3}
export PER_DEVICE_TRAIN_BATCH_SIZE=${PER_DEVICE_TRAIN_BATCH_SIZE:-8}
export GRADIENT_ACCUMULATION_STEPS=${GRADIENT_ACCUMULATION_STEPS:-1}

# Enable Python fault handler so a segfault in any of the 8 ranks dumps
# a stack trace into pod logs.
export PYTHONFAULTHANDLER=1
# DataLoader workers do their own tokenization; cap BLAS threads per worker
# so 4 ranks * 16 workers stay within the 192 vCPUs on c4-standard-192.
# (Lower DATALOADER_NUM_WORKERS if step-time IO timing looks CPU-bound:
# 4 ranks * 16 workers = 64 procs vs 192 vCPUs.)
export OMP_NUM_THREADS=${OMP_NUM_THREADS:-1}
export MKL_NUM_THREADS=${MKL_NUM_THREADS:-1}
export PYTHONPATH=${PYTHONPATH:-}:/workload/configs

torchrun \
  --nproc_per_node="${GPUS_PER_NODE:-4}" \
  --nnodes="$NNODES" \
  --node_rank="$NODE_RANK" \
  --master_addr="$MASTER_ADDR" \
  --master_port="$MASTER_PORT" \
  "$PYTHON_MAIN"
