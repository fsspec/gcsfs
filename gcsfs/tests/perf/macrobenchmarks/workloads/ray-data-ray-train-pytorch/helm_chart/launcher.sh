#!/usr/bin/env bash
set -euo pipefail

export PYTHONUNBUFFERED=1
export PYTHONFAULTHANDLER=1
export RAY_TRAIN_V2_ENABLED=1
export RAY_DEDUP_LOGS=0
export RAY_COLOR_PREFIX=0
export NO_COLOR=1
export RAY_DATA_VERBOSE_PROGRESS=0
export TQDM_DISABLE=1
export GLOO_SOCKET_IFNAME=${GLOO_SOCKET_IFNAME:-eth0}
export NCCL_SOCKET_IFNAME=${NCCL_SOCKET_IFNAME:-eth0}
export TOKENIZERS_PARALLELISM=false
export PYTHONPATH=${PYTHONPATH:-}:/workload/configs
export TMPDIR=${TMPDIR:-/dev/shm}
export RAY_TMPDIR=${RAY_TMPDIR:-/dev/shm}
: "${RAY_OBJECT_STORE_MEMORY_BYTES:?RAY_OBJECT_STORE_MEMORY_BYTES is required}"
ray_session_dir="${RAY_TMPDIR%/}/ray"
mkdir -p "$TMPDIR" "$RAY_TMPDIR" "$ray_session_dir"
test -d "$TMPDIR"
test -d "$RAY_TMPDIR"
test -d "$ray_session_dir"

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
# mistakes for a hit -- the failure the old "does config.json exist" guard would
# have made permanent now that the cache survives pod deletion.
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

if ! command -v curl >/dev/null 2>&1; then
  apt-get update -qq
  apt-get install -y --no-install-recommends curl ca-certificates
  rm -rf /var/lib/apt/lists/*
fi

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
  # Not under /tmp: that path is the memory-backed emptyDir, so the extracted
  # SDK would be charged against the pod's memory rather than the node's disk.
  gcloud_parent="${CACHE_ROOT:-/workload}"
  mkdir -p "$gcloud_parent"
  stage_once "$gcloud_parent/google-cloud-sdk" fetch_gcloud_sdk
  export PATH="$gcloud_parent/google-cloud-sdk/bin:$PATH"
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

# USE_GPU is the chart's workload.gpu. It is declared, not detected: a GPU run
# whose driver or device plugin is missing must fail here rather than quietly
# install the CPU wheel and benchmark the CPU probe instead.
if [[ "${USE_GPU:-false}" == "true" ]]; then
  if ! command -v nvidia-smi >/dev/null 2>&1 && [[ ! -e /dev/nvidia0 ]]; then
    echo "USE_GPU=true but no NVIDIA device is visible in this container" >&2
    exit 1
  fi
else
  pip3 install "${PIP_ARGS[@]}" -r /workload/configs/requirements-cpu.txt
fi
pip3 install "${PIP_ARGS[@]}" -r /workload/configs/requirements.txt
if [[ -n "${REQUIREMENTS:-}" ]]; then
  # Word splitting is intentional: this is the established operator override.
  # --no-cache-dir regardless of the shared pip cache: the build under test is
  # the one thing that must never be served from a previous pod's download, so a
  # re-pushed artifact at an unchanged URL can never go stale here.
  # shellcheck disable=SC2086
  pip3 install --no-cache-dir --force-reinstall $REQUIREMENTS
fi

python3 - <<'PY'
import importlib.metadata
from pathlib import Path

import gcsfs

distribution = importlib.metadata.distribution("gcsfs")
module = Path(gcsfs.__file__).resolve()
package = Path(distribution.locate_file("gcsfs")).resolve()
if module.parent != package or gcsfs.__version__ != distribution.version:
    raise RuntimeError(
        f"unexpected gcsfs: file={module}, version={gcsfs.__version__}, "
        f"distribution={package}, distribution_version={distribution.version}"
    )
print(f"gcsfs artifact ready: file={module} version={gcsfs.__version__}")
PY

model_name=$(basename "${MODEL_ID%/}")
# ~16GB of weights. Staged into the node-local cache so the measured release
# reuses what the seed release already pulled. This download is deliberately
# outside the measurement boundary (gcloud, not gcsfs) so caching it moves no
# metric -- it only removes idle time from the run.
model_root="${CACHE_ROOT:-/workload}/models"
LOCAL_MODEL_PATH="$model_root/$model_name"
mkdir -p "$model_root"
fetch_model_from_gcs() {
  gcloud storage cp -r "${MODEL_ID%/}" "$1/"
}
fetch_model_from_hf() {
  local hf_args=()
  if [[ -n "${HF_TOKEN:-}" ]]; then
    hf_args+=(--token "$HF_TOKEN")
  fi
  huggingface-cli download "$MODEL_ID" --local-dir "$1/$model_name" "${hf_args[@]}"
}
if [[ "${MODEL_ID:-}" == gs://* ]]; then
  stage_once "$LOCAL_MODEL_PATH" fetch_model_from_gcs
elif [[ "${MODEL_ID:-}" != /* && "${MODEL_ID:-}" != ./* ]]; then
  stage_once "$LOCAL_MODEL_PATH" fetch_model_from_hf
else
  LOCAL_MODEL_PATH="${MODEL_ID%/}"
fi
export LOCAL_MODEL_PATH

python3 - <<'PY'
import inspect

import ray
import torch
from ray.data._internal.iterator.stream_split_iterator import StreamSplitDataIterator
from ray.data._internal.stats import DatasetStats
from ray.train import (
    Checkpoint,
    CheckpointConsistencyMode,
    CheckpointUploadMode,
    UserCallback,
    get_all_reported_checkpoints,
    report,
)
from ray.train.v2._internal.execution.callback import ControllerCallback
from ray.train.v2._internal.execution.checkpoint import checkpoint_manager
from ray.train.v2._internal.execution.storage import (
    _exists_at_fs_path,
    _pyarrow_fs_copy_files,
)
from torch.distributed.checkpoint import FileSystemReader, FileSystemWriter
from torch.distributed.checkpoint.state_dict import get_state_dict, set_state_dict
from torch.distributed.fsdp import fully_shard

assert all(
    callable(api)
    for api in (
        FileSystemReader,
        FileSystemWriter,
        get_state_dict,
        set_state_dict,
        fully_shard,
    )
)

# Public checkpoint reporting surface.
assert CheckpointUploadMode.ASYNC is not None
assert CheckpointConsistencyMode.COMMITTED is not None
assert callable(get_all_reported_checkpoints)
assert callable(UserCallback.after_report)
report_parameters = inspect.signature(report).parameters
for required in ("checkpoint_dir_name", "checkpoint_upload_mode",
                 "delete_local_checkpoint_after_upload", "checkpoint_upload_fn"):
    assert required in report_parameters, required

# Non-public surface the benchmark observations depend on. Assert it here so a
# Ray upgrade fails the run at startup instead of silently dropping a metric.
assert callable(ControllerCallback.after_controller_start)
assert callable(checkpoint_manager.delete_fs_path)
assert callable(_exists_at_fs_path)
assert callable(_pyarrow_fs_copy_files)
assert callable(Checkpoint)
iteration_timers = DatasetStats(metadata={}, parent=None)
for timer in (iteration_timers.iter_total_blocked_s,
              iteration_timers.iter_time_to_first_batch_s):
    assert callable(timer.get)
    assert isinstance(timer._total_count, (int, float))
assert "_iter_stats" in inspect.getsource(StreamSplitDataIterator.__init__)

print(f"Ray benchmark API ready: version={ray.__version__}")
PY

RAY_PORT=${RAY_PORT:-6379}
RAY_RESOURCES="{\"train_slot\": ${RANKS_PER_NODE}}"

cleanup() {
  ray stop --force >/dev/null 2>&1 || true
}
trap cleanup EXIT
trap 'exit 143' INT TERM

wait_for_nodes() {
  local expected_nodes=$1
  local attempts=120
  while (( attempts > 0 )); do
    if EXPECTED_NODES="$expected_nodes" python3 - <<'PY'
import os

import ray

ray.init(address="auto", ignore_reinit_error=True, logging_level="ERROR")
live_nodes = [node for node in ray.nodes() if node["Alive"]]
expected_nodes = int(os.environ["EXPECTED_NODES"])
ray.shutdown()
assert len(live_nodes) == expected_nodes, (len(live_nodes), expected_nodes)
PY
    then
      return
    fi
    sleep 2
    ((attempts -= 1))
  done
  echo "Timed out waiting for $expected_nodes live Ray nodes" >&2
  return 1
}

wait_for_head_shutdown() {
  while ray health-check --address="$RAY_HEAD_ADDRESS" >/dev/null 2>&1; do
    sleep 5
  done
}

wait_for_head_ready() {
  local attempts=${RAY_HEAD_WAIT_ATTEMPTS:-120}
  local interval=${RAY_HEAD_WAIT_INTERVAL:-2}
  while (( attempts > 0 )); do
    if ray health-check --address="$RAY_HEAD_ADDRESS" >/dev/null 2>&1; then
      return
    fi
    attempts=$((attempts - 1))
    if (( attempts > 0 )); then
      sleep "$interval"
    fi
  done
  echo "Timed out waiting for Ray head at $RAY_HEAD_ADDRESS" >&2
  return 1
}

if [[ "$JOB_COMPLETION_INDEX" == "0" ]]; then
  ray start --head --port="$RAY_PORT" \
    --temp-dir="$ray_session_dir" \
    --object-store-memory="$RAY_OBJECT_STORE_MEMORY_BYTES" \
    --resources="$RAY_RESOURCES"
  wait_for_nodes "$NNODES"
  if python3 -c '
import importlib
import os

module_name = os.path.splitext(os.path.basename(os.environ["PYTHON_MAIN"]))[0]
importlib.import_module(module_name).main()
'; then
    status=0
  else
    status=$?
  fi
  cleanup
  trap - EXIT
  exit "$status"
fi

wait_for_head_ready
ray start --address="$RAY_HEAD_ADDRESS" \
  --temp-dir="$ray_session_dir" \
  --object-store-memory="$RAY_OBJECT_STORE_MEMORY_BYTES" \
  --resources="$RAY_RESOURCES"
wait_for_head_shutdown
cleanup
trap - EXIT
