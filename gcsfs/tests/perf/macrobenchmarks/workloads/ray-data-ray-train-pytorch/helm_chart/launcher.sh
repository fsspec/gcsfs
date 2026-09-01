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

if ! command -v curl >/dev/null 2>&1; then
  apt-get update -qq
  apt-get install -y --no-install-recommends curl ca-certificates
  rm -rf /var/lib/apt/lists/*
fi

if ! command -v gcloud >/dev/null 2>&1; then
  gcloud_archive=/tmp/google-cloud-cli-linux-x86_64.tar.gz
  curl -fsSL \
    https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-cli-linux-x86_64.tar.gz \
    -o "$gcloud_archive"
  tar -C /tmp -xf "$gcloud_archive"
  rm -f "$gcloud_archive"
  export PATH="/tmp/google-cloud-sdk/bin:$PATH"
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
  pip3 install --no-cache-dir -r /workload/configs/requirements-cpu.txt
fi
pip3 install --no-cache-dir -r /workload/configs/requirements.txt
if [[ -n "${REQUIREMENTS:-}" ]]; then
  # Word splitting is intentional: this is the established operator override.
  # shellcheck disable=SC2086
  pip3 install --force-reinstall $REQUIREMENTS
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
model_root=/workload/models
LOCAL_MODEL_PATH="$model_root/$model_name"
mkdir -p "$model_root"
if [[ "${MODEL_ID:-}" == gs://* ]]; then
  if [[ ! -f "$LOCAL_MODEL_PATH/config.json" ]]; then
    gcloud storage cp -r "${MODEL_ID%/}" "$model_root/"
  fi
elif [[ "${MODEL_ID:-}" != /* && "${MODEL_ID:-}" != ./* ]]; then
  if [[ ! -f "$LOCAL_MODEL_PATH/config.json" ]]; then
    hf_args=()
    if [[ -n "${HF_TOKEN:-}" ]]; then
      hf_args+=(--token "$HF_TOKEN")
    fi
    huggingface-cli download "$MODEL_ID" --local-dir "$LOCAL_MODEL_PATH" "${hf_args[@]}"
  fi
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
