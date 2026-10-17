#!/bin/bash
set -euxo pipefail

echo "Starting TorchTitan GPU workload launcher script..."

# Configure NCCL / GPUDirect-RDMA (gIB) on A3 Ultra / H200 clusters if present
if [[ -f /usr/local/gib/scripts/set_nccl_env.sh ]]; then
  echo "Sourcing /usr/local/gib/scripts/set_nccl_env.sh..."
  set +u
  source /usr/local/gib/scripts/set_nccl_env.sh
  set -u
  export NCCL_SOCKET_IFNAME="${NCCL_SOCKET_IFNAME_OVERRIDE:-eth0,eth1}"
  if [[ -f /usr/local/gib/configs/tuner_config_a3u.txtpb ]]; then
    export NCCL_TUNER_CONFIG_PATH="/usr/local/gib/configs/tuner_config_a3u.txtpb"
  fi
fi

if [[ -f "/usr/local/gib/configs/guest_config.txtpb" ]]; then
  export NCCL_SHIMNET_GUEST_CONFIG_CHECKER_CONFIG_FILE="/usr/local/gib/configs/guest_config.txtpb"
fi

if [[ -n "${NCCL_PLUGIN_PATH:-}" ]]; then
  export LD_LIBRARY_PATH="${NCCL_PLUGIN_PATH}${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}"
  echo "LD_LIBRARY_PATH is set to: $LD_LIBRARY_PATH"
fi

export TORCH_NCCL_AVOID_RECORD_STREAMS="${TORCH_NCCL_AVOID_RECORD_STREAMS:-1}"
export PYTORCH_CUDA_ALLOC_CONF="${PYTORCH_CUDA_ALLOC_CONF:-expandable_segments:True}"
export OMP_NUM_THREADS="${OMP_NUM_THREADS:-8}"

# Wait for head node DNS resolution when running multi-node JobSet (bounded to 150 attempts = 5 minutes)
if [[ "${NNODES:-1}" -gt 1 ]] && [[ -n "${MASTER_ADDR:-}" ]]; then
  echo "Waiting for head node (${MASTER_ADDR}) to resolve..."
  DNS_ATTEMPTS=0
  until getent hosts "${MASTER_ADDR}"; do
    DNS_ATTEMPTS=$((DNS_ATTEMPTS + 1))
    if [[ "${DNS_ATTEMPTS}" -ge 150 ]]; then
      echo "ERROR: Timed out waiting for head node (${MASTER_ADDR}) DNS resolution." >&2
      exit 1
    fi
    echo "Still waiting for ${MASTER_ADDR} (attempt ${DNS_ATTEMPTS}/150)..."
    sleep 2
  done
  echo "Head node (${MASTER_ADDR}) resolved successfully."
fi

# TorchTitan `main` tracks PyTorch nightly, and the NGC image's pre-release torch
# lacks APIs it imports (e.g. torch.distributed.pipelining
# .analyze_pipeline_activation_liveness), so replace torch and torchao with
# matching nightlies, then install TorchTitan `main` pinned to that torch.
# - PIP_CONFIG_FILE=/dev/null: NGC's /etc/xdg/pip/pip.conf points at
#   pypi.ngc.nvidia.com, which times out inside GKE pods.
# - torchao: NGC's build crashes on import against nightly triton, and
#   TorchTitan imports torchao unconditionally.
export PIP_CONFIG_FILE=/dev/null
unset PIP_EXTRA_INDEX_URL PIP_CONSTRAINT
if [[ -n "${HOST_CACHE_PATH:-}" ]]; then
  export PIP_CACHE_DIR="${HOST_CACHE_PATH}/pip"
fi
TORCH_NIGHTLY_INDEX="${TORCH_NIGHTLY_INDEX:-https://download.pytorch.org/whl/nightly/cu132}"

echo "Installing PyTorch + torchao nightlies from ${TORCH_NIGHTLY_INDEX}..."
pip uninstall -y torchtitan || true  # NGC ships an unreleased TorchTitan snapshot.
pip install --pre --force-reinstall --no-deps torch --index-url "${TORCH_NIGHTLY_INDEX}"
pip install --pre torch --index-url "${TORCH_NIGHTLY_INDEX}"
USE_CPP=0 pip install --pre --upgrade torchao --index-url "${TORCH_NIGHTLY_INDEX}"

# Keep later installs from swapping torch for a PyPI release.
TORCH_CONSTRAINT=/tmp/torch_nightly_constraint.txt
python3 - > "${TORCH_CONSTRAINT}" <<'PY'
import torch
print(f"torch=={torch.__version__}")
PY
export PIP_CONSTRAINT="${TORCH_CONSTRAINT}"

echo "Installing TorchTitan main..."
pip install -r /etc/workload-config/requirements.txt

# Install the gcsfs build under test in its own pip call.
# TorchTitan main caps datasets<4.8, which caps fsspec<=2026.2.0.
# Resolving them together makes pip silently fall back to an old gcsfs release.
# Installed separately, pip only warns about datasets' fsspec cap (streaming still works).
if [[ -n "${REQUIREMENTS:-}" ]]; then
  echo "Installing custom requirements: ${REQUIREMENTS}"
  pip install --no-cache-dir ${REQUIREMENTS}
  pip install --no-cache-dir --no-deps --force-reinstall ${REQUIREMENTS}
else
  pip install --upgrade gcsfs
fi

# Record the exact upstream TorchTitan commit and torch nightly this run used.
python3 - <<'PY'
import importlib.metadata, json, torch
direct = importlib.metadata.distribution("torchtitan").read_text("direct_url.json")
commit = json.loads(direct).get("vcs_info", {}).get("commit_id") if direct else None
print(f"Workload Versions : torch {torch.__version__} : "
      f"torchtitan {importlib.metadata.version('torchtitan')} : "
      f"torchtitan_commit {commit} : gcsfs {importlib.metadata.version('gcsfs')}", flush=True)
PY

# Stage lightweight tokenizer files once per node before torchrun. TorchTitan's
# HuggingFaceTokenizer reads them from a local `hf_assets_path`, and staging here
# keeps tokenizer reads out of the gcsfs dataset/checkpoint metrics.
if [[ "${MODEL_ID:-}" == gs://* ]]; then
  MODEL_SLUG="$(echo "${MODEL_ID}" | tr '/:' '__')"
  CACHE_ROOT="${HOST_CACHE_PATH:-/dev/shm}/torchtitan_tokenizer"
  mkdir -p "${CACHE_ROOT}"
  export LOCAL_TOKENIZER_PATH="${CACHE_ROOT}/${MODEL_SLUG}"
  READY_FILE="${LOCAL_TOKENIZER_PATH}/.ready"
  LOCK_FILE="${CACHE_ROOT}/.${MODEL_SLUG}.lock"
  (
    if command -v flock >/dev/null 2>&1; then
      flock -x 9
    fi
    if [[ ! -f "${READY_FILE}" ]]; then
      STAGE_TMP="$(mktemp -d "${CACHE_ROOT}/.${MODEL_SLUG}.tmp.XXXXXX")"
      STAGE_TMP_DIR="${STAGE_TMP}" python3 - <<'PY'
import os
import fsspec
model_id = os.environ["MODEL_ID"].rstrip("/")
stage_dir = os.environ["STAGE_TMP_DIR"]
fs, _ = fsspec.core.url_to_fs(model_id)
downloaded = 0
for remote_path in fs.ls(model_id, detail=False):
    fname = os.path.basename(remote_path.rstrip("/"))
    if any(pat in fname.lower() for pat in ("tokenizer", "special_tokens", "config.json")):
        fs.get(remote_path, os.path.join(stage_dir, fname))
        downloaded += 1
if downloaded == 0:
    raise RuntimeError(f"No tokenizer/config files found under {model_id}")
PY
      rm -rf "${LOCAL_TOKENIZER_PATH}"
      mv "${STAGE_TMP}" "${LOCAL_TOKENIZER_PATH}"
      touch "${READY_FILE}"
    fi
  ) 9>"${LOCK_FILE}"
fi

# The recipe module (torchtitan_train.py) is mounted from the chart's ConfigMap.
export PYTHONPATH="/etc/workload-config${PYTHONPATH:+:${PYTHONPATH}}"
TORCHTITAN_MODULE="${TORCHTITAN_MODULE:-torchtitan_train}"
TORCHTITAN_CONFIG="${TORCHTITAN_CONFIG:-llama3_70b_gcsfs}"

echo "Launching TorchTitan distributed training via torchrun..."
echo "  NNODES=${NNODES:-1}"
echo "  GPUS_PER_NODE=${GPUS_PER_NODE:-8}"
echo "  NODE_RANK=${NODE_RANK:-0}"
echo "  RDZV_ENDPOINT=${MASTER_ADDR:-127.0.0.1}:${MASTER_PORT:-29500}"
echo "  TORCHTITAN_FLAVOR=${TORCHTITAN_FLAVOR:-70B}"
echo "  RECIPE=${TORCHTITAN_MODULE}.${TORCHTITAN_CONFIG}"

torchrun \
  --nnodes="${NNODES:-1}" \
  --nproc_per_node="${GPUS_PER_NODE:-8}" \
  --rdzv_id="${JOB_UUID:-torchtitan-job}" \
  --rdzv_backend=c10d \
  --rdzv_endpoint="${MASTER_ADDR:-127.0.0.1}:${MASTER_PORT:-29500}" \
  -m torchtitan.train \
  --module "${TORCHTITAN_MODULE}" \
  --config "${TORCHTITAN_CONFIG}" \
  "$@"

echo "TorchTitan GPU workload completed successfully."
