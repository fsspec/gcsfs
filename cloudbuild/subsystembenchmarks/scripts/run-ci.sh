#!/usr/bin/env bash
# Per-PR lightweight subsystem benchmarks (subsystembenchmarks-cloudbuild.yaml, _MODE=ci).
#
# Runs every case in CASES concurrently on this VM, each in its group's own venv and under
# a hard timeout, then exits nonzero if any case failed or timed out. Correctness and
# liveness only: run.py prints throughput, but nothing here gates on it.
set -euo pipefail

source "$HOME/ssb_cloudbuild.env"
cd "$HOME/gcsfs"

SSB="gcsfs/tests/perf/subsystembenchmarks"
CI_DIR="$SSB/__run__/ci"
VENV_ROOT="$HOME/venvs"
CASE_TIMEOUT_SECONDS=540
CI_MODEL_ID="gs://huggingface-model-weights/gpt2"

# name|group|bucket type|pytest -k filter (empty = none). Rapid = zonal, Standard = regional.
# Guarded by gcsfs/tests/perf/subsystembenchmarks/tests/test_ci_suite.py.
CASES=(
  "dl-hf-zonal|dataloading/huggingface_datasets|zonal|"
  "dl-ray-hns|dataloading/ray_data|hns|"
  "dl-wds-regional|dataloading/webdataset|regional|"
  "ckpt-read-pl-zonal|checkpointing/pytorch_lightning|zonal|load"
  "ckpt-read-pl-regional|checkpointing/pytorch_lightning|regional|load"
  "ckpt-read-ray-zonal|checkpointing/ray_pytorch|zonal|load"
  "ckpt-read-ray-regional|checkpointing/ray_pytorch|regional|load"
  "ckpt-write-pl-zonal|checkpointing/pytorch_lightning|zonal|save"
  "ckpt-write-pl-regional|checkpointing/pytorch_lightning|regional|save"
  "ckpt-write-ray-zonal|checkpointing/ray_pytorch|zonal|save"
  "ckpt-write-ray-regional|checkpointing/ray_pytorch|regional|save"
)

venv_of() { echo "$VENV_ROOT/${1//\//-}"; }

# --- Setup ---------------------------------------------------------------------------
sudo apt-get update >/dev/null
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y \
  python3-pip python3-venv git >/dev/null
# Manual gcloud builds submit omits .git, which leaves hatch-vcs without a version.
if [[ ! -d .git ]]; then
  export SETUPTOOLS_SCM_PRETEND_VERSION="0.0.0"
fi
mkdir -p "$CI_DIR" "$VENV_ROOT"

mapfile -t GROUP_LIST < <(printf '%s\n' "${CASES[@]}" | cut -d'|' -f2 | sort -u)

# Editable installs run one at a time: each one writes gcsfs/_version.py (hatch-vcs).
for group in "${GROUP_LIST[@]}"; do
  venv=$(venv_of "$group")
  python3 -m venv "$venv"
  "$venv/bin/pip" install --quiet --upgrade pip
  "$venv/bin/pip" install --quiet -e .
done

# Framework requirements are independent per venv, so install them in parallel.
INSTALL_PIDS=()
for group in "${GROUP_LIST[@]}"; do
  "$(venv_of "$group")/bin/pip" install --quiet -r "$SSB/$group/requirements.txt" \
    >"$CI_DIR/install-${group//\//-}.log" 2>&1 &
  INSTALL_PIDS+=("$!")
done
for i in "${!GROUP_LIST[@]}"; do
  if ! wait "${INSTALL_PIDS[$i]}"; then
    echo "ERROR: installing ${GROUP_LIST[$i]} requirements failed:"
    tail -n 50 "$CI_DIR/install-${GROUP_LIST[$i]//\//-}.log"
    exit 1
  fi
done

# Checkpoint code reads a gs:// model id from /tmp/<dir> (checkpointing/*/common.py).
# Download into $HOME first: the image's snap-packaged gcloud has a private /tmp.
rm -rf "$HOME/gpt2" /tmp/gpt2
gcloud storage cp --recursive "$CI_MODEL_ID" "$HOME/"
mv "$HOME/gpt2" /tmp/gpt2

# --- Run all cases concurrently ------------------------------------------------------
export PYTHONPATH="$HOME/gcsfs"
# On timeout, SIGABRT goes to the case's whole process group; faulthandler makes every
# Python process in it (pytest, ranks, DataLoader workers) dump all thread stacks first.
export PYTHONFAULTHANDLER=1
# Five Ray-based cases each start a local Ray cluster on this VM; keep each object store
# small so they do not compete for /dev/shm.
export RAY_DEFAULT_OBJECT_STORE_MEMORY_PROPORTION=0.05

CASE_PIDS=()
for spec in "${CASES[@]}"; do
  IFS='|' read -r name group bucket_type filter <<<"$spec"
  out="$CI_DIR/$name"
  mkdir -p "$out"
  config="configs.yaml"
  if [[ "$group" == dataloading/* ]]; then
    config="configs.ci.yaml"
  fi
  GCSFS_SUBSYSTEM_CONFIG="$config" \
    timeout --signal=ABRT --kill-after=30 "$CASE_TIMEOUT_SECONDS" \
    "$(venv_of "$group")/bin/python" "$SSB/run.py" \
    "--group=$group" \
    "--sweep-axes=baseline" \
    "--filter=$filter" \
    "--bucket-prefix=$BUCKET_PREFIX" \
    "--bucket-type=$bucket_type" \
    "--project=$PROJECT_ID" \
    "--location=$REGION" \
    "--zone=$ZONE" \
    "--model-id=$CI_MODEL_ID" \
    "--results-dir=$out" \
    "--amplification-wait=0" \
    "--amplification-retry-wait=0" \
    >"$out/run.log" 2>&1 &
  CASE_PIDS+=("$!")
done

# --- Collect results -----------------------------------------------------------------
SUMMARY="$CI_DIR/summary.txt"
FAILED_CASES=()
printf '%-26s %s\n' "CASE" "RESULT" >"$SUMMARY"
for i in "${!CASES[@]}"; do
  name="${CASES[$i]%%|*}"
  rc=0
  wait "${CASE_PIDS[$i]}" || rc=$?
  case "$rc" in
    0) result="PASS" ;;
    124 | 137) result="TIMEOUT (${CASE_TIMEOUT_SECONDS}s)" ;;
    *) result="FAIL (exit $rc)" ;;
  esac
  if [[ "$rc" != 0 ]]; then
    FAILED_CASES+=("$name")
  fi
  printf '%-26s %s\n' "$name" "$result" >>"$SUMMARY"
done

for name in "${FAILED_CASES[@]}"; do
  echo "===== $name: last 50 lines of $CI_DIR/$name/run.log ====="
  tail -n 50 "$CI_DIR/$name/run.log" || true
done
echo "===== Per-PR subsystem benchmark summary ====="
cat "$SUMMARY"
if ((${#FAILED_CASES[@]})); then
  exit 1
fi
