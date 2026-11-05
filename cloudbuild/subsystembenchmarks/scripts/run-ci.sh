#!/usr/bin/env bash
# Per-PR lightweight subsystem benchmarks (subsystembenchmarks-cloudbuild.yaml, _MODE=ci).
#
# Runs every case in CASES concurrently on this VM, each in its group's own venv and under
# a hard timeout, then exits nonzero if any case failed or timed out. Correctness and
# liveness only: run.py prints throughput, but nothing here gates on it.
#
# The gcsfs under test is this checkout; fsspec and google-cloud-storage come from their
# upstream default branches, so the suite also catches breakage from unreleased changes.
set -euo pipefail

source "$HOME/ssb_cloudbuild.env"
cd "$HOME/gcsfs"

SSB="gcsfs/tests/perf/subsystembenchmarks"
CI_DIR="$SSB/__run__/ci"
VENV_ROOT="$HOME/venvs"
UPSTREAM="$HOME/upstream"
CASE_TIMEOUT_SECONDS=540
# _CI_MODEL_ID (subsystembenchmarks-cloudbuild.yaml) points each CI project at a copy its VM
# service account can read.
CI_MODEL_ID="${CI_MODEL_ID:-gs://huggingface-model-weights/gpt2}"
FSSPEC_GIT="https://github.com/fsspec/filesystem_spec.git"
FSSPEC_REF="master"
# google-cloud-storage moved from the archived googleapis/python-storage into this monorepo.
GCS_SDK_GIT="https://github.com/googleapis/google-cloud-python.git"
GCS_SDK_REF="main"
GCS_SDK_SUBDIR="packages/google-cloud-storage"

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
# Manual gcloud builds submit omits .git, which leaves hatch-vcs without a version. Pass the
# override to the gcsfs install only: exported, it would also stamp the fsspec checkout as
# 0.0.0, which cannot satisfy gcsfs's fsspec floor (hatch-vcs ignores the per-dist
# SETUPTOOLS_SCM_PRETEND_VERSION_FOR_GCSFS form).
GCSFS_BUILD_ENV=()
if [[ ! -d .git ]]; then
  GCSFS_BUILD_ENV=(SETUPTOOLS_SCM_PRETEND_VERSION=0.0.0)
fi
mkdir -p "$CI_DIR" "$VENV_ROOT"

mapfile -t GROUP_LIST < <(printf '%s\n' "${CASES[@]}" | cut -d'|' -f2 | sort -u)

# Upstream checkouts, shared by every venv. fsspec keeps its history (blobless) so hatch-vcs
# can derive a real version from its tags; the monorepo only needs the storage package.
rm -rf "$UPSTREAM"
git clone --quiet --filter=blob:none --branch "$FSSPEC_REF" "$FSSPEC_GIT" "$UPSTREAM/fsspec"
git clone --quiet --depth 1 --filter=blob:none --sparse --branch "$GCS_SDK_REF" \
  "$GCS_SDK_GIT" "$UPSTREAM/gcs-sdk"
git -C "$UPSTREAM/gcs-sdk" sparse-checkout set "$GCS_SDK_SUBDIR"

# Framework requirements first; the venvs are independent, so build them in parallel.
INSTALL_PIDS=()
for group in "${GROUP_LIST[@]}"; do
  venv=$(venv_of "$group")
  (
    python3 -m venv "$venv"
    "$venv/bin/pip" install --quiet --upgrade pip
    "$venv/bin/pip" install --quiet -r "$SSB/$group/requirements.txt"
  ) >"$CI_DIR/install-${group//\//-}.log" 2>&1 &
  INSTALL_PIDS+=("$!")
done
for i in "${!GROUP_LIST[@]}"; do
  if ! wait "${INSTALL_PIDS[$i]}"; then
    echo "ERROR: installing ${GROUP_LIST[$i]} requirements failed:"
    tail -n 50 "$CI_DIR/install-${GROUP_LIST[$i]//\//-}.log"
    exit 1
  fi
done

# The code under test goes in LAST, so its dependency floors win over framework caps
# (datasets caps fsspec below what gcsfs needs; installed first, fsspec gets downgraded and
# gcsfs breaks). Upstream fsspec/storage first, then gcsfs, whose floors they satisfy. One
# venv at a time: each editable install writes gcsfs/_version.py.
VERSIONS="$CI_DIR/versions.txt"
{
  echo "fsspec $FSSPEC_REF $(git -C "$UPSTREAM/fsspec" rev-parse HEAD)"
  echo "google-cloud-storage $GCS_SDK_REF $(git -C "$UPSTREAM/gcs-sdk" rev-parse HEAD)"
} >"$VERSIONS"
for group in "${GROUP_LIST[@]}"; do
  venv=$(venv_of "$group")
  log="$CI_DIR/install-${group//\//-}.log"
  if ! { "$venv/bin/pip" install --quiet "$UPSTREAM/fsspec" "$UPSTREAM/gcs-sdk/$GCS_SDK_SUBDIR" &&
    env "${GCSFS_BUILD_ENV[@]}" "$venv/bin/pip" install --quiet -e .; } >>"$log" 2>&1; then
    echo "ERROR: installing upstream libraries and gcsfs into $group failed:"
    tail -n 50 "$log"
    exit 1
  fi
  # Record where each library under test was imported from, per venv.
  "$venv/bin/python" - "$group" >>"$VERSIONS" <<'EOF'
import importlib.metadata as md
import json
import sys

parts = [sys.argv[1]]
for dist in ("gcsfs", "fsspec", "google-cloud-storage"):
    url = json.loads(md.distribution(dist).read_text("direct_url.json") or "{}").get("url")
    parts.append(f"{dist}={md.version(dist)}@{url or 'pypi'}")
print(" ".join(parts))
EOF
done
cat "$VERSIONS"

# Checkpoint code reads a gs:// model id from /tmp/<dir> (checkpointing/*/common.py).
# Download into $HOME first: the image's snap-packaged gcloud has a private /tmp.
MODEL_DIR=$(basename "${CI_MODEL_ID%/}")
rm -rf "${HOME:?}/$MODEL_DIR" "/tmp/$MODEL_DIR"
gcloud storage cp --recursive "${CI_MODEL_ID%/}" "$HOME/"
mv "$HOME/$MODEL_DIR" "/tmp/$MODEL_DIR"

# --- Run all cases concurrently ------------------------------------------------------
export PYTHONPATH="$HOME/gcsfs"
# On timeout, run_case_with_timeout SIGABRTs every process in the case (run.py, pytest,
# ranks, DataLoader workers); faulthandler makes each dump all thread stacks first.
export PYTHONFAULTHANDLER=1
# Five Ray-based cases each start a local Ray cluster on this VM; keep each object store
# small so they do not compete for /dev/shm.
export RAY_DEFAULT_OBJECT_STORE_MEMORY_PROPORTION=0.05
# Each case gets its own block of torch.distributed rendezvous ports
# (checkpointing/_dist.py:find_free_port); probing OS-assigned free ports raced between
# concurrent cases (EADDRINUSE). Above Ray's default worker ports (10002-19999), below the
# kernel's ephemeral range (32768+).
PORT_BLOCK_START=21000
PORT_BLOCK_SIZE=16

# run_case_with_timeout <timeout_s> <marker> <cmd...>: runs cmd in its own process group
# and returns its exit status, or 124 once timeout_s has passed. `timeout --signal=ABRT`
# would signal the whole group at once, and the concurrent faulthandler dumps interleave
# mid-line in the shared run.log. Instead the group is frozen, then each process gets
# SIGABRT + SIGCONT on its own, so every dump is written alone; SIGKILL ends the rest.
run_case_with_timeout() {
  local timeout_s=$1 marker=$2
  shift 2
  rm -f "$marker"
  # No job control in a script, so setsid execs in place and $! leads the new group.
  setsid "$@" &
  local pid=$!
  (
    sleeper=""
    trap 'kill "$sleeper" 2>/dev/null; exit 0' TERM
    sleep "$timeout_s" &
    sleeper=$!
    wait "$sleeper"
    # From here on finish the dumps even if the case ends and the caller stops us.
    trap '' TERM
    kill -0 "$pid" 2>/dev/null || exit 0
    touch "$marker"
    echo "===== TIMEOUT after ${timeout_s}s: stack dumps follow, one process at a time ====="
    kill -STOP -- "-$pid" 2>/dev/null || true
    for p in $(pgrep -g "$pid" | sort -rn); do
      echo "===== pid $p: $(tr '\0' ' ' <"/proc/$p/cmdline" 2>/dev/null | cut -c1-200)"
      kill -ABRT "$p" 2>/dev/null || true
      kill -CONT "$p" 2>/dev/null || true
      sleep 0.5
    done
    kill -KILL -- "-$pid" 2>/dev/null || true
  ) &
  local watchdog=$!
  local rc=0
  wait "$pid" || rc=$?
  kill "$watchdog" 2>/dev/null || true
  wait "$watchdog" 2>/dev/null || true
  if [[ -e "$marker" ]]; then
    return 124
  fi
  return "$rc"
}

CASE_PIDS=()
for i in "${!CASES[@]}"; do
  IFS='|' read -r name group bucket_type filter <<<"${CASES[$i]}"
  out="$CI_DIR/$name"
  mkdir -p "$out"
  run_case_with_timeout "$CASE_TIMEOUT_SECONDS" "$out/timed_out" \
    env GCSFS_SUBSYSTEM_CONFIG="configs.ci.yaml" \
    GCSFS_SUBSYSTEM_PORT_BASE="$((PORT_BLOCK_START + i * PORT_BLOCK_SIZE))" \
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
    124) result="TIMEOUT (${CASE_TIMEOUT_SECONDS}s)" ;;
    *) result="FAIL (exit $rc)" ;;
  esac
  if [[ "$rc" != 0 ]]; then
    FAILED_CASES+=("$name")
  fi
  printf '%-26s %s\n' "$name" "$result" >>"$SUMMARY"
done

for name in "${FAILED_CASES[@]}"; do
  log="$CI_DIR/$name/run.log"
  if [[ -e "$CI_DIR/$name/timed_out" ]]; then
    # Every process's stack dump; a 2-rank case writes a few hundred lines.
    echo "===== $name: $log from the timeout on ====="
    sed -n '/^===== TIMEOUT after/,$p' "$log" || true
  else
    echo "===== $name: last 200 lines of $log ====="
    tail -n 200 "$log" || true
  fi
done
echo "===== Per-PR subsystem benchmark summary ====="
cat "$SUMMARY"
if ((${#FAILED_CASES[@]})); then
  exit 1
fi
