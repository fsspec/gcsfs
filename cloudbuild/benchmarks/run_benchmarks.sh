#!/bin/bash
# Runs microbenchmarks on the PR and base branches and compares them.
#
# Executed on the benchmark VM (see cloudbuild/benchmarks/ci-perf-cloudbuild.yaml),
# which places the PR checkout in ~/gcsfs-pr and the base checkout in ~/gcsfs-base.
#
# Keeping this logic in a script rather than inline YAML avoids the triple escaping
# (Cloud Build substitution -> local shell -> remote shell) that inline heredocs need.
#
# Environment:
#   BENCHMARK_GROUPS  Space-separated group names. Empty runs every group.
#   BENCHMARK_CONFIG  Comma-separated scenario filter. Empty runs every scenario.
#   CHUNK_SIZES_MB    Comma-separated IO sizes in MB. Empty uses configs.yaml.
#   BUCKET_TYPES      Space-separated subset of "regional zonal hns".
#   REGIONAL_BUCKET / ZONAL_BUCKET / HNS_BUCKET   Bucket names.
#   REUSE_FILES       "true" (default) shares read fixture files between cases.
#   BASE_BRANCH       Base branch name, used for report labelling only.
#   THRESHOLD         Regression threshold percentage.
#   REPORT_PATH       Where to write the markdown report.
set -euo pipefail

BENCH_DIR="gcsfs/tests/perf/microbenchmarks"
BENCHMARK_GROUPS="${BENCHMARK_GROUPS:-}"
BENCHMARK_CONFIG="${BENCHMARK_CONFIG:-}"
BUCKET_TYPES="${BUCKET_TYPES:-regional}"
BASE_BRANCH="${BASE_BRANCH:-main}"
THRESHOLD="${THRESHOLD:-10.0}"
REPORT_PATH="${REPORT_PATH:-/tmp/perf_report.md}"

export TMPDIR=/mnt/ramdisk

# Build the argument list once. A bucket type that was not requested contributes no
# flag, and run.py skips any scenario whose bucket type is unavailable.
#
# These are if-blocks rather than `[ ... ] && RUN_ARGS+=(...)` because under `set -e`
# a false test as the last command of the line aborts the script.
RUN_ARGS=()
if [ -n "${BENCHMARK_CONFIG}" ]; then
  RUN_ARGS+=("--config=${BENCHMARK_CONFIG}")
fi
if [ -n "${CHUNK_SIZES_MB:-}" ]; then
  RUN_ARGS+=("--chunk-sizes=${CHUNK_SIZES_MB}")
fi
if [[ " ${BUCKET_TYPES} " == *" regional "* ]]; then
  RUN_ARGS+=("--regional-bucket=${REGIONAL_BUCKET:?regional requested but REGIONAL_BUCKET unset}")
fi
if [[ " ${BUCKET_TYPES} " == *" zonal "* ]]; then
  RUN_ARGS+=("--zonal-bucket=${ZONAL_BUCKET:?zonal requested but ZONAL_BUCKET unset}")
fi
if [[ " ${BUCKET_TYPES} " == *" hns "* ]]; then
  RUN_ARGS+=("--hns-bucket=${HNS_BUCKET:?hns requested but HNS_BUCKET unset}")
fi
RUN_ARGS+=("--log=true" "--log-level=INFO")
# Read benchmarks never mutate their fixture files, so cases that need identically
# sized files share one upload instead of rebuilding a 20 GB object each time.
if [ "${REUSE_FILES:-true}" = "true" ]; then
  RUN_ARGS+=("--reuse-files")
fi

# The build log is the only feedback channel a PR author has, so state exactly what
# this run resolved to rather than leaving it implied by the substitutions.
echo "=== Effective benchmark selection ==="
echo "  groups:      ${BENCHMARK_GROUPS:-<all>}"
echo "  scenarios:   ${BENCHMARK_CONFIG:-<all in group>}"
echo "  io sizes MB: ${CHUNK_SIZES_MB:-<from configs.yaml>}"
echo "  buckets:     ${BUCKET_TYPES}"
echo "  reuse files: ${REUSE_FILES:-true}"
echo "  base branch: ${BASE_BRANCH}"
echo "  threshold:   ${THRESHOLD}%"
echo "  run.py args: ${RUN_ARGS[*]}"
echo "====================================="

# Runs the selected groups in the current directory and prints a comma-separated
# list of every results.json produced. Progress goes to stderr so that only the
# path list lands on stdout for command substitution.
run_groups() {
  if [ -z "${BENCHMARK_GROUPS}" ]; then
    # Omitting --group makes run.py collect every group in a single pytest session.
    echo "--- running all benchmark groups ---" >&2
    "${PYTHON}" "${BENCH_DIR}/run.py" "${RUN_ARGS[@]}" >&2
  else
    for group in ${BENCHMARK_GROUPS}; do
      echo "--- running benchmark group: ${group} ---" >&2
      "${PYTHON}" "${BENCH_DIR}/run.py" --group="${group}" "${RUN_ARGS[@]}" >&2
    done
  fi

  # Collect every run, not just the newest. compare.py merges them by benchmark
  # name, so a per-group loop still yields one complete result set.
  # `|| true` keeps a no-match `ls` from tripping `set -e` before the check below.
  local paths
  paths=$(ls -1d "$(pwd)/${BENCH_DIR}"/__run__/*/results.json 2>/dev/null | paste -sd, - || true)
  if [ -z "${paths}" ]; then
    echo "ERROR: no results.json produced in $(pwd)" >&2
    return 1
  fi
  echo "${paths}"
}

# Creates a venv in the current tree and sets PYTHON to its interpreter.
# Each branch gets its own venv, and the interpreter is named explicitly rather
# than activated, so the two never shadow each other.
setup_env() {
  python3 -m venv env
  PYTHON="$(pwd)/env/bin/python"
  "${PYTHON}" -m pip install -q -e . -r "${BENCH_DIR}/requirements.txt"
}

echo "=== Benchmarking PR branch ==="
cd ~/gcsfs-pr
setup_env
PR_PYTHON="${PYTHON}"
PR_JSON_PATHS=$(run_groups)

echo "=== Benchmarking base branch (${BASE_BRANCH}) ==="
cd ~/gcsfs-base
setup_env

# Pin base to the PR's benchmark parameters. Without this, a PR that tunes rounds
# or file sizes would be measured against different base parameters, silently
# invalidating the comparison.
for cfg in ~/gcsfs-pr/"${BENCH_DIR}"/*/configs.yaml; do
  group=$(basename "$(dirname "${cfg}")")
  cp "${cfg}" ~/gcsfs-base/"${BENCH_DIR}/${group}/configs.yaml"
done

BASE_JSON_PATHS=$(run_groups)

echo "=== Comparing performance (threshold: ${THRESHOLD}%) ==="
cd ~/gcsfs-pr
"${PR_PYTHON}" "${BENCH_DIR}/compare.py" \
  "${BASE_JSON_PATHS}" \
  "${PR_JSON_PATHS}" \
  --threshold="${THRESHOLD}" \
  --base-ref="${BASE_BRANCH}" \
  --pr-ref="PR" \
  --output-markdown="${REPORT_PATH}"
