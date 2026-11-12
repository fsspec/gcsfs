#!/bin/bash
set -e
source env/bin/activate

# Common Exports
export STORAGE_EMULATOR_HOST=https://storage.googleapis.com
export GCSFS_TEST_PROJECT=${PROJECT_ID}
export GCSFS_TEST_KMS_KEY=projects/${PROJECT_ID}/locations/${REGION}/keyRings/${KEY_RING}/cryptoKeys/${KEY_NAME}
export GOOGLE_CLOUD_PROJECT=${PROJECT_ID}

# Pytest Arguments
ARGS=(
  -vv
  -s
  "--log-format=%(asctime)s %(levelname)s %(message)s"
  "--log-date-format=%H:%M:%S"
  --color=no
  --durations=50
)

PYTEST_XDIST_WORKERS="${PYTEST_XDIST_WORKERS:-1}"
if ! [[ "${PYTEST_XDIST_WORKERS}" =~ ^[0-9]+$ ]] || (( PYTEST_XDIST_WORKERS <= 0 )); then
  PYTEST_XDIST_WORKERS=1
fi
ARGS+=(-n "${PYTEST_XDIST_WORKERS}")

# Full pytest output goes to files instead of the build log. GitHub shows only the first ~65k chars
# of the Cloud Build log, so the test-report step prints a summary from the JUnit XML before these logs.
RESULTS_DIR="${HOME}/test-results"
RESULTS_LOG="${RESULTS_DIR}/${TEST_SUITE}.log"
mkdir -p "${RESULTS_DIR}"
ARGS+=("--junitxml=${RESULTS_DIR}/${TEST_SUITE}.xml")

# While pytest runs, print the tests still in progress every HEARTBEAT_SECS. A hung test is failed by
# pytest-timeout and a killed pytest leaves its log tail for test-report, but if the whole build times out
# (or the VM is lost) test-report never runs, so these lines keep a stuck test visible in the step log.
HEARTBEAT_SECS="${HEARTBEAT_SECS:-300}"
if ! [[ "${HEARTBEAT_SECS}" =~ ^[0-9]+$ ]] || (( HEARTBEAT_SECS <= 0 )); then
  HEARTBEAT_SECS=300
fi

in_progress_tests() {
  # With -vv, xdist logs "<nodeid>" when a test starts and "[gwN] <OUTCOME> <nodeid>" when it finishes.
  awk '
    { sub(/ <- .*/, ""); sub(/[ \t]+$/, "") }
    /^\[gw[0-9]+\] [A-Z]+ / { sub(/^\[gw[0-9]+\] [A-Z]+ /, ""); delete running[$0]; next }
    /^[^ ]+\.py::/ { running[$0] = 1 }
    END { for (t in running) printf "%s%s", (n++ ? ", " : ""), t; if (!n) printf "none" }
  ' "${RESULTS_LOG}" 2>/dev/null | cut -c1-500
}

heartbeat() {
  local elapsed=0
  # Sleep in 1s steps so stopping the heartbeat never leaves a long sleep holding the ssh session open.
  while sleep 1; do
    elapsed=$((elapsed + 1))
    if (( elapsed % HEARTBEAT_SECS == 0 )); then
      echo "--- ${TEST_SUITE}: still running after $((elapsed / 60))m; in progress: $(in_progress_tests) ---"
    fi
  done
}

run_pytest() {
  local status=0
  heartbeat &
  local heartbeat_pid=$!
  pytest "$@" > "${RESULTS_LOG}" 2>&1 || status=$?
  { kill "${heartbeat_pid}" && wait "${heartbeat_pid}"; } 2>/dev/null
  return "${status}"
}

raise_fd_limit() {
  # DirectPath gRPC xDS channels can exceed the default 4096 FD limit.
  local target=65536
  local soft hard
  soft=$(ulimit -Sn)
  hard=$(ulimit -Hn)
  if [[ "${soft}" == "unlimited" || "${soft}" -ge "${target}" ]]; then
    return 0
  fi
  if [[ "${hard}" != "unlimited" && "${hard}" -lt "${target}" ]]; then
    target="${hard}"
  fi
  ulimit -Sn "${target}"
}
STATUS=0

# Core test deselections shared by buckets whose writes go through the gRPC
# appendable writer (Zonal and Rapid Cache Ultra).
# 1. KMS & Metadata Support: gRPC appendable writes do not forward 'kmsKeyName',
#    'contentType', or custom metadata.
# 2. Copy/Merge: Server-side Rewrite/Compose are not supported on RAPID objects.
# 3. Write/Flush Mechanics: ZonalFile flushes directly to GCS via the SDK writer
#    rather than GCSFile's HTTP resumable buffer, does not support transaction
#    discard or x-mode MPU, and bypasses HTTP upload_chunk.
# 4. Auth/Regional/Requester-pays: test_sign needs a private key, test_mv_file_cache
#    targets standard HTTP buckets, and requester-pays buckets are not provisioned
#    for Rapid suites.
RAPID_CORE_DESELECTS=(
  "--deselect=gcsfs/tests/test_core.py::test_simple_upload_with_kms"
  "--deselect=gcsfs/tests/test_core.py::test_large_upload_with_kms"
  "--deselect=gcsfs/tests/test_core.py::test_multi_upload_with_kms"
  "--deselect=gcsfs/tests/test_core.py::test_multi_upload"
  "--deselect=gcsfs/tests/test_core.py::test_fixed_key_metadata"
  "--deselect=gcsfs/tests/test_core.py::test_content_type_set"
  "--deselect=gcsfs/tests/test_core.py::test_content_type_default"
  "--deselect=gcsfs/tests/test_core.py::test_content_type_guess"
  "--deselect=gcsfs/tests/test_core.py::test_content_type_put_guess"
  "--deselect=gcsfs/tests/test_core.py::test_attrs"
  "--deselect=gcsfs/tests/test_core.py::test_copy"
  "--deselect=gcsfs/tests/test_core.py::test_copy_recursive"
  "--deselect=gcsfs/tests/test_core.py::test_copy_errors"
  "--deselect=gcsfs/tests/test_core.py::test_cp_directory_recursive"
  "--deselect=gcsfs/tests/test_core.py::test_cp_two_files"
  "--deselect=gcsfs/tests/test_core.py::test_copy_cache_invalidated"
  "--deselect=gcsfs/tests/test_core.py::test_merge"
  "--deselect=gcsfs/tests/test_core.py::test_flush"
  "--deselect=gcsfs/tests/test_core.py::test_write_blocks"
  "--deselect=gcsfs/tests/test_core.py::test_write_blocks2"
  "--deselect=gcsfs/tests/test_core.py::test_transaction"
  "--deselect=gcsfs/tests/test_core.py::test_array"
  "--deselect=gcsfs/tests/test_core.py::test_sign"
  "--deselect=gcsfs/tests/test_core.py::test_mv_file_cache"
  "--deselect=gcsfs/tests/test_core.py::test_write_x_mpu"
  "--deselect=gcsfs/tests/test_core.py::test_put_file_resumable_upload_cleanup_on_chunk_failure"
  "--deselect=gcsfs/tests/test_core.py::test_requester_pays_fails_without_user_project"
)

echo "--- Running Test Suite: ${TEST_SUITE} ---"

case "$TEST_SUITE" in
  "standard")
    export GCSFS_TEST_BUCKET="gcsfs-test-standard-${SHORT_BUILD_ID}"
    export GCSFS_TEST_VERSIONED_BUCKET="gcsfs-test-versioned-${SHORT_BUILD_ID}"
    export GCSFS_TEST_REQ_PAYS_BUCKET="gcsfs-test-standard-req-pay-${SHORT_BUILD_ID}"
    run_pytest "${ARGS[@]}" gcsfs/ --deselect gcsfs/tests/test_core.py::test_sign || STATUS=$?
    ;;

  "zonal")
    export GCSFS_TEST_BUCKET="gcsfs-test-standard-for-zonal-${SHORT_BUILD_ID}"
    export GCSFS_ZONAL_TEST_BUCKET="gcsfs-test-zonal-${SHORT_BUILD_ID}"
    export GCSFS_HNS_TEST_BUCKET="gcsfs-test-zonal-${SHORT_BUILD_ID}"
    export GCSFS_HNS_TEST_REQ_PAYS_BUCKET="gcsfs-test-zonal-${SHORT_BUILD_ID}"
    raise_fd_limit
    export GCSFS_RUN_HNS_TESTS="true"
    export GCSFS_RUN_RAPID_TESTS="true"
    export GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT='true'
    # Excludes tests related to requster pays as Zonal buckets do not support requester pays feature
    run_pytest "${ARGS[@]}" \
      gcsfs/tests/test_zonal.py \
      gcsfs/tests/test_zonal_file.py \
      gcsfs/tests/test_async.py \
      gcsfs/tests/test_hns.py \
      --deselect gcsfs/tests/test_hns.py::TestExtendedGcsFileSystemHnsRequesterPays::test_hns_mkdir_fails_without_quota_project \
      --deselect gcsfs/tests/test_hns.py::TestExtendedGcsFileSystemHnsRequesterPays::test_hns_bucket_type_detection_with_req_pays || STATUS=$?
    ;;

  "hns")
    export GCSFS_TEST_BUCKET="gcsfs-test-hns-${SHORT_BUILD_ID}"
    export GCSFS_ZONAL_TEST_BUCKET="gcsfs-test-hns-${SHORT_BUILD_ID}"
    export GCSFS_HNS_TEST_BUCKET="gcsfs-test-hns-${SHORT_BUILD_ID}"
    export GCSFS_TEST_REQ_PAYS_BUCKET="gcsfs-test-hns-req-pay-${SHORT_BUILD_ID}"
    export GCSFS_HNS_TEST_REQ_PAYS_BUCKET="gcsfs-test-hns-req-pay-${SHORT_BUILD_ID}"
    export GCSFS_RUN_HNS_TESTS="true"
    export GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT='true'
    # Excludes tests that are not applicable to HNS buckets:
    # - test_zonal.py, test_zonal_file.py: Zonal bucket specific tests which won't work on HNS bucket.
    # - test_zonal_unit.py: Unit tests for zonal bucket features.
    # - test_flat_versioned.py: HNS buckets do not support versioning.
    # - test_core.py::test_sign: Current Cloud Build auth setup does not support this.
    # - test_core.py::test_mv_file_cache: Integration test only applicable for regional buckets.
    # - test_core.py::test_rm_wildcards_non_recursive: HNS buckets have different behavior for non-recursive wildcard deletion.
    run_pytest "${ARGS[@]}" gcsfs/ \
      --deselect gcsfs/tests/test_zonal.py \
      --deselect gcsfs/tests/test_zonal_file.py \
      --deselect gcsfs/tests/test_zonal_unit.py \
      --deselect gcsfs/tests/test_flat_versioned.py \
      --deselect gcsfs/tests/test_core.py::test_sign \
      --deselect gcsfs/tests/test_core.py::test_mv_file_cache \
      --deselect gcsfs/tests/test_core.py::test_rm_wildcards_non_recursive || STATUS=$?
    ;;

  "zonal-core")
    export GCSFS_TEST_BUCKET="gcsfs-test-zonal-core-${SHORT_BUILD_ID}"
    export GCSFS_TEST_REQ_PAYS_BUCKET="gcsfs-test-zonal-core-${SHORT_BUILD_ID}"
    export GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT='true'
    raise_fd_limit

    ZONAL_DESELECTS=(
      "${RAPID_CORE_DESELECTS[@]}"
      # HNS buckets have different behavior for non-recursive wildcard deletion.
      "--deselect=gcsfs/tests/test_core.py::test_rm_wildcards_non_recursive"
    )

    run_pytest "${ARGS[@]}" "${ZONAL_DESELECTS[@]}" gcsfs/tests/test_core.py || STATUS=$?
    ;;

  "rcu")
    export GCSFS_TEST_BUCKET="gcsfs-test-rcu-${SHORT_BUILD_ID}"
    export GCSFS_ZONAL_TEST_BUCKET="gcsfs-test-rcu-${SHORT_BUILD_ID}"
    export GCSFS_TEST_REQ_PAYS_BUCKET="gcsfs-test-rcu-${SHORT_BUILD_ID}"
    export GCSFS_CROSS_ZONE_RCU_TEST_BUCKET="gcsfs-test-rcu-cross-zone-${SHORT_BUILD_ID}"
    raise_fd_limit
    export GCSFS_RUN_RAPID_TESTS="true"
    export GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT='true'

    # Default RCU writes use HTTP (STANDARD), so HTTP write/metadata/copy/merge
    # tests run directly; skip_if_grpc_read still skips HTTP read-mechanics tests.
    run_pytest "${ARGS[@]}" \
      --deselect=gcsfs/tests/test_core.py::test_sign \
      --deselect=gcsfs/tests/test_core.py::test_requester_pays_fails_without_user_project \
      gcsfs/tests/test_rcu.py \
      gcsfs/tests/test_zonal.py \
      gcsfs/tests/test_zonal_file.py \
      gcsfs/tests/test_core.py || STATUS=$?
    ;;

  "hns-rcu")
    export GCSFS_TEST_BUCKET="gcsfs-test-hns-rcu-${SHORT_BUILD_ID}"
    export GCSFS_ZONAL_TEST_BUCKET="gcsfs-test-hns-rcu-${SHORT_BUILD_ID}"
    export GCSFS_HNS_TEST_BUCKET="gcsfs-test-hns-rcu-${SHORT_BUILD_ID}"
    export GCSFS_HNS_TEST_REQ_PAYS_BUCKET="gcsfs-test-hns-rcu-${SHORT_BUILD_ID}"
    raise_fd_limit
    export GCSFS_RUN_HNS_TESTS="true"
    export GCSFS_RUN_RAPID_TESTS="true"
    export GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT='true'

    run_pytest "${ARGS[@]}" \
      gcsfs/tests/test_rcu.py \
      gcsfs/tests/test_zonal.py \
      gcsfs/tests/test_zonal_file.py \
      gcsfs/tests/test_async.py \
      gcsfs/tests/test_hns.py \
      --deselect gcsfs/tests/test_hns.py::TestExtendedGcsFileSystemHnsRequesterPays::test_hns_mkdir_fails_without_quota_project \
      --deselect gcsfs/tests/test_hns.py::TestExtendedGcsFileSystemHnsRequesterPays::test_hns_bucket_type_detection_with_req_pays || STATUS=$?
    ;;
esac

# Pytest's final "=== N passed, M skipped in Xs ===" line, so the step log still shows the counts.
RESULT_LINE=$(grep -E '^=+ .* in [0-9.]+s' "${RESULTS_LOG}" 2>/dev/null | tail -n 1 | sed -E 's/^=+ | =+$//g')
echo "--- ${TEST_SUITE}: pytest exit ${STATUS} ${RESULT_LINE} ---"
exit "${STATUS}"
