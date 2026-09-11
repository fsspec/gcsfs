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
STATUS=0

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
    ulimit -n 4096
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
    ulimit -n 4096

    # Zonal Core Deselections
    # -----------------------
    # 1. KMS & Metadata Support: Zonal buckets do not support uploading with
    # 'kmsKeyName', 'contentType', or custom metadata.
    ZONAL_DESELECTS=(
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
    )

    # 2. Copy/Move/Merge: Not implemented for Zonal (requires _cp_file or Compose).
    ZONAL_DESELECTS+=(
      "--deselect=gcsfs/tests/test_core.py::test_copy"
      "--deselect=gcsfs/tests/test_core.py::test_copy_recursive"
      "--deselect=gcsfs/tests/test_core.py::test_copy_errors"
      "--deselect=gcsfs/tests/test_core.py::test_cp_directory_recursive"
      "--deselect=gcsfs/tests/test_core.py::test_cp_two_files"
      "--deselect=gcsfs/tests/test_core.py::test_copy_cache_invalidated"
      "--deselect=gcsfs/tests/test_core.py::test_merge"
    )

    # 3. Write/Flush Mechanics:
    # - test_flush fails because ZonalFile.flush flushes directly to GCS whereas
    # GCSFile.flush defers write on small block (<blocksize)
    # - test_write_blocks/2 fail since it checks buffer location and zonal write
    # uses SDK buffer directly, not the GCSFile buffer
    # - test_transaction fails since discard is not supported in Zonal
    # - test_array fails due to CRC32C TypeError with array objects.
    # - test_sign fails because it requires a private key
    # - test_mv_file_cache: Integration test only applicable for regional buckets.
    # - test_rm_wildcards_non_recursive: HNS buckets have different behavior for non-recursive wildcard deletion.
    # - test_write_x_mpu fails because zonal files do not support x mode.
    # - test_put_file_resumable_upload_cleanup_on_chunk_failure: Zonal uploads use gRPC and bypass upload_chunk, so mock is not triggered.
    ZONAL_DESELECTS+=(
      "--deselect=gcsfs/tests/test_core.py::test_flush"
      "--deselect=gcsfs/tests/test_core.py::test_write_blocks"
      "--deselect=gcsfs/tests/test_core.py::test_write_blocks2"
      "--deselect=gcsfs/tests/test_core.py::test_transaction"
      "--deselect=gcsfs/tests/test_core.py::test_array"
      "--deselect=gcsfs/tests/test_core.py::test_sign"
      "--deselect=gcsfs/tests/test_core.py::test_mv_file_cache"
      "--deselect=gcsfs/tests/test_core.py::test_rm_wildcards_non_recursive"
      "--deselect=gcsfs/tests/test_core.py::test_write_x_mpu"
      "--deselect=gcsfs/tests/test_core.py::test_put_file_resumable_upload_cleanup_on_chunk_failure"
    )

    # Zonal buckets do not support the requester pays feature
    ZONAL_DESELECTS+=(
      "--deselect=gcsfs/tests/test_core.py::test_requester_pays_fails_without_user_project"
    )

    run_pytest "${ARGS[@]}" "${ZONAL_DESELECTS[@]}" gcsfs/tests/test_core.py || STATUS=$?
    ;;
esac

# Pytest's final "=== N passed, M skipped in Xs ===" line, so the step log still shows the counts.
RESULT_LINE=$(grep -E '^=+ .* in [0-9.]+s' "${RESULTS_LOG}" 2>/dev/null | tail -n 1 | sed -E 's/^=+ | =+$//g')
echo "--- ${TEST_SUITE}: pytest exit ${STATUS} ${RESULT_LINE} ---"
exit "${STATUS}"
