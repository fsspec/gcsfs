#!/bin/bash
set -e
source env/bin/activate

# Temporary workaround: Disable mTLS for GCE Metadata Server discovery to avoid
# transport and SSL verification errors on mTLS-enabled VMs. This ensures
# stability across all Google SDKs while library-level mTLS fixes are finalized.
# This is added to support the versioned tests
export GCE_METADATA_MTLS_MODE=none

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
)

echo "--- Running Test Suite: ${TEST_SUITE} ---"

case "$TEST_SUITE" in
  "standard")
    export GCSFS_TEST_BUCKET="gcsfs-test-standard-${SHORT_BUILD_ID}"
    export GCSFS_TEST_VERSIONED_BUCKET="gcsfs-test-versioned-${SHORT_BUILD_ID}"
    export GCSFS_TEST_REQ_PAYS_BUCKET="gcsfs-test-standard-req-pay-${SHORT_BUILD_ID}"
    pytest "${ARGS[@]}" gcsfs/ --deselect gcsfs/tests/test_core.py::test_sign
    ;;

  "zonal")
    export GCSFS_TEST_BUCKET="gcsfs-test-standard-for-zonal-${SHORT_BUILD_ID}"
    export GCSFS_ZONAL_TEST_BUCKET="gcsfs-test-zonal-${SHORT_BUILD_ID}"
    export GCSFS_HNS_TEST_BUCKET="gcsfs-test-zonal-${SHORT_BUILD_ID}"
    export GCSFS_HNS_TEST_REQ_PAYS_BUCKET="gcsfs-test-zonal-${SHORT_BUILD_ID}"
    ulimit -n 4096
    export GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT='true'
    # Excludes tests related to requster pays as Zonal buckets do not support requester pays feature
    pytest "${ARGS[@]}" \
      gcsfs/tests/test_extended_gcsfs.py \
      gcsfs/tests/test_zonal_file.py \
      gcsfs/tests/integration/test_async_gcsfs.py \
      gcsfs/tests/integration/test_extended_hns.py \
      --deselect gcsfs/tests/integration/test_extended_hns.py::TestExtendedGcsFileSystemHnsRequesterPays::test_hns_mkdir_fails_without_quota_project \
      --deselect gcsfs/tests/integration/test_extended_hns.py::TestExtendedGcsFileSystemHnsRequesterPays::test_hns_bucket_type_detection_with_req_pays
    ;;

  "hns")
    export GCSFS_TEST_BUCKET="gcsfs-test-hns-${SHORT_BUILD_ID}"
    export GCSFS_ZONAL_TEST_BUCKET="gcsfs-test-hns-${SHORT_BUILD_ID}"
    export GCSFS_HNS_TEST_BUCKET="gcsfs-test-hns-${SHORT_BUILD_ID}"
    export GCSFS_TEST_REQ_PAYS_BUCKET="gcsfs-test-hns-req-pay-${SHORT_BUILD_ID}"
    export GCSFS_HNS_TEST_REQ_PAYS_BUCKET="gcsfs-test-hns-req-pay-${SHORT_BUILD_ID}"
    export GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT='true'
    # Excludes tests that are not applicable to HNS buckets:
    # - test_extended_gcsfs.py, test_zonal_file.py: Zonal bucket specific tests which won't work on HNS bucket.
    # - test_extended_gcsfs_unit.py: Unit tests for zonal bucket features.
    # - test_core_versioned.py: HNS buckets do not support versioning.
    # - test_core.py::test_sign: Current Cloud Build auth setup does not support this.
    # - test_core.py::test_mv_file_cache: Integration test only applicable for regional buckets.
    # - test_core.py::test_rm_wildcards_non_recursive: HNS buckets have different behavior for non-recursive wildcard deletion.
    pytest "${ARGS[@]}" gcsfs/ \
      --deselect gcsfs/tests/test_extended_gcsfs.py \
      --deselect gcsfs/tests/test_zonal_file.py \
      --deselect gcsfs/tests/test_extended_gcsfs_unit.py \
      --deselect gcsfs/tests/test_core_versioned.py \
      --deselect gcsfs/tests/test_core.py::test_sign \
      --deselect gcsfs/tests/test_core.py::test_mv_file_cache \
      --deselect gcsfs/tests/test_core.py::test_rm_wildcards_non_recursive
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

    # The prefetcher engine is not integrated for zonal in this bucket.
    # It will be integrated in a separate PR, after which this will be removed.
    ZONAL_DESELECTS+=(
      "--deselect=gcsfs/tests/test_core.py::test_cat_file_routing_and_thresholds"
      "--deselect=gcsfs/tests/test_core.py::test_cat_file_concurrent_data_integrity"
      "--deselect=gcsfs/tests/test_core.py::test_cat_file_concurrent_exception_cancellation"
      "--deselect=gcsfs/tests/test_core.py::test_gcsfile_prefetch_disabled_fallback"
      "--deselect=gcsfs/tests/test_core.py::test_gcsfile_prefetch_sequential_integrity"
      "--deselect=gcsfs/tests/test_core.py::test_gcsfile_prefetch_random_seek_integrity"
      "--deselect=gcsfs/tests/test_core.py::test_gcsfile_multithreaded_read_integrity"
      "--deselect=gcsfs/tests/test_core.py::test_gcsfile_not_satisfiable_range"
    )

    # Zonal buckets do not support the requester pays feature
    ZONAL_DESELECTS+=(
      "--deselect=gcsfs/tests/test_core.py::test_requester_pays_fails_without_user_project"
    )

    pytest "${ARGS[@]}" "${ZONAL_DESELECTS[@]}" gcsfs/tests/test_core.py
    ;;
esac
