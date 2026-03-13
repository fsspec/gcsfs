#!/bin/bash
set -e
source env/bin/activate

# Common Exports
export STORAGE_EMULATOR_HOST=https://storage.googleapis.com
export GCSFS_TEST_PROJECT=${PROJECT_ID}
export GCSFS_TEST_KMS_KEY=projects/${PROJECT_ID}/locations/${REGION}/keyRings/${KEY_RING}/cryptoKeys/${KEY_NAME}

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
    pytest "${ARGS[@]}" gcsfs/ --deselect gcsfs/tests/test_core.py::test_sign
    ;;

  "zonal")
    export GCSFS_TEST_BUCKET="gcsfs-test-standard-for-zonal-${SHORT_BUILD_ID}"
    export GCSFS_ZONAL_TEST_BUCKET="gcsfs-test-zonal-${SHORT_BUILD_ID}"
    export GCSFS_HNS_TEST_BUCKET="gcsfs-test-zonal-${SHORT_BUILD_ID}"
    ulimit -n 4096
    export GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT='true'
    pytest "${ARGS[@]}" \
      gcsfs/tests/test_extended_gcsfs.py \
      gcsfs/tests/test_zonal_file.py \
      gcsfs/tests/integration/test_async_gcsfs.py \
      gcsfs/tests/integration/test_extended_hns.py
    ;;

  "hns")
    export GCSFS_TEST_BUCKET="gcsfs-test-hns-${SHORT_BUILD_ID}"
    export GCSFS_ZONAL_TEST_BUCKET="gcsfs-test-hns-${SHORT_BUILD_ID}"
    export GCSFS_HNS_TEST_BUCKET="gcsfs-test-hns-${SHORT_BUILD_ID}"
    export GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT='true'
    # Excludes tests that are not applicable to HNS buckets:
    # - test_extended_gcsfs.py, test_zonal_file.py: Zonal bucket specific tests which won't work on HNS bucket.
    # - test_extended_gcsfs_unit.py: Unit tests for zonal bucket features.
    # - test_core_versioned.py: HNS buckets do not support versioning.
    # - test_core.py::test_sign: Current Cloud Build auth setup does not support this.
    # - test_core.py::test_mv_file_cache: Integration test only applicable for regional buckets.
    pytest "${ARGS[@]}" gcsfs/ \
      --deselect gcsfs/tests/test_extended_gcsfs.py \
      --deselect gcsfs/tests/test_zonal_file.py \
      --deselect gcsfs/tests/test_extended_gcsfs_unit.py \
      --deselect gcsfs/tests/test_core_versioned.py \
      --deselect gcsfs/tests/test_core.py::test_sign \
      --deselect gcsfs/tests/test_core.py::test_mv_file_cache
    ;;

  "zonal-core")
    export GCSFS_TEST_BUCKET="gcsfs-test-zonal-core-${SHORT_BUILD_ID}"
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
    ZONAL_DESELECTS+=(
      "--deselect=gcsfs/tests/test_core.py::test_flush"
      "--deselect=gcsfs/tests/test_core.py::test_write_blocks"
      "--deselect=gcsfs/tests/test_core.py::test_write_blocks2"
      "--deselect=gcsfs/tests/test_core.py::test_transaction"
      "--deselect=gcsfs/tests/test_core.py::test_array"
      "--deselect=gcsfs/tests/test_core.py::test_sign"
      "--deselect=gcsfs/tests/test_core.py::test_mv_file_cache"
    )

    # Zonal tests with prefetcher cache does not call _cat_file method.
    # The following tests depends upon mocking the _cat_file method for regional.
    ZONAL_DESELECTS+=(
      "--deselect=gcsfs/tests/test_core.py::test_prefetcher_logical_chunk_override"
      "--deselect=gcsfs/tests/test_core.py::test_fetch_logical_chunk_exception"
    )

    pytest "${ARGS[@]}" "${ZONAL_DESELECTS[@]}" gcsfs/tests/test_core.py
    ;;
esac
