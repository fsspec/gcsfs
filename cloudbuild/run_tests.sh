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

case $TEST_SUITE in
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
    pytest "${ARGS[@]}" gcsfs/tests/test_extended_gcsfs.py gcsfs/tests/test_zonal_file.py gcsfs/tests/test_async_gcsfs.py gcsfs/tests/integration/test_extended_hns.py
    ;;

  "zonal-core")
    export GCSFS_TEST_BUCKET="gcsfs-test-zonal-${SHORT_BUILD_ID}"
    export GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT='true'
    pytest "${ARGS[@]}" gcsfs/tests/test_core.py --deselect gcsfs/tests/test_core.py::test_sign
    ;;

  "hns")
    export GCSFS_TEST_BUCKET="gcsfs-test-hns-${SHORT_BUILD_ID}"
    export GCSFS_ZONAL_TEST_BUCKET="gcsfs-test-hns-${SHORT_BUILD_ID}"
    export GCSFS_HNS_TEST_BUCKET="gcsfs-test-hns-${SHORT_BUILD_ID}"
    export GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT='true'
    # Excludes tests that are not applicable to HNS buckets:
    # - test_extended_gcsfs.py, test_zonal_file.py: Zonal bucket specific tests which won't work on HNS bucket.
    # - test_core_versioned.py: HNS buckets do not support versioning.
    # - test_core.py::test_sign: Current Cloud Build auth setup does not support this.
    # - test_core.py::test_info_on_directory_with_only_subdirectories: Unit test for regional buckets.
    # - test_core.py::test_mv_file_cache: Integration test only applicable for regional buckets.
    pytest "${ARGS[@]}" gcsfs/ --deselect gcsfs/tests/test_extended_gcsfs.py --deselect gcsfs/tests/test_zonal_file.py --deselect gcsfs/tests/test_core_versioned.py --deselect gcsfs/tests/test_core.py::test_sign --deselect gcsfs/tests/test_core.py::test_info_on_directory_with_only_subdirectories --deselect gcsfs/tests/test_core.py::test_mv_file_cache"
    ;;
esac
