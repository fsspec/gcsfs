import os

worker_id = os.environ.get("PYTEST_XDIST_WORKER")
suffix = f"_{worker_id}" if worker_id else ""

TEST_BUCKET = os.getenv("GCSFS_TEST_BUCKET", "gcsfs_test") + suffix
TEST_VERSIONED_BUCKET = (
    os.getenv("GCSFS_TEST_VERSIONED_BUCKET", "gcsfs_test_versioned") + suffix
)
TEST_HNS_BUCKET = os.getenv("GCSFS_HNS_TEST_BUCKET", "gcsfs_hns_test") + suffix
TEST_ZONAL_BUCKET = os.getenv("GCSFS_ZONAL_TEST_BUCKET", "gcsfs_zonal_test") + suffix
TEST_PROJECT = os.getenv("GCSFS_TEST_PROJECT", "project")
TEST_REQUESTER_PAYS_BUCKET = (
    os.getenv("GCSFS_TEST_REQ_PAYS_BUCKET", "gcsfs_test_req_pays") + suffix
)
TEST_HNS_REQUESTER_PAYS_BUCKET = (
    os.getenv("GCSFS_HNS_TEST_REQ_PAYS_BUCKET", "gcsfs_hns_test_req_pays") + suffix
)
TEST_KMS_KEY = os.getenv(
    "GCSFS_TEST_KMS_KEY",
    f"projects/{TEST_PROJECT}/locations/us/keyRings/gcsfs_test/cryptKeys/gcsfs_test_key",
)

# =============================================================================
# Performance Benchmark Settings
# =============================================================================
BENCHMARK_FILTER = os.environ.get("GCSFS_BENCHMARK_FILTER", "")
BENCHMARK_CPU_AFFINITY = (
    os.environ.get("GCSFS_BENCHMARK_CPU_AFFINITY", "false").lower() == "true"
)
