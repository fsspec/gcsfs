import os


def _get_bucket_name(env_var: str, default_name: str) -> str:
    worker_id = os.environ.get("PYTEST_XDIST_WORKER")
    suffix = f"_{worker_id}" if worker_id else ""
    return os.getenv(env_var, default_name) + suffix


TEST_BUCKET = _get_bucket_name("GCSFS_TEST_BUCKET", "gcsfs_test")
TEST_VERSIONED_BUCKET = _get_bucket_name(
    "GCSFS_TEST_VERSIONED_BUCKET", "gcsfs_test_versioned"
)
TEST_HNS_BUCKET = _get_bucket_name("GCSFS_HNS_TEST_BUCKET", "gcsfs_hns_test")
# Expected to point at a standard (non-HNS / flat) bucket.
TEST_FLAT_BUCKET = _get_bucket_name("GCSFS_FLAT_TEST_BUCKET", "gcsfs_flat_test")
TEST_ZONAL_BUCKET = _get_bucket_name("GCSFS_ZONAL_TEST_BUCKET", "gcsfs_zonal_test")
TEST_PROJECT = os.getenv("GCSFS_TEST_PROJECT", "project")
TEST_REGION = os.getenv("GCSFS_TEST_REGION", "us-central1")
TEST_REQUESTER_PAYS_BUCKET = _get_bucket_name(
    "GCSFS_TEST_REQ_PAYS_BUCKET", "gcsfs_test_req_pays"
)
TEST_HNS_REQUESTER_PAYS_BUCKET = _get_bucket_name(
    "GCSFS_HNS_TEST_REQ_PAYS_BUCKET", "gcsfs_hns_test_req_pays"
)
TEST_KMS_KEY = os.getenv(
    "GCSFS_TEST_KMS_KEY",
    f"projects/{TEST_PROJECT}/locations/{TEST_REGION}/keyRings/gcsfs_test/cryptoKeys/gcsfs_test_key",
)

# =============================================================================
# Performance Benchmark Settings
# =============================================================================
BENCHMARK_FILTER = os.environ.get("GCSFS_BENCHMARK_FILTER", "")
BENCHMARK_CPU_AFFINITY = (
    os.environ.get("GCSFS_BENCHMARK_CPU_AFFINITY", "false").lower() == "true"
)
