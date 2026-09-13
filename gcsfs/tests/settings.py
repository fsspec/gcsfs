import os


def _get_bucket_name(env_var: str, default_name: str) -> str:
    worker_id = os.environ.get("PYTEST_XDIST_WORKER")
    suffix = f"-{worker_id}" if worker_id else ""
    return os.getenv(env_var, default_name) + suffix


TEST_BUCKET = _get_bucket_name("GCSFS_TEST_BUCKET", "gcsfs_test")
TEST_VERSIONED_BUCKET = _get_bucket_name(
    "GCSFS_TEST_VERSIONED_BUCKET", "gcsfs_test_versioned"
)
TEST_HNS_BUCKET = _get_bucket_name("GCSFS_HNS_TEST_BUCKET", "gcsfs_hns_test")
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
# Share read-benchmark fixture files between cases that need identical files instead of
# rebuilding them per case. Opt-in: leaving it off preserves the historical behaviour of
# every case reading a freshly written object.
BENCHMARK_REUSE_FILES = (
    os.environ.get("GCSFS_BENCHMARK_REUSE_FILES", "false").lower() == "true"
)
# Comma-separated IO sizes in MB that replace each group's configured chunk_sizes_mb.
# Empty leaves the configured sizes alone. Lets a pipeline narrow the IO dimension
# without editing the shared per-group configs.yaml.
BENCHMARK_CHUNK_SIZES_MB = os.environ.get("GCSFS_BENCHMARK_CHUNK_SIZES_MB", "")
