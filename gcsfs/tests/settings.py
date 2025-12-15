import os

TEST_BUCKET = os.getenv("GCSFS_TEST_BUCKET", "gcsfs_test")
TEST_VERSIONED_BUCKET = os.getenv("GCSFS_TEST_VERSIONED_BUCKET", "gcsfs_test_versioned")
TEST_ZONAL_BUCKET = os.getenv("GCSFS_ZONAL_TEST_BUCKET", "gcsfs_zonal_test")
TEST_HNS_BUCKET = os.getenv("GCSFS_HNS_TEST_BUCKET", "gcsfs_hns_test")
TEST_PROJECT = os.getenv("GCSFS_TEST_PROJECT", "project")
TEST_REQUESTER_PAYS_BUCKET = f"{TEST_BUCKET}_req_pay"
TEST_KMS_KEY = os.getenv(
    "GCSFS_TEST_KMS_KEY",
    f"projects/{TEST_PROJECT}/locations/us/keyRings/gcsfs_test/cryptKeys/gcsfs_test_key",
)

# =============================================================================
# Performance Benchmark Settings
# =============================================================================
BENCHMARK_FILTER = os.environ.get("GCSFS_BENCHMARK_FILTER", "")

BENCHMARK_FILE_SIZES_MB_STR = os.environ.get(
    "GCSFS_BENCHMARK_FILE_SIZES", "128"
)  # comma separated list of sizes in MB
BENCHMARK_FILE_SIZES_MB = [int(s) for s in BENCHMARK_FILE_SIZES_MB_STR.split(",") if s]

BENCHMARK_THREADS_STR = os.environ.get(
    "GCSFS_BENCHMARK_THREADS", "1"
)  # comma separated list of thread counts
BENCHMARK_THREADS = [int(s) for s in BENCHMARK_THREADS_STR.split(",") if s]

BENCHMARK_PROCESSES_STR = os.environ.get(
    "GCSFS_BENCHMARK_PROCESSES", "1"
)  # comma separated list of process counts
BENCHMARK_PROCESSES = [int(s) for s in BENCHMARK_PROCESSES_STR.split(",") if s]

BENCHMARK_CHUNK_SIZE_MB = int(os.environ.get("GCSFS_BENCHMARK_CHUNK_SIZE_MB", "16"))
BENCHMARK_BLOCK_SIZE_MB = int(os.environ.get("GCSFS_BENCHMARK_BLOCK_SIZE_MB", "16"))

BENCHMARK_ROUNDS = int(os.environ.get("GCSFS_BENCHMARK_ROUNDS", "10"))
BENCHMARK_SKIP_TESTS = os.environ.get("GCSFS_BENCHMARK_SKIP_TESTS", "true").lower() in (
    "true",
    "1",
)
