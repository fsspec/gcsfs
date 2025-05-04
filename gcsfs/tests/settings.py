import os

TEST_BUCKET = os.getenv("GCSFS_TEST_BUCKET", "gcsfs_test")
TEST_PROJECT = os.getenv("GCSFS_TEST_PROJECT", "project")
TEST_REQUESTER_PAYS_BUCKET = "gcsfs_test_req_pay"
TEST_KMS_KEY = os.getenv(
    "GCSFS_TEST_KMS_KEY",
    f"projects/{TEST_PROJECT}/locations/us/keyRings/gcsfs_test/cryptKeys/gcsfs_test_key",
)
