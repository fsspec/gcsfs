import os

TEST_BUCKET = os.getenv("GCSFS_TEST_BUCKET", "gcsfs_test")
TEST_PROJECT = os.getenv("GCSFS_TEST_PROJECT", "project")
TEST_REQUESTER_PAYS_BUCKET = "gcsfs_test_req_pay"
