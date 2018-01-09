
import os
import gcsfs.core

RECORD_MODE = os.environ.get('GCSFS_RECORD_MODE', 'none')
TEST_PROJECT = os.environ.get('GCSFS_TEST_PROJECT', 'test_project')

TEST_BUCKET = os.environ.get('GCSFS_TEST_BUCKET', 'gcsfs-testing')

FAKE_TOKEN = {'access_token': 'xxx', 'expires_in': 0,
              'grant_type': 'refresh_token',
              'refresh_token': 'xxx', 'timestamp': 1487859400.}

FAKE_TOKEN.update(gcsfs.core.not_secret)

FAKE_GOOGLE_TOKEN = {
  "client_id": "764086051850-6qr4p6gpi6hn506pt8ejuq83di341hur."
               "apps.googleusercontent.com",
  "client_secret": "d-FL95Q19q7MQmFpd7hHD0Ty",
  "refresh_token": "xxx",
  "type": "authorized_user"
}
GOOGLE_TOKEN = os.environ.get('GCSFS_GOOGLE_TOKEN', FAKE_GOOGLE_TOKEN)
DEBUG = os.environ.get('GCSFS_DEBUG', False)
