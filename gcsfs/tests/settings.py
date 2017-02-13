
import os

RECORD_MODE = os.environ.get('GCSFS_RECORD_MODE', 'all')
TEST_PROJECT = os.environ.get('GCSFS_TEST_PROJECT', '')
GOOGLE_TOKEN = os.environ.get('GCSFS_GOOGLE_TOKEN', None)
