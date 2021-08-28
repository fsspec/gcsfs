For Developers
==============

We welcome contributions to gcsfs!

Please file issues and requests on github_ and we welcome pull requests.

.. _github: https://github.com/dask/gcsfs/issues

Testing and VCR
---------------

VCR_ records requests to the remote server, so that they can be replayed during
tests - so long as the requests match exactly the original. It is set to strip
out sensitive information before writing the request and responses into yaml
files in the tests/recordings/ directory; the file-name matches the test name,
so all tests must have unique names, across all test files.

The process is as follows:

-   Create a bucket for testing
-   Set environment variables so that the tests run against your GCS
    credentials, and recording occurs

    .. code-block:: bash

       export GCSFS_RECORD_MODE=all
       export GCSFS_TEST_PROJECT='...'
       export GCSFS_TEST_BUCKET='...'  # the bucket from step 1 (without gs:// prefix).
       export GCSFS_GOOGLE_TOKEN=~/.config/gcloud/application_default_credentials.json
       py.test -vv -x -s gcsfs

    If ~/.config/gcloud/application_default_credentials.json file does not exist,
    run ``gcloud auth application-default login``
    These variables can also be set in ``gcsfs/tests/settings.py``

-   Run this again, setting ``GCSFS_RECORD_MODE=once``, which should alert you
    if your tests make different requests the second time around

-   Finally, test as TravisCI will, using only the recordings

    .. code-block:: bash

       export GCSFS_RECORD_MODE=none
       py.test -vv -x -s gcsfs

To reset recording and start again, delete the yaml file corresponding to the
test in ``gcsfs/tests/recordings/*.yaml``.

.. _VCR: https://vcrpy.readthedocs.io/en/latest/
