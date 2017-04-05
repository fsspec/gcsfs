For Developers
==============

We welcome contributions to gcsfs!

Please file issues and requests on github_ and we welcome pull requests.

.. _github: https://github.com/martindurant/gcsfs/issues

Testing and VCR
---------------

VCR_ records requests to the remote server, so that they can be replayed during tests -
so long as the requests match exactly the original. It is set to strip out sensitive
information before writing the request and responses into yaml files in the tests/recordings/
directory; the file-name matches the test name, so all tests must have unique names, across
all test files.

The process is as follows:

- set environment variables so that the tests run against your GCS credentials, and recording
occurs

.. code-block:: bash

   GCSFS_RECORD_MODE=once GCSFS_TEST_PROJECT='...' \
   GCSFS_GOOGLE_TOKEN=/my_credentials.json py.test -vv -x -s gcsfs

- run this again, the ``RECORD_MODE=once`` should alert you if your tests are making
different requests the second time around

- finally, test as TravisCI will, using only the recordings

.. code-block:: bash

   GCSFS_RECORD_MODE=none py.test -vv -x -s gcsfs

To reset recording and start again, delete the yaml file corresponding to the test.

.. _VCR: https://vcrpy.readthedocs.io/en/latest/