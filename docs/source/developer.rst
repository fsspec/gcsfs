For Developers
==============

We welcome contributions to gcsfs!

Please file issues and requests on github_ and we welcome pull requests.

.. _github: https://github.com/fsspec/gcsfs/issues

Testing
-------

The testing framework supports using your own GCS-compliant endpoint, by
setting the "STORAGE_EMULATOR_HOST" environment variable. If this is
not set, then an emulator will be spun up using ``docker`` and
`fake-gcs-server`_. This emulator has almost all the functionality of
real GCS. A small number of tests run differently or are skipped.

If you want to actually test against real GCS, then you should set
STORAGE_EMULATOR_HOST to "https://storage.googleapis.com" and also
provide appropriate GCSFS_TEST_BUCKET and GCSFS_TEST_PROJECT, as well
as setting your default google credentials (or providing them via the
fsspec config).

.. _fake-gcs-server: https://github.com/fsouza/fake-gcs-server
