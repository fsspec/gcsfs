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
provide appropriate GCSFS_TEST_BUCKET, GCSFS_TEST_VERSIONED_BUCKET
(To use for tests that target GCS object versioning, this bucket must have versioning enabled),
GCSFS_ZONAL_TEST_BUCKET(To use for testing Rapid storage features) and GCSFS_TEST_PROJECT,
as well as setting your default google credentials (or providing them via the fsspec config).

When running tests against a real GCS endpoint, you have two options for test buckets:

- **Provide existing buckets**: If you specify buckets that already exist, the
  test suite will manage objects *within* them (creating, modifying, and deleting
  objects as needed). The buckets themselves will **not** be deleted upon completion.
  **Warning**: The test suite will clear the contents of the bucket at the beginning and end of the
  test run, so be sure to use a bucket that does not contain important data.
- **Let the tests create buckets**: If you specify bucket names that do not exist,
  the test suite will create them for the test run and automatically delete them
  during final cleanup.

.. _fake-gcs-server: https://github.com/fsouza/fake-gcs-server

.. raw:: html

    <script data-goatcounter="https://gcsfs.goatcounter.com/count"
        async src="//gc.zgo.at/count.js"></script>
