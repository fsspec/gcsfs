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
GCSFS_ZONAL_TEST_BUCKET(To use for testing Rapid storage features), GCSFS_HNS_TEST_BUCKET
(To use for testing HNS features) and GCSFS_TEST_PROJECT, as well as setting your
default google credentials (or providing them via the fsspec config).

When running tests against a real GCS endpoint, you have two options for test buckets:

- **Provide existing buckets**: If you specify buckets that already exist, the
  test suite will manage objects *within* them (creating, modifying, and deleting
  objects as needed). The buckets themselves will **not** be deleted upon completion.
  **Warning**: The test suite will clear the contents of the bucket at the beginning and end of the
  test run, so be sure to use a bucket that does not contain important data.
- **Let the tests create buckets**: If you specify bucket names that do not exist,
  the test suite will create them for the test run and automatically delete them
  during final cleanup.

End-to-end Testing CI Pipeline
------------------------------

We have a Cloud Build pipeline for end-to-end tests which includes tests on zonal
and regional buckets. When a pull request is created for the ``main`` branch,
there will be a ``end-to-end-tests-trigger`` check in the GitHub checks section.

The pipeline's behavior depends on the author of the pull request:

- If the PR is created by an owner or a collaborator, the pipeline will be
  triggered immediately.
- If the PR is from an external contributor, an owner or collaborator must add
  the comment ``/gcbrun`` to the PR to trigger the pipeline,
  until then pipeline would be in failure state.

The pipeline will also be triggered when a new commit is added to the PR. For
external contributors, a new ``/gcbrun`` comment is required from an owner or
collaborator after the new commit. The pipeline can also be manually
re-triggered by adding a ``/gcbrun`` comment or by using re-run option from Github UI.

The logs from the test run are available in the "details" section of the Checks
tab in the pull request.

Performance Testing
-------------------

GCSFS provides microbenchmarks in ``gcsfs/tests/perf/microbenchmarks`` to measure
operation latency and throughput.

PR Performance Benchmarking
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Pull requests can be evaluated for performance regressions against ``main``
via the Cloud Build performance pipeline (``cloudbuild/benchmarks/ci-perf-cloudbuild.yaml``):

- **Trigger**: Apply the ``execute-perf-test`` label to the PR and comment ``/gcbrun``.
- **GitHub Check**: Reported as the ``gcsfs-ci-perf-test`` check in the PR Checks tab.
- **Filter**: If the ``execute-perf-test`` label is not present on the PR, the pipeline exits early without provisioning infrastructure, preventing unwanted benchmark runs when running standard tests.
- **Environment**: Runs on a Google Cloud VM against real buckets. Which bucket types are created is controlled by ``_BUCKET_TYPES``; the read-path default uses Regional and Zonal, and HNS is available for the metadata groups.
- **Scope**: Defaults to the core read path at a single 1 MB IO size (the ``read``
  group's sequential and random fixed-duration scenarios, 8 cases, roughly 20 minutes
  per branch). Widen it with ``_BENCHMARK_GROUPS``, ``_BENCHMARK_CONFIG`` and
  ``_CHUNK_SIZES_MB``. Cost is ``cases x rounds x runtime``, so restoring all four IO
  sizes gives 32 cases and running every group (458 cases) takes many hours and needs
  the build timeout raised.
- **Fixtures**: Read benchmarks never mutate their fixture files, so the pipeline passes
  ``--reuse-files`` to build one file set per distinct (bucket, size, count) and share it
  across cases. The default selection does 2 uploads per branch instead of one per case.
  Set ``_REUSE_FILES`` to ``false`` to give every case a freshly written object.
- **Per-PR selection**: Cloud Build matches ``/gcbrun`` exactly and cannot carry
  arguments, so the selection is taken from the PR description instead. Add any of these
  lines to the PR body before commenting ``/gcbrun``:

  .. code-block:: text

      perf-groups:    read write
      perf-scenarios: read_seq_fixed_duration
      perf-io-sizes:  1, 16

  Each directive must begin its own line (indentation and ``-``, ``*`` or ``>``
  markers are fine), may appear at most once, and takes values separated by commas or
  spaces. Directives inside fenced code blocks are ignored, so the syntax can be quoted
  when explaining it. Each is optional and falls back to the substitution default. Naming ``perf-groups``
  without ``perf-scenarios`` clears the scenario filter, since the default filter lists
  read-specific scenarios that would match nothing in another group. Values must be bare
  identifiers or numbers, and group and scenario names are checked against the groups and
  ``configs.yaml`` scenarios that exist in the repository. A typo fails the build
  immediately with the valid names listed, before any infrastructure is created; this
  matters because the runner matches scenario names exactly, so an unchecked typo would
  otherwise run zero benchmarks and report nothing. List the valid names with:

  .. code-block:: bash

      python cloudbuild/benchmarks/check_pr_label.py --list

- **Averaging**: Each scenario repeats according to the ``rounds`` setting in its group's ``configs.yaml``, and the mean is compared. The base branch is run with the PR's ``configs.yaml`` so both sides use identical parameters.
- **Threshold**: Regressions exceeding **10%** are flagged in bold in the report. They are reported only and do not fail the build; pass ``--fail-on-regression`` to ``compare.py`` to make them blocking.
- **Local Comparison**: Compare benchmark runs locally using ``compare.py``:

  .. code-block:: bash

      python gcsfs/tests/perf/microbenchmarks/compare.py base.json pr.json --threshold=10.0

  Results from several groups can be combined by passing a comma-separated list of
  ``results.json`` paths for either side.

Release Process
---------------

For details on the release process, see :doc:`release`.

.. _fake-gcs-server: https://github.com/fsouza/fake-gcs-server

.. raw:: html

    <script data-goatcounter="https://gcsfs.goatcounter.com/count"
        async src="//gc.zgo.at/count.js"></script>
