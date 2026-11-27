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

Any new test that creates temporary buckets against real GCS must prefix their
names with ``gcsfs-test-``, because the Cloud Build worker and E2E test VM
service accounts are IAM-scoped to ``gcsfs-test-*`` buckets.

Usage Telemetry
---------------

The internal ``gcsfs.telemetry`` package collects lightweight usage attribution (calling framework, top-level method name, whether allowlisted flags are enabled, and compute environment) and encodes it into ``x-goog-storage-gcsfs-features`` on outgoing HTTP and gRPC requests. For user-facing behavior and privacy guarantees, see :doc:`telemetry`.

Architecture
~~~~~~~~~~~~

- ``gcsfs/telemetry/context.py``: ``Dimension`` keys (``fw``, ``env``, ``m``, ``mp``), header constants, and the ``ContextVar`` used to propagate call-scoped attribution across sync/async and thread boundaries (reset on ``os.register_at_fork``).
- ``gcsfs/telemetry/detector.py``: ``FrameworkDetector`` (call-scoped stack inspection capped at 64 frames) and ``EnvironmentDetector`` (process-wide ``gce``/``gke``/``oth`` probe).
- ``gcsfs/telemetry/manager.py``: ``UsageMetricsTracker`` and its default ``default_usage_tracker`` singleton coordinating opt-out checks (``DO_NOT_TRACK`` / ``GCSFS_NO_TELEMETRY``), detector execution, and ``encode_features()``.
- ``gcsfs/telemetry/features_proto.py`` & ``gcsfs/telemetry/schema.py``: ``proto-plus`` schema (``GcsfsFeatures``, ``GcsfsMethod``, ``GcsfsFramework``, ``GcsfsEnvironment``) and LRU-cached web-safe base64 wire encoder.
- ``gcsfs/telemetry/wrapper.py``: Sync, async, generator, and file-handle wrappers (``mirror_gcs_methods``, ``bind_gcs_sync_methods``, ``_gcs_sync_wrapper``, ``with_telemetry_context``, ``wrap_file_methods``). See the module docstring in ``gcsfs/telemetry/wrapper.py`` for the 6 execution flows and wrapper selection guide.
- ``gcsfs/telemetry/grpc.py``: ``ControlPlaneTelemetryInterceptor`` attaching feature metadata to Storage Control unary RPCs. HTTP requests attach the header in ``GCSFileSystem._get_headers`` (``gcsfs/core.py``), and Rapid/RCU stream opens attach it in ``gcsfs/zb_hns_utils.py``.

Extending Telemetry
~~~~~~~~~~~~~~~~~~~

- **Adding a public method or file operation:** Add the enum value to ``GcsfsMethod`` in ``gcsfs/telemetry/features_proto.py`` (never reuse reserved wire numbers ``2, 42, 43, 45``, and keep wire numbers aligned with the server-side GCS protobuf definition). Token names must match the lowercased suffix of their ``GCSFS_METHOD_*`` enum name (otherwise ``schema.py`` silently drops them). ``_TRACKED_METHODS`` in ``wrapper.py`` derives automatically from ``GcsfsMethod``, excluding ``GCSFS_METHOD_FILE_*`` entries:

  - New GCSFS-only ``async def _foo`` methods (not on ``AbstractFileSystem``/``AsyncFileSystem``) are skipped by ``mirror_gcs_methods`` unless a class-level ``foo = asyn.sync_wrapper(_foo)`` is declared first, and every ``_TRACKED_METHODS`` entry must exist on both ``GCSFileSystem`` and ``ExtendedGcsFileSystem`` (enforced by ``test_filesystem_methods_are_telemetry_wrapped``). Keep ``bind_gcs_sync_methods`` for mirrored sync methods so ``fork()`` + ``exec()`` safety (e.g. ``subprocess.Popen(..., preexec_fn=...)``) is preserved.
  - For new file-handle methods that issue requests, use a ``GCSFS_METHOD_FILE_*`` enum name and add an entry to ``_FILE_METHOD_TOKENS``. Note that ``wrap_file_methods`` only wraps attributes defined directly in ``cls.__dict__`` (inherited ``AbstractBufferedFile`` methods need explicit handling, as done for ``GCSFile.flush``).
  - Update the expected method/file-token counts in ``test_tracked_and_file_methods_map_to_schema_enums`` (``gcsfs/tests/test_telemetry.py``), and if the new method accepts any allowlisted parameter flag, update ``_TRACKED_PARAM_METHODS`` or ``_INTENTIONALLY_UNTRACKED_PARAMS`` for ``test_method_tracked_params_match_signatures_and_kwargs_forwarding``.

- **Adding a tracked method parameter:** Only whether a flag is enabled (truthy, including defaults — never raw argument values) may be recorded. Add a ``proto.BOOL`` field to ``GcsfsFeatures.MethodParams`` in ``features_proto.py`` (and the server-side protobuf), add a corresponding row to ``_TRACKED_PARAM_METHODS`` in ``wrapper.py`` (and ``_METHOD_IMPLICIT_TRUTHY_DEFAULTS`` if a method defaults a flag to truthy via ``kwargs.pop``), and run ``test_method_tracked_params_match_signatures_and_kwargs_forwarding`` in ``gcsfs/tests/test_telemetry.py`` to identify every accepting method across ``GCSFileSystem`` and ``ExtendedGcsFileSystem`` (recording any intentional omissions in ``_INTENTIONALLY_UNTRACKED_PARAMS``). Update :doc:`telemetry` and ``README.md`` to reflect the updated allowlist.
- **Adding an upstream framework:** Register the package mapping in ``FrameworkDetector.KNOWN_FRAMEWORKS`` (``detector.py``) so the token matches the lowercased suffix of its ``GCSFS_FRAMEWORK_*`` enum name, add the enum value to ``GcsfsFramework`` (``features_proto.py`` and server-side protobuf), update :doc:`telemetry`, and add coverage in ``gcsfs/tests/test_telemetry.py`` and ``gcsfs/tests/test_upstream_frameworks_real.py``.

Telemetry Tests
~~~~~~~~~~~~~~~

- Unit tests: ``pytest gcsfs/tests/test_telemetry.py``
- End-to-end tests: ``pytest gcsfs/tests/test_telemetry_e2e.py``
- Real upstream framework integration tests: ``pytest gcsfs/tests/test_upstream_frameworks_real.py``
- **Test prerequisites:**

  - Without Docker on ``PATH``, ``conftest.py``'s session-scoped autouse ``final_cleanup`` fixture skips even the mocked unit suites (``test_telemetry.py``, ``test_upstream_frameworks_real.py``) unless ``STORAGE_EMULATOR_HOST`` is set (any placeholder such as ``STORAGE_EMULATOR_HOST=http://localhost:4443`` suffices for mocked suites; ``test_telemetry_e2e.py`` requires a running emulator or real GCS).
  - ``test_upstream_frameworks_real.py`` (and the framework E2E tests in ``test_telemetry_e2e.py``) skip any framework package that is not installed (``pandas``, ``pytorch-cpu``, and ``lightning`` are listed in ``environment_gcsfs.yaml``, not ``pyproject.toml``'s ``[dev]`` extra).

- **Module reload note:** ``gcsfs/tests/test_init.py`` keeps ``gcsfs.telemetry.features_proto`` in ``_KEEP_MODULES`` because protobuf's default descriptor pool cannot register the same proto definitions twice in one process.

Release Process
---------------

For details on the release process, see :doc:`release`.

.. _fake-gcs-server: https://github.com/fsouza/fake-gcs-server

.. raw:: html

    <script data-goatcounter="https://gcsfs.goatcounter.com/count"
        async src="//gc.zgo.at/count.js"></script>
