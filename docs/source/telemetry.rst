Telemetry & Usage Attribution
=============================

``gcsfs`` includes lightweight, automatic caller framework detection to identify the upstream data science and machine learning libraries that interact with Google Cloud Storage (GCS), along with compute environment detection.

Purpose
-------

The Python data ecosystem comprises a rich variety of frameworks—including Pandas, PyTorch, Ray, Dask, Hugging Face, and PyArrow. Telemetry attribution helps the maintainers and Google Cloud Storage understand real-world usage patterns, identify performance bottlenecks for specific workloads, and prioritize compatibility improvements across popular open-source libraries.

What is Collected
-----------------

When an operation is executed, ``gcsfs`` identifies the name of the top-level calling framework and the compute environment, and appends an anonymous brand identifier token and an environment token to the HTTP ``User-Agent`` header of outgoing requests to Google Cloud Storage.

Outgoing HTTP JSON requests also emit the encoded ``X-Goog-Storage-Gcsfs-Features`` header alongside ``User-Agent`` (matching ``x-goog-storage-gcsfs-features`` on gRPC), honoring caller-supplied overrides case-insensitively and omitting the header when no attribution dimensions are active.

Example Header
~~~~~~~~~~~~~~

.. code-block:: text

   User-Agent: python-gcsfs/2026.3.0 fw/pandas env/gke
   X-Goog-Storage-Gcsfs-Features: EAEYAg==

Recognized Frameworks
~~~~~~~~~~~~~~~~~~~~~

The detector recognizes major ecosystem libraries, including:

- **Data Processing & Analytics:** ``pandas`` (``fw/pandas``), ``polars`` (``fw/polars``), ``dask`` (``fw/dask``), ``ray`` (``fw/ray``), ``pyspark`` (``fw/spark``), ``pyarrow`` (``fw/pyarrow``), ``duckdb`` (``fw/duckdb``), ``xarray`` (``fw/xarray``), ``fastparquet`` (``fw/fastparquet``).
- **Machine Learning & Deep Learning:** ``torch`` (``fw/torch``), ``torchdata`` (``fw/torchdata``), ``lightning`` / ``pytorch_lightning`` (``fw/lightning``), ``datasets`` (``fw/datasets``), ``transformers`` (``fw/transformers``), ``orbax`` (``fw/orbax``), ``tensorstore`` (``fw/tensorstore``), ``tensorflow`` (``fw/tensorflow``), ``jax`` (``fw/jax``).

If no recognized framework is present on the call stack (e.g. pure Python scripts or unrecognized utilities), no framework token is added to the header. When multiple recognized libraries appear in the same call (for example, ``dask`` calling ``pandas`` and ``pyarrow``), only a single token for the outermost recognized framework (``fw/dask``) is included.

Compute Environments
~~~~~~~~~~~~~~~~~~~~

The environment detector classifies the host into one of three values:

- ``env/gce``: VMs on Compute Engine.
- ``env/gke``: GKE pods.
- ``env/oth``: Serverless runtimes (Cloud Run, Cloud Functions, App Engine) and hosts outside Google Cloud.

Detection runs once per process (and not at all when telemetry is disabled). On Google Cloud VMs, ``gcsfs`` makes a background request to the `metadata server <https://cloud.google.com/compute/docs/metadata/overview>`_ to tell GKE apart from Compute Engine; elsewhere it uses only local environment variables and the machine's hardware product name. Setting ``GCE_METADATA_HOST``, ``GCE_METADATA_ROOT``, or ``GCE_METADATA_IP`` points ``gcsfs`` at that metadata server outside Google Cloud VMs too (unless a serverless runtime matched first), and ``NO_GCE_CHECK=true`` skips the network request altogether. Storage operations never wait for this lookup, so requests sent before it finishes omit the ``env/`` token.

How the environment is detected
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

``gcsfs`` checks the host in order:

- **Serverless runtimes** (Cloud Run, Cloud Functions, App Engine) are recognized from their
  platform environment variables and resolve to ``oth`` without any network request, unless
  the process runs inside Kubernetes.
- **Hosts that are not Google Cloud VMs** resolve to ``oth`` without any network request,
  unless ``GCE_METADATA_*`` is set. On Linux, ``gcsfs`` reads the hardware product name from
  ``/sys/class/dmi/id/product_name`` and treats ``Google`` or ``Google Compute Engine`` as a
  Google Cloud VM; other operating systems resolve to ``oth``.
- **Google Cloud VMs** (and hosts with ``GCE_METADATA_*`` set): when the filesystem is
  created, ``gcsfs`` queries the metadata server in the background for the instance's
  ``cluster-name`` attribute. If it is set, the host resolves to ``gke``, otherwise ``gce``
  (the cluster name itself is never sent). If the metadata server is not ready yet, ``gcsfs``
  retries a few times with backoff.

Each process caches the result: ``fork`` children reuse a resolved value, and ``spawn`` or
``forkserver`` workers each detect once. If ``NO_GCE_CHECK=true`` is set, or the metadata
server cannot be reached or does not answer as a Google metadata server, ``gcsfs`` decides
from local signals only (the hardware product name above and whether the process runs in a
Kubernetes pod).

gRPC Metadata
~~~~~~~~~~~~~

Because gRPC channels use a ``User-Agent`` fixed when the client is created, ``gcsfs`` instead attaches the encoded ``x-goog-storage-gcsfs-features`` metadata header (a web-safe base64-encoded ``GcsfsFeatures`` protobuf payload) when active attribution dimensions are present:

- **Storage Control Unary RPCs:** Every unary call (including LRO operations, bucket layout lookups, and HNS folder operations such as ``mkdir``, ``rmdir``, and ``mv``) attaches ``x-goog-storage-gcsfs-features`` per RPC.
- **Rapid (Zonal) and Rapid Cache Ultra Stream Opens:** Initial read (``MRD``) stream opens, and appendable write stream opens (Rapid zonal writes, and Rapid Cache Ultra writes when ``rcu_rapid_write=True``), attach ``x-goog-storage-gcsfs-features`` at open time.

Caller-supplied ``x-goog-storage-gcsfs-features`` metadata is preserved verbatim (case-insensitively) without being overwritten, and the header is omitted when no recognized attribution dimensions are active.

.. code-block:: text

   x-goog-storage-gcsfs-features: EAE=

.. note::

   Pooled ``MRD`` read streams retain stream-opener attribution across subsequent pooled reads, and internal mid-stream SDK reconnects omit initial open-time metadata.

Privacy & Security Guarantees
-----------------------------

Telemetry collection is strictly limited to open-source library names and environment categories (``gce``, ``gke``, ``oth``), and respects user privacy and security:

- **No User Data or Payloads:** Object contents read or written are never inspected or recorded.
- **No File Paths or Bucket Names:** Bucket names, object keys, folder hierarchies, and URIs are never collected.
- **No User Code or Logic:** Variable names, function arguments, and script logic are never accessed.
- **No Credentials or Identifiers:** API keys, service account credentials, IAM identities, project IDs, and IP addresses are never collected.
- **Header Injection Safety:** All tokens are sanitized strictly against RFC 9110 HTTP token specifications (alphanumeric characters, hyphens, underscores, and dots only).
- **No Cluster or Instance Names:** The metadata query checks only whether ``cluster-name`` is set and never sends cluster names, instance names, or zones to GCS.

How It Works
------------

1. **Zero Configuration:** Upstream libraries do not need any code changes or special configuration. Detection occurs transparently when a filesystem is created and filesystem methods (e.g. ``fs.ls``, ``fs.cat_file``, ``fs.open``, ``df.to_parquet``, ``torch.load``) are called.
2. **Thread Boundary Crossing:** In ``fsspec``, synchronous operations dispatch coroutines to a background asyncio event loop thread (``fsspecIO`` by default). ``gcsfs`` captures the calling framework on the user's thread and safely propagates it to the background event loop via Python ``contextvars``.
3. **Performance & Caching:** Framework detection uses lightweight stack frame inspection (via ``sys._getframe()``) capped at 64 frames, and runs once per call on the caller's thread (never on the background event loop thread for synchronous calls). Compute environment detection runs once per process (querying the metadata server in the background when needed) and caches its result in memory. Active ``GCSFile`` instances cache the framework detected at ``open()`` (including when none is detected) so subsequent chunk reads, writes, and background prefetching execute with zero stack-walking overhead (``O(1)``); ``ZonalFile`` likewise reuses that cached attribution for subsequent HTTP metadata lookups and initial gRPC stream opens.

Disabling Telemetry
-------------------

Telemetry collection is enabled by default, but you can opt out at any time by setting an environment variable.

Using Environment Variables
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Set ``GCSFS_NO_TELEMETRY=true`` or the standard ``DO_NOT_TRACK=1`` (both accept ``1``, ``true``, or ``yes``, case-insensitive) in your shell environment:

.. code-block:: bash

   export GCSFS_NO_TELEMETRY=true
   # or
   export DO_NOT_TRACK=1

In Python Code
~~~~~~~~~~~~~~

The setting is read on every call, so it can be set (or changed) at any time before the storage calls you want to exclude:

.. code-block:: python

   import os
   os.environ["DO_NOT_TRACK"] = "1"
   # or os.environ["GCSFS_NO_TELEMETRY"] = "true"

   import gcsfs

When disabled:

- All stack frame inspections and detector evaluations are skipped immediately.
- Outgoing ``User-Agent`` headers omit ``fw/`` and ``env/`` telemetry tokens, HTTP requests omit ``X-Goog-Storage-Gcsfs-Features``, and gRPC calls omit ``x-goog-storage-gcsfs-features`` metadata.
- ``gcsfs`` sends no requests to the metadata server.
- Caller-supplied overrides (such as explicit HTTP ``X-Goog-Storage-Gcsfs-Features`` headers or gRPC ``x-goog-storage-gcsfs-features`` metadata) are preserved untouched.
