Telemetry & Usage Attribution
=============================

``gcsfs`` includes lightweight, automatic caller framework detection to identify the upstream data science and machine learning libraries that interact with Google Cloud Storage (GCS).

Purpose
-------

The Python data ecosystem comprises a rich variety of frameworks—including Pandas, PyTorch, Ray, Dask, Hugging Face, and PyArrow. Telemetry attribution helps the maintainers and Google Cloud Storage understand real-world usage patterns, identify performance bottlenecks for specific workloads, and prioritize compatibility improvements across popular open-source libraries.

What is Collected
-----------------

When an operation is executed, ``gcsfs`` identifies the name of the top-level calling framework and appends an anonymous brand identifier token to the HTTP ``User-Agent`` header of outgoing requests to Google Cloud Storage.

Outgoing HTTP JSON requests also emit the encoded ``X-Goog-Storage-Gcsfs-Features`` header alongside ``User-Agent`` (matching ``x-goog-storage-gcsfs-features`` on gRPC), honoring caller-supplied overrides case-insensitively and omitting the header when no attribution dimensions are active.

Example Header
~~~~~~~~~~~~~~

.. code-block:: text

   User-Agent: python-gcsfs/2026.3.0 fw/pandas
   X-Goog-Storage-Gcsfs-Features: EAE=

Recognized Frameworks
~~~~~~~~~~~~~~~~~~~~~

The detector recognizes major ecosystem libraries, including:

- **Data Processing & Analytics:** ``pandas`` (``fw/pandas``), ``polars`` (``fw/polars``), ``dask`` (``fw/dask``), ``ray`` (``fw/ray``), ``pyspark`` (``fw/spark``), ``pyarrow`` (``fw/pyarrow``), ``duckdb`` (``fw/duckdb``), ``xarray`` (``fw/xarray``), ``fastparquet`` (``fw/fastparquet``).
- **Machine Learning & Deep Learning:** ``torch`` (``fw/torch``), ``torchdata`` (``fw/torchdata``), ``lightning`` / ``pytorch_lightning`` (``fw/lightning``), ``datasets`` (``fw/datasets``), ``transformers`` (``fw/transformers``), ``orbax`` (``fw/orbax``), ``tensorstore`` (``fw/tensorstore``), ``tensorflow`` (``fw/tensorflow``), ``jax`` (``fw/jax``).

If no recognized framework is present on the call stack (e.g. pure Python scripts or unrecognized utilities), no framework token is added to the header.

gRPC Metadata
~~~~~~~~~~~~~

Because gRPC channels use a ``User-Agent`` fixed when the client is created, ``gcsfs`` instead attaches the encoded ``x-goog-storage-gcsfs-features`` metadata header (a web-safe base64-encoded ``GcsfsFeatures`` protobuf payload) when active attribution dimensions are present:

- **Storage Control Unary RPCs:** Every unary call (including LRO operations, bucket layout lookups, and HNS folder operations such as ``mkdir``, ``rmdir``, and ``mv``) attaches ``x-goog-storage-gcsfs-features`` per RPC.
- **Rapid (Zonal) and Rapid Cache Ultra Stream Opens:** Initial read (``MRD``) and write stream opens attach ``x-goog-storage-gcsfs-features`` at open time.

Caller-supplied ``x-goog-storage-gcsfs-features`` metadata is preserved verbatim (case-insensitively) without being overwritten, and the header is omitted when no recognized attribution dimensions are active.

.. code-block:: text

   x-goog-storage-gcsfs-features: EAE=

.. note::

   Pooled ``MRD`` read streams retain stream-opener attribution across subsequent pooled reads, and internal mid-stream SDK reconnects omit initial open-time metadata.

Privacy & Security Guarantees
-----------------------------

Telemetry collection is strictly limited to open-source library names and respects user privacy and security:

- **No User Data or Payloads:** Object contents read or written are never inspected or recorded.
- **No File Paths or Bucket Names:** Bucket names, object keys, folder hierarchies, and URIs are never collected.
- **No User Code or Logic:** Variable names, function arguments, and script logic are never accessed.
- **No Credentials or Identifiers:** API keys, service account credentials, IAM identities, project IDs, and IP addresses are never collected.
- **Header Injection Safety:** All tokens are sanitized strictly against RFC 9110 HTTP token specifications (alphanumeric characters, hyphens, underscores, and dots only).

How It Works
------------

1. **Zero Configuration:** Upstream libraries do not need any code changes or special configuration. Detection occurs transparently when filesystem methods (e.g. ``fs.ls``, ``fs.cat_file``, ``fs.open``, ``df.to_parquet``, ``torch.load``) are called.
2. **Thread Boundary Crossing:** In ``fsspec``, synchronous operations dispatch coroutines to a background asyncio event loop thread. ``gcsfs`` captures caller identity on the user's thread and safely propagates it to the background event loop via Python ``contextvars``.
3. **Performance & Caching:** Detection uses lightweight stack frame inspection (via ``sys._getframe()``) capped at 64 frames, and runs once per call on the caller's thread (never on the background event loop thread). Active ``GCSFile`` instances cache their framework attribution so subsequent chunk downloads and background prefetching operations execute with zero stack-walking overhead (:math:`O(1)`).

Disabling Telemetry
-------------------

Telemetry collection is enabled by default, but you can opt out at any time by setting an environment variable.

Using Environment Variables
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Set ``GCSFS_NO_TELEMETRY=true`` or the standard ``DO_NOT_TRACK=1`` in your shell environment:

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
- Outgoing HTTP ``User-Agent`` framework tokens, HTTP ``X-Goog-Storage-Gcsfs-Features`` headers, and gRPC ``x-goog-storage-gcsfs-features`` metadata are suppressed (HTTP ``User-Agent`` headers contain only standard ``python-gcsfs/<version>`` headers).
- Caller-supplied overrides (such as explicit HTTP ``X-Goog-Storage-Gcsfs-Features`` headers or gRPC ``x-goog-storage-gcsfs-features`` metadata) are preserved untouched.
