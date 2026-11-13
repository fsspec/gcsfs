Telemetry & Usage Attribution
=============================

``gcsfs`` includes lightweight, automatic caller framework detection to identify the upstream data science and machine learning libraries that interact with Google Cloud Storage (GCS).

Purpose
-------

The Python data ecosystem comprises a rich variety of frameworks—including Pandas, PyTorch, Ray, Dask, Hugging Face, and PyArrow. Telemetry attribution helps the maintainers and Google Cloud Storage understand real-world usage patterns, identify performance bottlenecks for specific workloads, and prioritize compatibility improvements across popular open-source libraries.

What is Collected
-----------------

When an operation is executed, ``gcsfs`` identifies the name of the top-level calling framework and appends an anonymous brand identifier token to the HTTP ``User-Agent`` header of outgoing requests to Google Cloud Storage.

Example Header
~~~~~~~~~~~~~~

.. code-block:: text

   User-Agent: python-gcsfs/2026.3.0 fw/pandas

Recognized Frameworks
~~~~~~~~~~~~~~~~~~~~~

The detector recognizes major ecosystem libraries, including:

- **Data Processing & Analytics:** ``pandas`` (``fw/pandas``), ``polars`` (``fw/polars``), ``dask`` (``fw/dask``), ``ray`` (``fw/ray``), ``pyspark`` (``fw/spark``), ``pyarrow`` (``fw/pyarrow``), ``duckdb`` (``fw/duckdb``), ``xarray`` (``fw/xarray``), ``fastparquet`` (``fw/fastparquet``).
- **Machine Learning & Deep Learning:** ``torch`` (``fw/torch``), ``torchdata`` (``fw/torchdata``), ``lightning`` / ``pytorch_lightning`` (``fw/lightning``), ``datasets`` (``fw/datasets``), ``transformers`` (``fw/transformers``), ``orbax`` (``fw/orbax``), ``tensorstore`` (``fw/tensorstore``), ``tensorflow`` (``fw/tensorflow``), ``jax`` (``fw/jax``).

If no recognized framework is present on the call stack (e.g. pure Python scripts or unrecognized utilities), no framework token is added to the header.

.. note::

   The framework token is attached to JSON API (HTTP) requests only. gRPC requests use a ``User-Agent`` fixed
   when the gRPC client is created and do not yet carry the framework token. This includes Rapid (zonal) reads
   and writes, Rapid Cache Ultra reads (and writes when ``rcu_rapid_write=True``), and Storage Control API calls
   (bucket layout lookups and HNS folder operations such as ``mkdir``, ``rmdir`` and ``mv``).

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
2. **Thread Boundary Crossing:** In ``fsspec``, synchronous operations dispatch coroutines to a background asyncio event loop thread (``fsspecIO``). ``gcsfs`` captures caller identity on the user's thread and safely propagates it to the background event loop via Python ``contextvars``.
3. **Performance & Caching:** Detection uses lightweight stack frame inspection (via ``sys._getframe()``) capped at 64 frames, and runs once per call on the caller's thread (never on the background event loop thread). Active ``GCSFile`` / ``ZonalFile`` instances cache their framework attribution so subsequent chunk downloads and background prefetching operations execute with zero stack-walking overhead (:math:`O(1)`).

Execution Flows
~~~~~~~~~~~~~~~

All methods in ``gcsfs`` follow one of three telemetry execution paths (implemented in ``gcsfs/telemetry/wrapper.py``):

.. code-block:: text

   1. Synchronous FileSystem Call (e.g. fs.ls(), pd.read_csv())
      [Caller Thread]
        fs.ls() -> _gcs_sync_wrapper
          -> _run_sync(): walks caller stack ONCE & sets ContextVar
          -> fsspec.asyn.sync(): copies ContextVar to fsspecIO thread
      [fsspecIO Event Loop Thread]
        fs._ls() -> _gcs_async_wrapper (ContextVar already set -> O(1) no-op)
          -> _get_headers(): reads ContextVar -> attaches "fw/pandas"

   2. Native Asynchronous FileSystem Call (e.g. await fs._cat(...))
      [Event Loop Thread]
        await fs._cat() -> _gcs_async_wrapper
          -> _telemetry_context(): walks stack ONCE & sets ContextVar
          -> inner await fs._cat_file() (ContextVar already set -> O(1) no-op)
          -> _get_headers(): reads ContextVar -> attaches "fw/..."

   3. GCSFile / ZonalFile Lifecycle (e.g. with fs.open(...) as f: f.read())
      - Open (GCSFile.__init__):
          Captures self.caller_framework ONCE on the file instance.
      - Read / Write / Background Prefetch / Deferred Close (_file_telemetry_wrapper):
          Restores self.caller_framework into ContextVar for every chunk fetch,
          background prefetcher task (_async_fetch_range), or deferred close thread,
          even when the original caller stack is no longer present.

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
- Outgoing requests contain only standard ``python-gcsfs/<version>`` headers.
