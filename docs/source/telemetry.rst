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

   User-Agent: gcsfs/2026.3.0 fsspec/2026.3.0 fw/pandas

Recognized Frameworks
~~~~~~~~~~~~~~~~~~~~~

The detector recognizes major ecosystem libraries, including:

- **Data Processing & Analytics:** ``pandas`` (``fw/pandas``), ``dask`` (``fw/dask``), ``ray`` (``fw/ray``), ``pyspark`` (``fw/spark``), ``pyarrow`` (``fw/pyarrow``), ``duckdb`` (``fw/duckdb``), ``xarray`` (``fw/xarray``), ``fastparquet`` (``fw/fastparquet``).
- **Machine Learning & Deep Learning:** ``torch`` (``fw/torch``), ``torchdata`` (``fw/torchdata``), ``lightning`` / ``pytorch_lightning`` (``fw/lightning``), ``datasets`` (``fw/datasets``), ``transformers`` (``fw/transformers``), ``orbax`` (``fw/orbax``), ``tensorstore`` (``fw/tensorstore``), ``tensorflow`` (``fw/tensorflow``), ``jax`` (``fw/jax``).

If no recognized framework is present on the call stack (e.g. pure Python scripts or unrecognized utilities), no framework token is added to the header.

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
3. **Performance & Caching:** Detection uses lightweight stack frame inspection (via ``sys._getframe()``) capped at shallow depths. Active ``GCSFile`` instances cache their framework attribution so subsequent chunk downloads and background prefetching operations execute with zero stack-walking overhead (:math:`O(1)`).

Disabling Telemetry
-------------------

Telemetry collection is enabled by default, but you can opt out at any time by setting an environment variable.

Using Environment Variables
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Set ``GCSFS_NO_TELEMETRY=true`` in your shell environment:

.. code-block:: bash

   export GCSFS_NO_TELEMETRY=true

In Python Code
~~~~~~~~~~~~~~

Set the environment variable before importing ``gcsfs`` or executing storage calls:

.. code-block:: python

   import os
   os.environ["GCSFS_NO_TELEMETRY"] = "true"

   import gcsfs

When disabled:

- All stack frame inspections and detector evaluations are skipped immediately.
- Outgoing requests contain only standard ``gcsfs/<version>`` and ``fsspec/<version>`` headers.
