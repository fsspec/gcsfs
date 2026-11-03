Workload I/O Tracer & Visualizer
================================

``gcsfs.iotracer`` is a zero-code-change diagnostic subsystem and CLI runner (``gcsfs-iotrace``) designed to trace, analyze, and visualize I/O access patterns across multi-process Python workloads (such as PyTorch DataLoader workers, Hugging Face streaming datasets, and Dask workers).

It captures both:

* **Logical Application Reads**: ``f.read()``, ``f.readline()``, and ``f.readinto()`` with seek offsets, requested lengths, timestamps, process/thread IDs, and buffer cache hit states.
* **Storage Wire Range Requests**: Underlying GCS HTTP/gRPC Range requests (``_cat_file_sequential``, ``ZonalFile._fetch_range``) with wire durations, byte ranges, response headers (such as ``x-goog-request-id``), and wire origins (cache miss vs. async background prefetch).

Tracing outputs a standalone interactive HTML5 Canvas dashboard with time-zoom, space-time file offset scatter plots, worker Gantt timelines, bandwidth curves, and summary KPI cards, as well as optional Chrome Trace Event (``trace.json``) export for `ui.perfetto.dev <https://ui.perfetto.dev>`_.

Quick Start
-----------

1. Zero-Code-Change CLI Runner
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Run any Python training or data ingestion script through ``gcsfs-iotrace``:

.. code-block:: bash

   gcsfs-iotrace run train.py --batch-size 32

Or using the Python module syntax:

.. code-block:: bash

   python -m gcsfs.iotracer train.py --batch-size 32

By default, this runs the script, captures all parent and spawned/forked child process I/O into a local temporary directory, and outputs ``io_trace.html`` alongside ``io_trace.summary.json``.

Export to Chrome Trace (Perfetto):

.. code-block:: bash

   gcsfs-iotrace run --export-trace trace.json train.py

2. Environment Variable Invocation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In distributed or containerized runtimes (e.g. Kubeflow, Slurm, Ray) where wrapping the launch command is inconvenient, enable tracing via environment variable:

.. code-block:: bash

   export GCSFS_IO_TRACE=1
   python train.py

Child processes automatically inherit tracing, and report synthesis runs upon workload completion.

3. Programmatic Context Manager
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To trace a specific code segment in a script or unit test:

.. code-block:: python

   import gcsfs.iotracer

   with gcsfs.iotracer.trace_io(output_html="dataloader_trace.html"):
       for batch in dataloader:
           train_step(batch)

4. Post-Mortem Report Generation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If raw per-process JSONL trace logs were retained, re-render dashboards anytime:

.. code-block:: bash

   gcsfs-iotrace report /tmp/gcsfs_iotrace_run/ --output-html report.html

Architecture & Performance Protections
--------------------------------------

* **Sequential Cache-Hit Coalescing**: Consecutive cache-hit reads on the same file within a 2 ms window are coalesced in-memory, cutting JSONL write overhead by up to 90% on tight record-parsing loops.
* **Durability Flush Triggers**: Buffered 64 KiB I/O commits to disk on wire completion, file closure, or 1,000 logical events, ensuring zero dropped wire events when PyTorch workers terminate via ``os._exit(0)``.
* **Visual LOD Decimation**: The standalone visualizer downsamples scatter plots to $\le 20,000$ representative points, keeping browser dashboards responsive even with runs generating millions of events.
