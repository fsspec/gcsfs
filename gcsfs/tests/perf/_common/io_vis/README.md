# User Guide: Multi-Process I/O Pattern Visualizer

This guide shows how to trace Python storage workloads and read the interactive HTML dashboard using [io_tracer.py](file:///home/princer_google_com/c1dev/gcsfs/gcsfs/tests/perf/_common/io_vis/io_tracer.py) and [demo_hf_dataloader.py](file:///home/princer_google_com/c1dev/gcsfs/gcsfs/tests/perf/_common/io_vis/demo_hf_dataloader.py). We cover quick-start commands, how to read the visual panels, and common bottleneck patterns.

## 1. Quick Start

Run the bundled HuggingFace streaming demo or wrap any custom workload in Python:

```bash
# 1. Generate the trace HTML (4 workers, 4 Parquet shards, 1 MiB blockcache)
/home/princer_google_com/c1dev/gcsfs/.venv/bin/python \
  /home/princer_google_com/c1dev/gcsfs/gcsfs/tests/perf/_common/io_vis/demo_hf_dataloader.py \
  --num-workers 4 \
  --block-size 1048576 \
  --cache-type blockcache \
  --output /home/princer_google_com/c1dev/gcsfs/hf_dataloader_io_trace.html

# 2. Serve locally and open http://localhost:8765/hf_dataloader_io_trace.html in your browser
python3 -m http.server 8765 --directory /home/princer_google_com/c1dev/gcsfs
# 2. Run the real GCS HuggingFace subsystem benchmark with I/O tracing enabled
HF_HUB_OFFLINE=1 HF_DATASETS_OFFLINE=1 \
GCSFS_IO_TRACE_DIR_OUT=/home/princer_google_com/c1dev/gcsfs/io_traces \
/home/princer_google_com/c1dev/gcsfs/.venv/bin/python -m gcsfs.tests.perf.subsystembenchmarks.run \
  --group=dataloading/huggingface_datasets \
  --filter="read-hf-txpq-shuf-nw4-rg1800-fc256x10920-splitws8div-mbis10-reg and not pf4" \
  --bucket-prefix=io-trace-hf \
  --project=gcs-aiml-clients-testing-101 \
  --location=us-central1 \
  --amplification-wait=0

# 3. Serve locally and open http://localhost:8765/ in your browser
/home/princer_google_com/c1dev/gcsfs/.venv/bin/python -m http.server 8765 --directory /home/princer_google_com/c1dev/gcsfs/io_traces
```

To trace your own code in Python:

```python
from gcsfs.tests.perf._common.io_vis.io_tracer import trace_io

with trace_io("my_trace.html", title="My Workload"):
    run_workload()
```

## 2. How to Read the Dashboard

The dashboard compares two I/O layers across every process identifier:

* Logical reads (solid boxes) show the exact byte range requested by your code or PyArrow.
* Wire fetches (translucent dashed boxes) show the actual byte range fetched from storage or the read-ahead cache.

The page is organized into three time-synchronized panels sharing the horizontal time axis:

1. Space-Time Offset Map: The vertical axis splits into file swimlanes (byte 0 at the bottom, file size at the top), and color indicates the process identifier. Drag a box across the canvas to zoom into time and byte offset, scroll the mouse wheel to zoom time, or hover over any box to see its exact offset, length, duration, and cache hit status.
2. Per-PID Timeline: Each worker process gets one row (top half = wire fetches, bottom half = logical reads). Empty horizontal gaps show time spent outside storage on CPU decoding, batch collation, or worker startup.
3. Throughput & Summary Tables: Plots instantaneous wire vs. logical megabytes per second and breaks down read amplification (wire bytes divided by logical bytes) and cache hit rate per process and per file.

## 3. Four Bottlenecks to Look For

* Tall translucent dashed box with a tiny solid bar inside: Read amplification. The cache fetched a large block (for example, 1 megabyte), but the caller only used a small slice before seeking away or closing the file.
* Thin bars repeated at the very top of a file lane: Repeated Parquet footer seeks. Workers are opening a new file handle per row group instead of reusing an open reader.
* Multiple colors overlapping in the same file lane and byte range: Cross-worker contention. Multiple workers are redundantly reading the same shard or metadata block.
* Long empty gap between main process reads and worker reads: Worker spawn and initialization overhead before batch streaming starts.

## 4. Configuration Parameters

| Parameter | Description | Default |
| :--- | :--- | :--- |
| output_html | Output path for the standalone HTML file | io_trace.html |
| title | Dashboard header title | Workload I/O Pattern Trace |
| local_block_size | Simulated wire block size in bytes for local files (0 to disable) | 524,288 (512 KiB) |
| local_cache_type | Cache policy for local simulation (readahead, blockcache, bytes, none) | readahead |
| simulated_wire_latency_ms | Simulated network latency per local wire fetch | 1.5 ms |
