#!/usr/bin/env python3
"""
Validation Benchmark: Run only gcsfs_http with 48 processes.
Does NOT modify any documentation or saved results.
Reads 10 GiB per process across 48 processes (480 GiB total).
16 MB chunk I/O, Concurrency 16, 256 MB Readahead.
"""

import multiprocessing as mp
import os
import sys
import time

# Ensure gcsfs in workspace is prioritized
WORKSPACE_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if WORKSPACE_ROOT not in sys.path:
    sys.path.insert(0, WORKSPACE_ROOT)

import gcsfs

BUCKET = "princer-bucket"
NUM_WORKERS = 48
CHUNK_SIZE = 16 * 1024 * 1024        # 16 MB
FILE_SIZE = 10 * 1024 * 1024 * 1024   # 10 GiB
CONCURRENCY = 2                      # 16 range requests per file
MAX_PREFETCH_SIZE = 128 * 1024 * 1024 # 256 MB readahead buffer


from concurrent.futures import ProcessPoolExecutor, as_completed

def worker_fn(worker_id: int) -> dict:
    # Ensure fresh GCSFileSystem and event loop inside each worker process
    os.environ["GCSFS_READ_BACKEND"] = "http"
    fs = gcsfs.GCSFileSystem(read_backend="http")
    path = f"{BUCKET}/test_10g/file_{worker_id}.bin"

    t0 = time.perf_counter()
    total_bytes = 0
    with fs.open(
        path,
        "rb",
        concurrency=CONCURRENCY,
        max_prefetch_size=MAX_PREFETCH_SIZE,
    ) as f:
        while total_bytes < FILE_SIZE:
            to_read = min(CHUNK_SIZE, FILE_SIZE - total_bytes)
            chunk = f.read(to_read)
            if not chunk:
                break
            total_bytes += len(chunk)

    elapsed = time.perf_counter() - t0
    return {
        "worker_id": worker_id,
        "bytes": total_bytes,
        "elapsed": elapsed,
        "mbps": (total_bytes / (1024 * 1024)) / elapsed if elapsed > 0 else 0,
    }


def main():
    print("=" * 90)
    print(f" VALIDATION RUN: gcsfs_http (ProcessPoolExecutor) | 48 Processes | 10 GiB / worker")
    print(f" Chunk Size: {CHUNK_SIZE/(1024*1024):.0f}MB | Concurrency: {CONCURRENCY} | Readahead: {MAX_PREFETCH_SIZE/(1024*1024):.0f}MB")
    print("=" * 90)

    ctx = mp.get_context("spawn")
    t_wall_start = time.perf_counter()

    with ProcessPoolExecutor(max_workers=NUM_WORKERS, mp_context=ctx) as executor:
        futures = [executor.submit(worker_fn, i) for i in range(NUM_WORKERS)]
        results = [fut.result() for fut in as_completed(futures)]

    t_wall_end = time.perf_counter()
    wall_elapsed = t_wall_end - t_wall_start

    total_bytes = sum(r["bytes"] for r in results)
    total_gib = total_bytes / (1024 ** 3)
    agg_mbps = (total_bytes / (1024 * 1024)) / wall_elapsed if wall_elapsed > 0 else 0
    line_gbps = (total_bytes * 8) / (wall_elapsed * 1e9) if wall_elapsed > 0 else 0

    print("\n" + "=" * 90)
    print(" RESULTS")
    print("=" * 90)
    print(f" Total Transferred : {total_gib:.2f} GiB")
    print(f" Wall Time         : {wall_elapsed:.2f} s")
    print(f" Agg Throughput    : {agg_mbps:,.2f} MB/s ({agg_mbps/1024:.2f} GB/s)")
    print(f" Line Rate         : {line_gbps:.2f} Gbps")
    print("=" * 90)


if __name__ == "__main__":
    main()

