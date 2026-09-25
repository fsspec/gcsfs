#!/usr/bin/env python3
"""
Benchmark 16MB IO: Multi-Threading vs Multi-Processing directly through gcsfs.

Compares 3 backends:
1. gcsfs existing backend (read_backend='http' via aiohttp)
2. gcsfs with Rust backend + HTTP JSON (read_backend='rust', rust_transport='http')
3. gcsfs with Rust backend + gRPC Cloud-Path (read_backend='rust', rust_transport='grpc', 48 channels)

Reads distinct 10 GiB files (gs://princer-bucket/test_10g/file_{i}.bin) per worker.
"""

import argparse
import concurrent.futures
import json
import multiprocessing as mp
import os
import sys
import threading
import time
from typing import Dict, Any, List

# Ensure gcsfs in workspace is prioritized
WORKSPACE_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if WORKSPACE_ROOT not in sys.path:
    sys.path.insert(0, WORKSPACE_ROOT)

import gcsfs


def get_process_rss_kb(pid: int) -> int:
    try:
        with open(f"/proc/{pid}/status", "r") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1])
    except (FileNotFoundError, ProcessLookupError, IndexError, ValueError, PermissionError):
        return 0
    return 0


class MemorySampler(threading.Thread):
    def __init__(self, pids: List[int], interval: float = 0.05):
        super().__init__(daemon=True)
        self.pids = pids
        self.interval = interval
        self.stop_event = threading.Event()
        self.peak_aggregate_rss_kb = 0
        self.peak_per_proc_rss_kb = 0

    def run(self):
        while not self.stop_event.is_set():
            agg = 0
            for pid in self.pids:
                rss = get_process_rss_kb(pid)
                agg += rss
                if rss > self.peak_per_proc_rss_kb:
                    self.peak_per_proc_rss_kb = rss
            if agg > self.peak_aggregate_rss_kb:
                self.peak_aggregate_rss_kb = agg
            time.sleep(self.interval)

    def stop(self):
        self.stop_event.set()


def create_gcsfs_instance(backend_type: str, channels: int = 48) -> gcsfs.GCSFileSystem:
    if backend_type == "gcsfs_http":
        os.environ["GCSFS_READ_BACKEND"] = "http"
        return gcsfs.GCSFileSystem(read_backend="http")
    elif backend_type == "rust_http":
        os.environ["GCSFS_READ_BACKEND"] = "rust"
        os.environ["GCSFS_RUST_TRANSPORT"] = "http"
        return gcsfs.GCSFileSystem(read_backend="rust", rust_transport="http")
    elif backend_type == "rust_grpc":
        os.environ["GCSFS_READ_BACKEND"] = "rust"
        os.environ["GCSFS_RUST_TRANSPORT"] = "grpc"
        os.environ["GCSFS_RUST_CHANNELS"] = str(channels)
        return gcsfs.GCSFileSystem(read_backend="rust", rust_transport="grpc")
    else:
        raise ValueError(f"Unknown backend_type: {backend_type}")


def read_file_prefetch(
    fs: gcsfs.GCSFileSystem,
    bucket: str,
    object_name: str,
    chunk_size: int,
    file_size: int,
    concurrency: int = 16,
    max_prefetch_size: int = 256 * 1024 * 1024,
) -> int:
    path = f"{bucket}/{object_name}"
    total_bytes = 0
    with fs.open(
        path,
        "rb",
        concurrency=concurrency,
        max_prefetch_size=max_prefetch_size,
    ) as f:
        while total_bytes < file_size:
            to_read = min(chunk_size, file_size - total_bytes)
            chunk = f.read(to_read)
            if not chunk:
                break
            total_bytes += len(chunk)
    return total_bytes


# ---------------------------------------------------------------------------
# Multi-Threading Worker
# ---------------------------------------------------------------------------
def _thread_worker(
    worker_idx: int,
    fs: gcsfs.GCSFileSystem,
    bucket: str,
    object_name: str,
    chunk_size: int,
    file_size: int,
    concurrency: int,
    max_prefetch_size: int,
    barrier: threading.Barrier,
) -> Dict[str, Any]:
    barrier.wait()
    t0 = time.perf_counter()
    nbytes = read_file_prefetch(
        fs,
        bucket,
        object_name,
        chunk_size,
        file_size,
        concurrency=concurrency,
        max_prefetch_size=max_prefetch_size,
    )
    elapsed = time.perf_counter() - t0
    return {
        "worker_idx": worker_idx,
        "bytes_read": nbytes,
        "elapsed": elapsed,
        "throughput_mbps": (nbytes / (1024 * 1024)) / elapsed if elapsed > 0 else 0,
    }


def run_multi_threading(
    workers: int,
    backend_type: str,
    bucket: str,
    channels: int,
    chunk_size: int,
    file_size: int,
    concurrency: int = 16,
    max_prefetch_size: int = 256 * 1024 * 1024,
) -> Dict[str, Any]:
    fs = create_gcsfs_instance(backend_type, channels=channels)
    barrier = threading.Barrier(workers + 1)
    pids = [os.getpid()]
    sampler = MemorySampler(pids, interval=0.05)
    sampler.start()

    futures = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        for i in range(workers):
            obj_name = f"test_10g/file_{i}.bin" if workers > 1 else "10gfile.bin"
            f = executor.submit(
                _thread_worker,
                i,
                fs,
                bucket,
                obj_name,
                chunk_size,
                file_size,
                concurrency,
                max_prefetch_size,
                barrier,
            )
            futures.append(f)

        t0_cpu = os.times()
        t0_wall = time.perf_counter()
        barrier.wait()

        results = [f.result() for f in futures]

        t1_wall = time.perf_counter()
        t1_cpu = os.times()

    sampler.stop()
    sampler.join()

    wall_elapsed = t1_wall - t0_wall
    total_bytes = sum(r["bytes_read"] for r in results)
    gib_read = total_bytes / (1024 ** 3)
    agg_throughput_mbps = (total_bytes / (1024 * 1024)) / wall_elapsed if wall_elapsed > 0 else 0

    user_time = t1_cpu.user - t0_cpu.user
    sys_time = t1_cpu.system - t0_cpu.system
    total_cpu_time = user_time + sys_time
    cpu_percent = (total_cpu_time / wall_elapsed) * 100.0 if wall_elapsed > 0 else 0
    cpu_s_per_gib = total_cpu_time / gib_read if gib_read > 0 else 0

    peak_agg_rss_mb = sampler.peak_aggregate_rss_kb / 1024.0

    return {
        "mode": "multi-threading",
        "backend": backend_type,
        "workers": workers,
        "channels": channels if backend_type == "rust_grpc" else 1,
        "chunk_size": chunk_size,
        "total_bytes": total_bytes,
        "gib_read": gib_read,
        "wall_elapsed": wall_elapsed,
        "agg_throughput_mbps": agg_throughput_mbps,
        "throughput_gbps": (total_bytes * 8.0) / (wall_elapsed * 1e9),
        "peak_agg_rss_mb": peak_agg_rss_mb,
        "peak_per_worker_rss_mb": peak_agg_rss_mb / workers,
        "user_time": user_time,
        "sys_time": sys_time,
        "total_cpu_time": total_cpu_time,
        "cpu_percent": cpu_percent,
        "cpu_s_per_gib": cpu_s_per_gib,
    }


# ---------------------------------------------------------------------------
# Multi-Processing Worker
# ---------------------------------------------------------------------------
def _process_worker(
    worker_idx: int,
    backend_type: str,
    bucket: str,
    object_name: str,
    channels: int,
    chunk_size: int,
    file_size: int,
    concurrency: int,
    max_prefetch_size: int,
    barrier: Any,
    result_queue: Any,
):
    pid = os.getpid()
    fs = create_gcsfs_instance(backend_type, channels=channels)
    barrier.wait()

    t0 = time.perf_counter()
    nbytes = read_file_prefetch(
        fs,
        bucket,
        object_name,
        chunk_size,
        file_size,
        concurrency=concurrency,
        max_prefetch_size=max_prefetch_size,
    )
    elapsed = time.perf_counter() - t0

    result_queue.put({
        "worker_idx": worker_idx,
        "pid": pid,
        "bytes_read": nbytes,
        "elapsed": elapsed,
        "throughput_mbps": (nbytes / (1024 * 1024)) / elapsed if elapsed > 0 else 0,
    })


def run_multi_processing(
    workers: int,
    backend_type: str,
    bucket: str,
    channels: int,
    chunk_size: int,
    file_size: int,
    concurrency: int = 16,
    max_prefetch_size: int = 256 * 1024 * 1024,
) -> Dict[str, Any]:
    ctx = mp.get_context("spawn")
    barrier = ctx.Barrier(workers + 1)
    result_queue = ctx.Queue()

    processes = []
    for i in range(workers):
        obj_name = f"test_10g/file_{i}.bin" if workers > 1 else "10gfile.bin"
        p = ctx.Process(
            target=_process_worker,
            args=(
                i,
                backend_type,
                bucket,
                obj_name,
                channels,
                chunk_size,
                file_size,
                concurrency,
                max_prefetch_size,
                barrier,
                result_queue,
            ),
        )
        processes.append(p)
        p.start()

    pids = [p.pid for p in processes]
    sampler = MemorySampler(pids, interval=0.05)
    sampler.start()

    t0_cpu = os.times()
    t0_wall = time.perf_counter()
    barrier.wait()

    for p in processes:
        p.join()

    t1_wall = time.perf_counter()
    t1_cpu = os.times()

    sampler.stop()
    sampler.join()

    results = []
    while not result_queue.empty():
        results.append(result_queue.get())

    wall_elapsed = t1_wall - t0_wall
    total_bytes = sum(r["bytes_read"] for r in results)
    gib_read = total_bytes / (1024 ** 3)
    agg_throughput_mbps = (total_bytes / (1024 * 1024)) / wall_elapsed if wall_elapsed > 0 else 0

    user_time = (t1_cpu.user + t1_cpu.children_user) - (t0_cpu.user + t0_cpu.children_user)
    sys_time = (t1_cpu.system + t1_cpu.children_system) - (t0_cpu.system + t0_cpu.children_system)
    total_cpu_time = user_time + sys_time
    cpu_percent = (total_cpu_time / wall_elapsed) * 100.0 if wall_elapsed > 0 else 0
    cpu_s_per_gib = total_cpu_time / gib_read if gib_read > 0 else 0

    peak_agg_rss_mb = sampler.peak_aggregate_rss_kb / 1024.0
    peak_proc_rss_mb = sampler.peak_per_proc_rss_kb / 1024.0

    return {
        "mode": "multi-processing",
        "backend": backend_type,
        "workers": workers,
        "channels": channels if backend_type == "rust_grpc" else 1,
        "chunk_size": chunk_size,
        "total_bytes": total_bytes,
        "gib_read": gib_read,
        "wall_elapsed": wall_elapsed,
        "agg_throughput_mbps": agg_throughput_mbps,
        "throughput_gbps": (total_bytes * 8.0) / (wall_elapsed * 1e9),
        "peak_agg_rss_mb": peak_agg_rss_mb,
        "peak_per_worker_rss_mb": peak_proc_rss_mb,
        "user_time": user_time,
        "sys_time": sys_time,
        "total_cpu_time": total_cpu_time,
        "cpu_percent": cpu_percent,
        "cpu_s_per_gib": cpu_s_per_gib,
    }


def main():
    parser = argparse.ArgumentParser(description="Benchmark 16MB IO: Multi-Threading vs Multi-Processing through GCSFS")
    parser.add_argument("--workers", "-w", type=int, default=1, help="Number of workers (files)")
    parser.add_argument("--bucket", default="princer-bucket", help="GCS bucket")
    parser.add_argument(
        "--backend",
        choices=["gcsfs_http", "rust_http", "rust_grpc", "all"],
        default="all",
        help="Backend type: gcsfs_http (aiohttp), rust_http (Rust JSON REST), rust_grpc (Rust gRPC Cloud-Path), or all",
    )
    parser.add_argument("--channels", type=int, default=200, help="gRPC channel pool size (default 200)")
    parser.add_argument("--chunk-size", type=int, default=16 * 1024 * 1024, help="Chunk IO size in bytes (16MB)")
    parser.add_argument("--size-bytes", type=int, default=10 * 1024 * 1024 * 1024, help="Bytes per file (default 10 GiB)")
    parser.add_argument("--concurrency", type=int, default=16, help="Prefetch concurrency per file (default 16)")
    parser.add_argument("--max-prefetch-size", type=int, default=256 * 1024 * 1024, help="Max prefetch readahead size in bytes (default 256MB)")
    parser.add_argument("--mode", choices=["both", "threads", "procs"], default="both", help="Execution mode")
    parser.add_argument("--json", action="store_true", help="Output JSON results")
    parser.add_argument("--output-json", type=str, default="", help="Path to save JSON results")
    args = parser.parse_args()


    backends = ["gcsfs_http", "rust_http", "rust_grpc"] if args.backend == "all" else [args.backend]
    modes = ["multi-threading", "multi-processing"] if args.mode == "both" else [
        "multi-threading" if args.mode == "threads" else "multi-processing"
    ]

    all_results = []

    print("=" * 115)
    print(f" GCSFS BENCHMARK: {args.workers} Worker(s) | {args.chunk_size / (1024*1024):.1f} MB IO | {args.size_bytes / (1024**3):.1f} GiB/worker | Prefetch Concurrency: {args.concurrency} | Readahead: {args.max_prefetch_size / (1024*1024):.0f}MB | gRPC Channels: {args.channels}")
    print("=" * 115)

    for b in backends:
        for m in modes:
            print(f"Running [{b.upper()}] with [{m.upper()}] ({args.workers} workers)...", flush=True)
            if m == "multi-threading":
                res = run_multi_threading(
                    workers=args.workers,
                    backend_type=b,
                    bucket=args.bucket,
                    channels=args.channels,
                    chunk_size=args.chunk_size,
                    file_size=args.size_bytes,
                    concurrency=args.concurrency,
                    max_prefetch_size=args.max_prefetch_size,
                )
            else:
                res = run_multi_processing(
                    workers=args.workers,
                    backend_type=b,
                    bucket=args.bucket,
                    channels=args.channels,
                    chunk_size=args.chunk_size,
                    file_size=args.size_bytes,
                    concurrency=args.concurrency,
                    max_prefetch_size=args.max_prefetch_size,
                )
            all_results.append(res)
            print(
                f"  -> Read {res['gib_read']:5.1f} GiB in {res['wall_elapsed']:6.2f}s | "
                f"Throughput: {res['agg_throughput_mbps']:8.2f} MB/s ({res['throughput_gbps']:5.2f} Gbps) | "
                f"Peak RSS: {res['peak_agg_rss_mb']:7.1f} MB | CPU: {res['cpu_percent']:6.1f}% ({res['cpu_s_per_gib']:4.2f} s/GiB)"
            )

    if args.json:
        print(json.dumps(all_results, indent=2))

    if args.output_json:
        with open(args.output_json, "w") as f:
            json.dump(all_results, f, indent=2)
        print(f"Results saved to {args.output_json}")


    print("\n" + "=" * 115)
    print(" SUMMARY COMPARISON TABLE")
    print("=" * 115)
    print(f"| {'Backend':<14} | {'Architecture':<18} | {'Workers':<7} | {'Read (GiB)':<10} | {'Time (s)':<8} | {'Throughput (MB/s)':<17} | {'Gbps':<6} | {'Peak RSS (MB)':<13} | {'CPU Cost (s/GiB)':<16} |")
    print(f"|{'-'*16}|{'-'*20}|{'-'*9}|{'-'*12}|{'-'*10}|{'-'*19}|{'-'*8}|{'-'*15}|{'-'*18}|")
    for r in all_results:
        print(f"| {r['backend']:<14} | {r['mode']:<18} | {r['workers']:<7} | {r['gib_read']:>8.1f}   | {r['wall_elapsed']:>6.2f}s  | {r['agg_throughput_mbps']:>15.2f}   | {r['throughput_gbps']:>5.2f}  | {r['peak_agg_rss_mb']:>12.1f}  | {r['cpu_s_per_gib']:>15.2f}  |")
    print("=" * 115)


if __name__ == "__main__":
    main()
