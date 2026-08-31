#!/usr/bin/env python3
"""
Benchmark multi-process GCS read performance comparing fsspec vs rust backend.
Each worker process opens and reads a distinct file from GCS.
"""

import argparse
import multiprocessing as mp
import os
import sys
import time
import threading
from typing import List, Dict, Any


def get_process_rss_kb(pid: int) -> int:
    """Read VmRSS from /proc/{pid}/status in KB."""
    try:
        with open(f"/proc/{pid}/status", "r") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1])
    except (FileNotFoundError, ProcessLookupError, IndexError, ValueError, PermissionError):
        return 0
    return 0


def worker_proc(
    proc_idx: int,
    url: str,
    backend: str,
    io_size: int,
    block_size: int,
    concurrency: int,
    max_prefetch: int,
    bytes_limit: int | None,
    barrier: Any,
    result_queue: Any,
) -> None:
    if backend == "rust":
        os.environ["GCSFS_READ_BACKEND"] = "rust"
    else:
        os.environ["GCSFS_READ_BACKEND"] = "http"

    import fsspec

    open_kwargs = {}
    if block_size:
        open_kwargs["block_size"] = block_size
    if concurrency:
        open_kwargs["concurrency"] = concurrency
    if max_prefetch:
        open_kwargs["max_prefetch_size"] = max_prefetch

    pid = os.getpid()

    # Wait for all processes to be ready
    barrier.wait()

    t0 = time.perf_counter()
    total_bytes = 0
    peak_rss_kb = 0

    with fsspec.open(url, "rb", **open_kwargs) as f:
        while True:
            if bytes_limit is not None and total_bytes >= bytes_limit:
                break
            to_read = io_size
            if bytes_limit is not None:
                to_read = min(io_size, bytes_limit - total_bytes)
                if to_read <= 0:
                    break

            chunk = f.read(to_read)
            if not chunk:
                break
            total_bytes += len(chunk)

    elapsed = time.perf_counter() - t0
    rss_kb = get_process_rss_kb(pid)
    peak_rss_kb = max(peak_rss_kb, rss_kb)

    result_queue.put({
        "proc_idx": proc_idx,
        "url": url,
        "bytes_read": total_bytes,
        "elapsed": elapsed,
        "peak_rss_mb": peak_rss_kb / 1024.0,
    })


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


def run_benchmark(args) -> Dict[str, Any]:
    ctx = mp.get_context("spawn")
    barrier = ctx.Barrier(args.processes + 1)
    result_queue = ctx.Queue()

    processes = []
    for i in range(args.processes):
        url = args.url_pattern.format(i)
        p = ctx.Process(
            target=worker_proc,
            args=(
                i,
                url,
                args.backend,
                args.io_size,
                args.block_size,
                args.concurrency,
                args.max_prefetch_size,
                args.bytes_per_process,
                barrier,
                result_queue,
            ),
        )
        processes.append(p)
        p.start()

    pids = [p.pid for p in processes]
    sampler = MemorySampler(pids, interval=0.05)
    sampler.start()

    # Synchronize start
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
    agg_throughput_mbps = (total_bytes / (1024 * 1024)) / wall_elapsed if wall_elapsed > 0 else 0

    # CPU metrics (user + sys across children and self)
    user_time = (t1_cpu.user + t1_cpu.children_user) - (t0_cpu.user + t0_cpu.children_user)
    sys_time = (t1_cpu.system + t1_cpu.children_system) - (t0_cpu.system + t0_cpu.children_system)
    total_cpu_time = user_time + sys_time
    cpu_percent = (total_cpu_time / wall_elapsed) * 100.0 if wall_elapsed > 0 else 0

    peak_agg_rss_mb = sampler.peak_aggregate_rss_kb / 1024.0
    peak_proc_rss_mb = sampler.peak_per_proc_rss_kb / 1024.0

    return {
        "processes": args.processes,
        "backend": args.backend,
        "total_bytes": total_bytes,
        "wall_elapsed": wall_elapsed,
        "agg_throughput_mbps": agg_throughput_mbps,
        "peak_agg_rss_mb": peak_agg_rss_mb,
        "peak_proc_rss_mb": peak_proc_rss_mb,
        "cpu_percent": cpu_percent,
        "user_time": user_time,
        "sys_time": sys_time,
        "worker_results": sorted(results, key=lambda x: x["proc_idx"]),
    }


def main():
    parser = argparse.ArgumentParser(description="Multi-process GCS read benchmark")
    parser.add_argument("--processes", "-p", type=int, default=1, help="Number of worker processes")
    parser.add_argument("--url-pattern", default="gs://princer-bucket/test_10g/file_{}.bin", help="GCS URL pattern with {} for index")
    parser.add_argument("--backend", choices=["rust", "fsspec"], default="rust", help="Read backend")
    parser.add_argument("--io-size", type=int, default=8 * 1024 * 1024, help="Chunk read size in bytes (default: 8MiB)")
    parser.add_argument("--block-size", type=int, default=5 * 1024 * 1024, help="Block size (default: 5MiB)")
    parser.add_argument("--concurrency", type=int, default=16, help="Range request concurrency per read (default: 16)")
    parser.add_argument("--max-prefetch-size", type=int, default=256 * 1024 * 1024, help="Readahead ceiling (default: 256MiB)")
    parser.add_argument("--bytes-per-process", type=int, default=None, help="Bytes limit per process (default: None, reads full 10GB file)")

    args = parser.parse_args()

    res = run_benchmark(args)
    gib = res["total_bytes"] / (1024 ** 3)
    print(f"[{res['backend'].upper()}] Processes: {res['processes']:2d} | Read {gib:6.1f} GiB in {res['wall_elapsed']:6.2f}s | "
          f"Throughput: {res['agg_throughput_mbps']:8.2f} MB/s | "
          f"Peak Agg RSS: {res['peak_agg_rss_mb']:7.1f} MB (per-proc: {res['peak_proc_rss_mb']:5.1f} MB) | "
          f"CPU: {res['cpu_percent']:6.1f}% (User: {res['user_time']:.1f}s, Sys: {res['sys_time']:.1f}s)")


if __name__ == "__main__":
    main()
