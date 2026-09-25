#!/usr/bin/env python3
"""
Run multi-process 10GB GCS read benchmarks across 1, 2, 4, 8, 16, 32, 48 processes,
taking the average over 3 interleaved runs per configuration.
"""

import argparse
import json
import os
import subprocess
import sys
from typing import List, Dict, Any


def run_single(python_bin: str, script_path: str, processes: int, backend: str, url_pattern: str) -> Dict[str, Any]:
    cmd = [
        python_bin,
        script_path,
        "--processes", str(processes),
        "--backend", backend,
        "--url-pattern", url_pattern,
        "--json",
    ]
    env = dict(os.environ)
    env["MALLOC_ARENA_MAX"] = "4"
    res = subprocess.run(cmd, capture_output=True, text=True, env=env, check=True)
    for line in res.stdout.strip().splitlines():
        line = line.strip()
        if line.startswith("{") and line.endswith("}"):
            return json.loads(line)
    raise ValueError(f"Could not parse JSON from output:\nStdout:\n{res.stdout}\nStderr:\n{res.stderr}")


def main():
    parser = argparse.ArgumentParser(description="Multi-process benchmark orchestrator")
    parser.add_argument("--runs", type=int, default=3, help="Number of runs per configuration")
    parser.add_argument("--procs", default="1,4,16,24,32,40,48", help="Comma-separated list of process counts")

    parser.add_argument("--backends", default="rust,fsspec", help="Comma-separated list of backends")
    parser.add_argument("--url-pattern", default="gs://princer-bucket/test_10g/file_{}.bin", help="URL pattern")
    parser.add_argument("--output-json", default="/tmp/multiprocess_3runs_results.json", help="Path to save output JSON")
    args = parser.parse_args()

    procs_list = [int(p) for p in args.procs.split(",")]
    backends = [b.strip() for b in args.backends.split(",")]
    python_bin = sys.executable
    script_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bench_multiprocess.py")

    print("=" * 100)
    print("GCSFS Multi-Process 10GB Read Benchmark (Averaged Over Runs)")
    print(f"Process counts : {procs_list}")
    print(f"Backends       : {backends}")
    print(f"Runs per config: {args.runs} (interleaved)")
    print(f"URL Pattern    : {args.url_pattern}")
    print("=" * 100)
    print()

    all_data = []

    for p in procs_list:
        print(f"\n====================== Processes: {p:2d} ({p * 10} GiB total) ======================")
        for run_idx in range(1, args.runs + 1):
            print(f"--- Run {run_idx}/{args.runs} ---")
            for b in backends:
                print(f"  Running [{b.upper()}] (p={p}, run={run_idx})... ", end="", flush=True)
                data = run_single(python_bin, script_path, p, b, args.url_pattern)
                data["run_idx"] = run_idx
                all_data.append(data)
                print(f"Done in {data['wall_elapsed']:5.2f}s -> {data['agg_throughput_mbps']:8.2f} MB/s | "
                      f"Peak Agg RSS: {data['peak_agg_rss_mb']:7.1f} MB (per-proc: {data['peak_proc_rss_mb']:5.1f} MB) | "
                      f"CPU: {data['cpu_percent']:6.1f}%")

    with open(args.output_json, "w") as f:
        json.dump(all_data, f, indent=2)

    print("\n" + "=" * 100)
    print(f"SUMMARY: Mean Across {args.runs} Runs Per Configuration")
    print("=" * 100)
    print(f"| Workers / Files | Total Data Read | Backend | Elapsed Time | Aggregate Throughput | Speedup | Peak Agg RSS | Per-Proc RSS | Total CPU% | Total CPU Time |")
    print(f"|:---:|:---:|:---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|")

    summary_by_p = {}
    for p in procs_list:
        summary_by_p[p] = {}
        for b in backends:
            runs_for_bp = [d for d in all_data if d["processes"] == p and d["backend"] == b]
            n = len(runs_for_bp)
            avg_elapsed = sum(d["wall_elapsed"] for d in runs_for_bp) / n
            avg_mbps = sum(d["agg_throughput_mbps"] for d in runs_for_bp) / n
            min_mbps = min(d["agg_throughput_mbps"] for d in runs_for_bp)
            max_mbps = max(d["agg_throughput_mbps"] for d in runs_for_bp)
            avg_agg_rss = sum(d["peak_agg_rss_mb"] for d in runs_for_bp) / n
            avg_proc_rss = sum(d["peak_proc_rss_mb"] for d in runs_for_bp) / n
            avg_cpu = sum(d["cpu_percent"] for d in runs_for_bp) / n
            avg_cpu_time = sum(d["user_time"] + d["sys_time"] for d in runs_for_bp) / n
            total_gib = runs_for_bp[0]["total_bytes"] / (1024 ** 3)

            summary_by_p[p][b] = {
                "gib": total_gib,
                "elapsed": avg_elapsed,
                "mbps": avg_mbps,
                "min_mbps": min_mbps,
                "max_mbps": max_mbps,
                "agg_rss": avg_agg_rss,
                "proc_rss": avg_proc_rss,
                "cpu": avg_cpu,
                "cpu_time": avg_cpu_time,
            }

    for p in procs_list:
        fsspec_mbps = summary_by_p[p]["fsspec"]["mbps"]
        rust_data = summary_by_p[p]["rust"]
        fsspec_data = summary_by_p[p]["fsspec"]

        rust_speedup = rust_data["mbps"] / fsspec_mbps if fsspec_mbps > 0 else 1.0

        # Format RSS
        def format_rss(mb):
            return f"{mb / 1024:.2f} GB" if mb >= 1024 else f"{mb:.1f} MB"

        print(f"| **{p}** | {rust_data['gib']:.0f} GiB | **rust** | **{rust_data['elapsed']:.2f}s** | "
              f"**{rust_data['mbps']:.1f} MB/s** ({rust_data['min_mbps']:.0f}–{rust_data['max_mbps']:.0f}) | "
              f"**{rust_speedup:.2f}x** | **{format_rss(rust_data['agg_rss'])}** | **{rust_data['proc_rss']:.1f} MB** | "
              f"{rust_data['cpu']:.1f}% | {rust_data['cpu_time']:.1f}s |")
        print(f"| | | fsspec | {fsspec_data['elapsed']:.2f}s | "
              f"{fsspec_data['mbps']:.1f} MB/s ({fsspec_data['min_mbps']:.0f}–{fsspec_data['max_mbps']:.0f}) | "
              f"1.00x | {format_rss(fsspec_data['agg_rss'])} | {fsspec_data['proc_rss']:.1f} MB | "
              f"{fsspec_data['cpu']:.1f}% | {fsspec_data['cpu_time']:.1f}s |")


if __name__ == "__main__":
    main()
