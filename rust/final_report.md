# Final Benchmark Report: GCSFS Multi-Threading & Multi-Processing

## Executive Summary
This report presents empirical benchmarking for `gcsfs` comparing:
1. **`gcsfs_http`**: Existing Python `aiohttp` / `fsspec` implementation.
2. **`rust_http`**: Official Google Cloud Storage Rust SDK over HTTP JSON REST.
3. **`rust_grpc`**: Official Google Cloud Storage Rust SDK over gRPC Cloud-Path (evaluated with **48 channels** and **200 channels**).

### Environment & Parameters
- **Host VM**: Google Cloud `c4-standard-192` (192 vCPUs, 708 GB RAM, `us-west4-a`).
- **Storage Target**: `gs://princer-bucket/` (files `test_10g/file_{0..47}.bin`, 10 GiB per file).
- **Application I/O Size**: 16 MB (`16,777,216` bytes) via `f.read()`.
- **Prefetch Concurrency**: 16 concurrent range fetches per file.
- **Prefetch Readahead**: 256 MB (`268,435,456` bytes) buffer window via `BackgroundPrefetcher`.
- **Evaluated Parallelism**: 1, 16, 24, and 48 worker units.

---

## 1. Multi-Threading Results (Single Process, Shared Pool)

| Workers (Threads) | Backend | Total Transferred | Elapsed Time | Aggregate Throughput | Network Bandwidth | Peak Process RSS | Total CPU % | CPU Cost (s/GiB) |
|:---:|:---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| **1 Worker** | `gcsfs_http` | 10.0 GiB | 14.03s | 730.11 MB/s | 6.12 Gbps | 437.9 MB | 97.6% (1.0 cores) | 1.37 |
| | `rust_http` | 10.0 GiB | 7.93s | **1,291.41 MB/s** | **10.83 Gbps** | 508.5 MB | 223.3% (2.2 cores) | 1.77 |
| | `rust_grpc (48 ch)` | 10.0 GiB | 6.92s | **1,479.99 MB/s** | **12.42 Gbps** | 1,135.2 MB | 431.6% (4.3 cores) | 2.99 |
| | `rust_grpc (200 ch)` | 10.0 GiB | 15.29s | 669.75 MB/s | 5.62 Gbps | 580.7 MB | 177.3% (1.8 cores) | 2.71 |
| **16 Workers** | `gcsfs_http` | 160.0 GiB | 268.31s | 610.65 MB/s | 5.12 Gbps | 2,186.8 MB | 100.6% (1.0 cores) | 1.69 |
| | `rust_http` | 160.0 GiB | 42.74s | **3,833.42 MB/s** | **32.16 Gbps** | 3,897.0 MB | 671.0% (6.7 cores) | 1.79 |
| | `rust_grpc (48 ch)` | 160.0 GiB | 43.78s | **3,741.94 MB/s** | **31.39 Gbps** | 7,673.3 MB | 901.5% (9.0 cores) | 2.47 |
| | `rust_grpc (200 ch)` | 160.0 GiB | 41.85s | **3,914.92 MB/s** | **32.84 Gbps** | 4,567.7 MB | 916.2% (9.2 cores) | 2.40 |
| **24 Workers** | `gcsfs_http` | 240.0 GiB | 388.23s | 633.03 MB/s | 5.31 Gbps | 2,925.8 MB | 100.7% (1.0 cores) | 1.63 |
| | `rust_http` | 240.0 GiB | 62.56s | **3,928.11 MB/s** | **32.95 Gbps** | 5,459.3 MB | 725.6% (7.3 cores) | 1.89 |
| | `rust_grpc (48 ch)` | 240.0 GiB | 65.15s | **3,771.94 MB/s** | **31.64 Gbps** | 10,654.9 MB | 859.4% (8.6 cores) | 2.33 |
| | `rust_grpc (200 ch)` | 240.0 GiB | 55.54s | **4,425.27 MB/s** | **37.12 Gbps** | 6,129.9 MB | 1,026.9% (10.3 cores) | 2.38 |
| **48 Workers** | `gcsfs_http` | 480.0 GiB | 673.15s | 730.17 MB/s | 6.13 Gbps | 4,928.5 MB | 100.7% (1.0 cores) | 1.41 |
| | `rust_http` | 480.0 GiB | 101.23s | **4,855.32 MB/s** | **40.73 Gbps** | 9849.7 MB | 819.4% (8.2 cores) | 1.73 |
| | `rust_grpc (48 ch)` | 480.0 GiB | 119.26s | **4,121.32 MB/s** | **34.57 Gbps** | 18,465.1 MB | 961.0% (9.6 cores) | 2.39 |
| | `rust_grpc (200 ch)` | 480.0 GiB | 107.00s | **4,593.57 MB/s** | **38.53 Gbps** | 11,055.4 MB | 986.9% (9.9 cores) | 2.20 |

---

## 2. Multi-Processing Results (Separate Processes, Independent Pools)

| Workers (Procs) | Backend | Total Transferred | Elapsed Time | Aggregate Throughput | Network Bandwidth | Peak Agg RSS | Per-Proc RSS | Total CPU % | CPU Cost (s/GiB) |
|:---:|:---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| **1 Worker** | `gcsfs_http` | 10.0 GiB | 15.63s | 655.13 MB/s | 5.50 Gbps | 519.8 MB | 519.8 MB | 98.1% | 1.53 |
| | `rust_http` | 10.0 GiB | 7.01s | **1,461.31 MB/s** | **12.26 Gbps** | 368.9 MB | 368.9 MB | 317.4% | 2.22 |
| | `rust_grpc (48 ch)` | 10.0 GiB | 6.68s | **1,534.03 MB/s** | **12.87 Gbps** | 1,006.0 MB | 1,006.0 MB | 418.3% | 2.79 |
| | `rust_grpc (200 ch)` | 10.0 GiB | 19.32s | 530.01 MB/s | 4.45 Gbps | 598.4 MB | 598.4 MB | 141.0% | 2.72 |
| **16 Workers** | `gcsfs_http` | 160.0 GiB | 24.81s | 6,604.17 MB/s | 55.40 Gbps | 6,200.5 MB | 387.5 MB | 1,406.8% | 2.18 |
| | `rust_http` | 160.0 GiB | 11.97s | **13,683.78 MB/s** | **114.79 Gbps** | 5,488.7 MB | 343.0 MB | 3,751.0% | 2.81 |
| | `rust_grpc (48 ch)` | 160.0 GiB | 19.62s | **8349.14 MB/s** | **70.04 Gbps** | 12,349.4 MB | 771.8 MB | 4,109.1% | 5.04 |
| | `rust_grpc (200 ch)` | 160.0 GiB | 25.42s | 6,444.74 MB/s | 54.06 Gbps | 8,973.6 MB | 560.8 MB | 2,675.2% | 4.25 |
| **24 Workers** | `gcsfs_http` | 240.0 GiB | 26.90s | 9,137.54 MB/s | 76.65 Gbps | 9,583.2 MB | 399.3 MB | 2,108.5% | 2.36 |
| | `rust_http` | 240.0 GiB | 13.59s | **18,084.74 MB/s** | **151.71 Gbps** | 8,061.3 MB | 335.9 MB | 5,390.3% | 3.05 |
| | `rust_grpc (48 ch)` | 240.0 GiB | 27.16s | **9,047.51 MB/s** | **75.90 Gbps** | 16,184.7 MB | 674.4 MB | 4,452.3% | 5.04 |
| | `rust_grpc (200 ch)` | 240.0 GiB | 28.89s | 8,507.83 MB/s | 71.37 Gbps | 13,574.0 MB | 565.6 MB | 4,074.2% | 4.90 |
| **48 Workers** | `gcsfs_http` | 480.0 GiB | 48.49s | 10,136.31 MB/s | 85.03 Gbps | 18,097.8 MB | 377.0 MB | 4,019.6% | 4.06 |
| | `rust_http` | 480.0 GiB | 28.00s | **17,551.20 MB/s** | **147.23 Gbps** | 16,087.0 MB | 335.1 MB | 5,904.1% | 3.44 |
| | `rust_grpc (48 ch)` | 480.0 GiB | 39.51s | **12,439.37 MB/s** | **104.35 Gbps** | 22,558.7 MB | 470.0 MB | 6,303.3% | 5.19 |
| | `rust_grpc (200 ch)` | 480.0 GiB | 173.17s | 2,838.29 MB/s | 23.81 Gbps | 22,599.2 MB | 470.8 MB | 1,410.8% | 5.09 |

---

## 3. Key Findings & Architecture Comparisons

### A. The Multi-Threading GIL Bottleneck
- In multi-threaded mode, legacy `gcsfs_http` is completely pinned by the Python GIL and a single-threaded `fsspec` asyncio event loop at **~610–730 MB/s (5.1–6.1 Gbps)**, regardless of whether 1, 16, 24, or 48 threads are used. Transferring 480 GiB took **11.2 minutes (673.15s)**.
- In contrast, both `rust_http` and `rust_grpc` run network I/O, decompression, and TLS in Tokio without holding the GIL, reaching up to **4,855.32 MB/s (40.73 Gbps)** and completing 480 GiB in **101–107 seconds (6.6x faster)**.

### B. Multi-Processing Line-Rate Saturation
- `rust_http` saturates line rate at **18,084.74 MB/s (151.71 Gbps, ~18.1 GB/s)** with **just 24 processes**, reading 240 GiB in only **13.59s**.
- Legacy `gcsfs_http` requires 48 processes to reach 10,136 MB/s (85 Gbps), requiring **18.1 GB of RAM**.
- `rust_http` at 24 processes delivers **1.78x higher throughput** than `gcsfs_http` at 48 processes while using less than half the total memory (**8.06 GB vs 18.10 GB**).

### C. gRPC Cloud-Path: 48 vs. 200 Channels
- **Multi-Threading win with 200 channels**: In multi-threading, all threads share a single unified pool. Moving to 200 channels improved 24-thread throughput to **4,425.27 MB/s (37.12 Gbps)** and 48-thread throughput to **4,593.57 MB/s (38.53 Gbps)**, while cutting peak memory from 18.5 GB down to 11.1 GB.
- **Multi-Processing connection explosion**: Spawning 200 channels across 48 independent processes creates $48 \times 200 = 9,600$ TCP connections, triggering connection thrashing and backend throttling. For multi-processing, 48 channels (or fewer channels per process) is optimal.

---

## 4. How to Refresh the Benchmark Numbers
To rerun the benchmarks or regenerate this report, use the helper script [rust/bench/refresh_benchmarks.py](rust/bench/refresh_benchmarks.py):
```bash
# 1. Regenerate final_report.md from existing JSON results:
python3 rust/bench/refresh_benchmarks.py --only-report

# 2. Re-run multi-threading for all backends:
python3 rust/bench/refresh_benchmarks.py --modes threads

# 3. Re-run multi-processing for all backends:
python3 rust/bench/refresh_benchmarks.py --modes procs

# 4. Re-run specific backend (e.g. rust_grpc with 200 channels):
python3 rust/bench/refresh_benchmarks.py --backends rust_grpc --channels 200

# 5. Full rerun of all configurations:
python3 rust/bench/refresh_benchmarks.py
```
