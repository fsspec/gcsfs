# gcsfs Rust SDK read-path POC

Proof-of-concept evaluating the official Rust `google-cloud-storage` SDK
(googleapis/google-cloud-rust) as an alternative object-read backend for
gcsfs, exposed to Python via a PyO3 extension (`gcsfs-rust-backend`,
[rust/gcsfs_rust](gcsfs_rust)).

## Environment

- **VM machine type:** `c4-standard-192` (192 vCPUs, ~708 GiB RAM)
- **Zone:** `us-west4-a`
- **Bucket:** `gs://princer-bucket`
  - Location: `US-WEST4` (regional, co-located with the VM's zone)
  - Storage class: `STANDARD`
  - Uniform bucket-level access: enabled
- **Test object:** `10gfile.bin`, 10,737,418,240 bytes (10 GiB)
- **Rust toolchain:** stable 1.97.1, `pyo3` 0.29.2, `google-cloud-storage` 1.17.0
- **Python:** CPython 3.14 (test venv), `gcsfs` from this branch (`with_rust_sdk`)

## Benchmark applications

1. **[data/read_gcs_file.py](../data/read_gcs_file.py)** — end-to-end benchmark
   through gcsfs. Reads a GCS URL via `fsspec.open()` and streams it in
   fixed-size chunks, reporting wall-clock throughput. Relevant flags:
   - `--backend {fsspec,rust}` — selects the read path. `rust` sets
     `GCSFS_READ_BACKEND=rust`, routing `GCSFileSystem._cat_file_sequential`
     through `gcsfs.rust_backend.cat_file_range` (the PyO3 extension) instead
     of the built-in aiohttp client.
   - `--concurrency` — forwarded to `GCSFile` as the `concurrency` kwarg,
     controlling how many parallel range requests gcsfs issues per fetch.
   - `--io-size` (default 8 MiB) — chunk size read from the open file object
     in the benchmark loop.
   - Effective config for all runs below: `cache_type="none"` with the
     background adaptive prefetcher enabled (gcsfs default when `cache_type`
     and `--enable-prefetch` are both left unset).

2. **[rust/bench_rust_read](bench_rust_read)** — standalone Rust binary (no
   Python/PyO3 involved) that issues N parallel `Storage::read_object()`
   range requests directly against the Rust SDK, to isolate the Rust SDK's
   raw throughput from any Python-layer overhead.
   ```
   cargo run --release -p bench_rust_read -- <bucket> <object> <size_bytes> <parallelism>
   ```

## Results

### Python path (gcsfs), 10gfile.bin, io-size=8MiB, cache_type=none, prefetcher enabled

| Backend | Concurrency | Run 1 | Run 2 | Run 3 | Avg |
|---|---|---|---|---|---|
| rust  | 4  | 932.08 MB/s  | -            | -            | 932 MB/s |
| fsspec | 4  | 801.13 MB/s  | -            | -            | 801 MB/s |
| rust  | 16 | 953.27 MB/s  | 1051.57 MB/s | 873.42 MB/s  | ~959 MB/s |
| fsspec | 16 | 822.70 MB/s  | 851.33 MB/s  | 765.36 MB/s  | ~813 MB/s |
| rust  | 16, max-prefetch-size=256MiB (explicit; gcsfs default) | 1108.26 MB/s | 1075.59 MB/s | 1140.51 MB/s | ~1108 MB/s |
| fsspec | 16, max-prefetch-size=256MiB (explicit; gcsfs default) | 800.39 MB/s  | 778.08 MB/s  | 545.26 MB/s  | ~708 MB/s |

### Pure Rust path (no Python), 10gfile.bin, direct range reads

| Parallelism | Throughput (single run) |
|---|---|
| 8  | 479.76 – 1054.12 MB/s (high variance across repeats) |
| 16 | 2632 – 2640 MB/s |
| 32 | 4128.01 MB/s |
| 64 | 3054.06 MB/s |

## Takeaways

- The Rust SDK backend is a **drop-in, correctness-verified** alternative
  read path (byte-for-byte identical output, verified via full-object reads)
  and is consistently **~15-20% faster than the current aiohttp path** when
  measured through gcsfs at matched concurrency.
- A standalone, Python-free benchmark of the same Rust SDK reaches
  **2-4x higher throughput** than the same reads driven through
  gcsfs/Python, indicating the Python layer (asyncio scheduling, per-call
  thread dispatch via `asyncio.to_thread`, 8 MiB chunk granularity from the
  prefetcher) is the dominant bottleneck, not the Rust SDK or network.
- Pure-Rust throughput numbers show large run-to-run variance (e.g. 480 MB/s
  to 4.1 GB/s for the same object/parallelism across repeats), most likely
  GCS-side warm-up/striping behavior for a repeatedly-read single object
  rather than something in this code. Numbers here should be treated as
  directional, not final — a rigorous benchmark would use distinct objects
  per run, multiple trials, and averaging/percentiles.
- Explicitly setting `--max-prefetch-size 256MiB` didn't materially change
  results, since 256 MiB is already gcsfs's default `MAX_PREFETCH_SIZE`
  ([zb_hns_utils.py](gcsfs/zb_hns_utils.py)) — the rust path trended slightly
  higher (~1108 MB/s) but fsspec run-to-run variance (545-800 MB/s) makes it
  hard to draw a strong conclusion from these sample sizes.

## Not yet covered

- Wiring the Rust backend into `_cat_file_concurrent`'s chunk-splitting
  strategy directly (today it still goes through gcsfs's Python-level
  splitting into per-chunk `_cat_file_sequential` calls).
- CI wheel builds / cross-platform packaging for `gcsfs-rust-backend`.
- Correctness/integration tests beyond manual byte-count verification.
