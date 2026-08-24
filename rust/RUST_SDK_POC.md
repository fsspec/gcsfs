# gcsfs Rust SDK read-path POC

Proof-of-concept evaluating the official Rust `google-cloud-storage` SDK
(googleapis/google-cloud-rust) as an alternative object-read backend for
gcsfs, exposed to Python via a PyO3 extension (`gcsfs-rust-backend`,
[rust/gcsfs_rust](gcsfs_rust)).

## Executive summary

- Built a working, opt-in Rust read backend for gcsfs
  (`read_backend="rust"` / `GCSFS_READ_BACKEND=rust`). Correctness verified
  byte-for-byte against the existing aiohttp path.
- At the best-tested config (concurrency=16, 256 MiB readahead, 10 GiB
  object) it reads at **~1400 MB/s vs ~685 MB/s** for the current path
  (**~2x**), at **comparable peak memory** (~346 MB vs ~374 MB). CPU is
  ~363% vs ~81%; because it also finishes ~2x sooner, that is ~2.2x the CPU
  *per byte*.
- Getting there required three things in the binding layer, not in the SDK:
  a **native Python awaitable** (via `pyo3-async-runtimes`) instead of an
  `asyncio.to_thread` hop, **bounded Tokio worker threads**, and a
  **preallocated read buffer**.
- A standalone, Python-free benchmark of the same Rust SDK still reaches
  2-4x higher throughput than the same reads driven through gcsfs, so the
  Python layer remains the limiting factor on what this integration can
  deliver today.
- Both backends use the same JSON REST API (`storage.googleapis.com`). The
  crate's gRPC path exists but is unstable and requires Google account-team
  allowlisting per project/bucket, so it wasn't evaluated.

## Environment

- **VM machine type:** `c4-standard-192` (192 vCPUs, ~708 GiB RAM)
- **Zone:** `us-west4-a`
- **Bucket:** `gs://princer-bucket`
  - Location: `US-WEST4` (regional, co-located with the VM's zone)
  - Storage class: `STANDARD`
  - Uniform bucket-level access: enabled
- **Test object:** `10gfile.bin`, 10,737,418,240 bytes (10 GiB)
- **Rust toolchain:** stable 1.97.1, `pyo3` 0.29.2,
  `pyo3-async-runtimes` 0.29.0, `google-cloud-storage` 1.17.0
- **Python:** CPython 3.14 (test venv), `gcsfs` from this branch (`with_rust_sdk`)
- **Wire protocol:** confirmed via live trace (`RUST_LOG=reqwest=trace,hyper=debug`)
  that `Storage::read_object()` goes over the **JSON REST API** via `reqwest`
  to `storage.googleapis.com:443`. `tonic`/gRPC is a dependency of the crate
  but only serves `StorageControl` (IAM, long-running ops), not object
  reads — so both backends hit the same JSON API.

## Build & run the rust backend

```bash
# 1. Install a Rust toolchain (if not already present)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
. "$HOME/.cargo/env"

# 2. Build the PyO3 extension into your Python env (needs `maturin`)
pip install maturin
cd rust/gcsfs_rust
maturin develop --release   # or: maturin build --release && pip install <wheel>

# 3. Enable it in gcsfs — either per-instance or globally via env var
python -c "import gcsfs; fs = gcsfs.GCSFileSystem(read_backend='rust')"
# or
export GCSFS_READ_BACKEND=rust

# 4. Try the benchmark script with it
python data/read_gcs_file.py --url gs://<bucket>/<object> --backend rust
```

Tuning knobs:

- `GCSFS_RUST_WORKER_THREADS` (default 16) — Tokio worker threads. Reads are
  I/O-bound, so a worker per core costs memory for no benefit.
- `MALLOC_ARENA_MAX=4` — recommended at process launch; caps glibc
  per-thread malloc arenas, which otherwise retain freed read buffers and
  inflate RSS. Must be set before process start, so it cannot be applied
  from inside the extension.

`maturin develop` requires an active virtualenv (`VIRTUAL_ENV` set, or a
`.venv` in/above the cwd) — otherwise it errors with "Couldn't find a
virtualenv or conda environment".

## Benchmark applications

1. **[data/read_gcs_file.py](../data/read_gcs_file.py)** — end-to-end
   benchmark through gcsfs. Reads a GCS URL via `fsspec.open()` and streams
   it in fixed-size chunks, reporting wall-clock throughput. Relevant flags:
   - `--backend {fsspec,rust}` — selects the read path. `rust` sets
     `GCSFS_READ_BACKEND=rust`, routing
     `GCSFileSystem._cat_file_sequential` through
     `gcsfs.rust_backend.cat_file_range` instead of the aiohttp client.
   - `--concurrency` — forwarded to `GCSFile`, controlling how many parallel
     range requests gcsfs issues per fetch.
   - `--io-size` (default 8 MiB) — chunk size read from the open file object.
   - `--max-prefetch-size` — readahead ceiling for the adaptive prefetcher.
   - All runs below use `cache_type="none"` with the background adaptive
     prefetcher enabled (the gcsfs default when `cache_type` and
     `--enable-prefetch` are left unset).

2. **[rust/bench_rust_read](bench_rust_read)** — standalone Rust binary (no
   Python/PyO3) issuing N parallel `Storage::read_object()` range requests
   directly against the SDK, to separate SDK cost from binding cost.
   ```
   cargo run --release -p bench_rust_read -- <bucket> <object> <size_bytes> <parallelism>
   ```

## Results

### concurrency=16, max-prefetch-size=256MiB, io-size=8MiB, 10gfile.bin

Every run reads `gs://princer-bucket/10gfile.bin` (10 GiB) end-to-end.
Throughput, peak RSS and CPU% all come from the same process invocation,
measured with `/usr/bin/time`. The `rust` rows use the shipped defaults plus
`MALLOC_ARENA_MAX=4`.

| Backend | Run | Throughput | Peak RSS | CPU% |
|---|---|---|---|---|
| rust | 1 | 1361.0 MB/s | 360,968 KB (~353 MB) | 384% |
| rust | 2 | 1443.5 MB/s | 346,004 KB (~338 MB) | 363% |
| rust | 3 | 1423.3 MB/s | 356,496 KB (~348 MB) | 342% |
| **rust** | **avg** | **~1409 MB/s** | **~346 MB** | **~363%** |
| fsspec | 1 | 660.92 MB/s | 375,304 KB (~367 MB) | 76% |
| fsspec | 2 | 622.16 MB/s | 379,504 KB (~371 MB) | 74% |
| fsspec | 3 | 772.38 MB/s | 381,780 KB (~373 MB) | 92% |
| **fsspec** | **avg** | **~685 MB/s** | **~374 MB** | **~81%** |

CPU% is `(user + system) / elapsed`, so it is time-normalised: a faster run
shows a higher percentage for the same total work. Since the rust path
finishes ~2x sooner, its **CPU per byte** is ~2.2x fsspec's, not the ~4.5x
the raw percentages suggest.

Correctness was verified against the `http` backend — byte-identical across
zero-length, inverted, 1-byte, unaligned, tail-of-object and 64 MiB ranges.

**On variance:** throughput swings noticeably run to run (fsspec 622-772
MB/s here; wider spreads elsewhere in testing). Each process run opens fresh
TCP/TLS connections, so results are sensitive to per-connection GCS backend
routing and TCP slow-start. Peak RSS, by contrast, was stable across runs.
Treat throughput figures as directional; a rigorous benchmark would use
distinct objects per run and report percentiles over many trials.

### Pure Rust ceiling (no Python), direct range reads

| Parallelism | Throughput |
|---|---|
| 8  | 480 – 1054 MB/s (high variance across repeats) |
| 16 | ~2630 MB/s |
| 32 | ~4128 MB/s |
| 64 | ~3054 MB/s |

The same SDK driven without Python is 2-4x faster than through gcsfs, which
bounds what this integration can currently deliver.

## Design notes: what the performance depends on

Three properties of the binding layer dominate. All were established by
changing one variable at a time against the same build.

### 1. Native awaitable instead of a thread hop — the CPU factor

**In plain terms — scheduler overhead.** Two different threads were doing one
job: one watched the network connection, another collected the result. Data
arrives as thousands of small packets, so every few KB had to be handed
between them, waking a sleeping thread each time — **2.2 million wake-ups per
10 GB read**. Waking a thread goes through the operating system scheduler, so
most of the CPU went on the hand-off rather than on moving data. Fixed by
letting the thread that was already watching the connection also do the
collecting, and notifying Python just once per read.

**The detail.** [gcsfs/rust_backend.py](../gcsfs/rust_backend.py) awaits
`read_range_async`, built on `pyo3-async-runtimes`, so the caller's own event
loop drives the Rust future. (The blocking `read_range` is retained for
callers without a running loop, and as a fallback if an older extension build
is installed.) The alternative — `asyncio.to_thread` into a blocking
`runtime.block_on(...)` — measures like this:

| Path | Voluntary ctx switches | User | System | CPU% |
|---|---|---|---|---|
| thread hop | **2,234,777** | 30.5s | 45.2s | ~829% |
| native awaitable | **267,483** | 12.2s | 14.5s | ~363% |

Note that *system* (kernel) time was the larger half of the old cost — a
signature of scheduler overhead rather than useful work.

Both rows are the same build, same buffer handling, and instrumentation
confirmed both issued exactly **1280 calls** at **max 16 concurrent
in-flight** reads — the only difference is the thread hop.

The switches are **per network chunk, not per call**: holding bytes constant
while cutting call count 8x (`block_size` 8 MiB → 64 MiB) did not reduce
them (2.18M vs 2.76M). At ~2.18M switches per 10 GiB that is one per
~4.9 KB, roughly one per TCP segment.

Mechanism: `block_on(fut)` **parks the calling pool thread**, while the
socket is polled by Tokio's I/O driver on a *different* worker thread. Every
arriving chunk of response body therefore needs a `futex` wake of the parked
thread, which consumes the chunk and parks again. `future_into_py` instead
spawns the future onto the runtime, so the thread polling the socket is the
same one consuming chunks; Python is signalled once per call at completion
via `call_soon_threadsafe`. Hence 209 switches per call rather than 1746.

### 2. Thread count and glibc malloc arenas — the memory factor

**In plain terms — stranded memory pools.** To stop threads queuing behind
each other when they ask for memory, the C library gives busy threads their
own separate pools. Rust's async runtime defaulted to one worker thread per
CPU core — 192 here — so we ended up with **229 pools**. Memory freed into
one pool can't be reused by a thread on another, so hundreds of pools sat on
idle memory while new requests kept asking the OS for more: **592 MB held to
do ~128 MB of real work**. Fixed by using far fewer worker threads (16),
since network reads are waiting-heavy, not compute-heavy.

**The detail.** glibc splits the heap into per-thread **arenas** to reduce
lock contention, up to 8 × cores (1536 on this VM). Freed 8 MiB read buffers
are stranded on the free list of whichever arena allocated them — not
reusable by other threads, and not returned to the OS (non-main arenas are
64 MiB mmap'd heaps trimmed only from the top).

Measured with `malloc_info()` on an otherwise identical run:

| Config | Arenas | malloc system bytes | RSS |
|---|---|---|---|
| 192 workers, default arenas | **229** | 592 MiB | ~650 MB |
| 192 workers, `MALLOC_ARENA_MAX=4` | **4** | 210 MiB | ~293 MB |

Either knob recovers the memory:

| Config | Throughput | Peak RSS |
|---|---|---|
| 192 workers, default arenas | ~1317 MB/s | ~554 MB |
| 192 workers, `MALLOC_ARENA_MAX=2` | ~1416 MB/s | ~348 MB |
| 16 workers, default arenas | ~1360 MB/s | ~355 MB |
| 16 workers, `MALLOC_ARENA_MAX=4` | ~1422 MB/s | ~331 MB |

The in-code fix is bounding Tokio workers (default 16); the arena cap is a
deployment-side knob. A non-glibc allocator (mimalloc/jemalloc) in the
extension would likely remove the need for the env var — untested.

Two caveats worth stating plainly:

- This is a **glibc property, not a Rust one**. Arenas apply to any process
  linked against glibc; the pure-Python fsspec path creates them too (23
  arenas / 183 MiB on the same workload — CPython sends allocations above
  512 B straight to `malloc`, and these buffers are 8 MiB). What differed
  was thread count, not language. Capping arenas does *not* help fsspec
  (183 → 201 MiB) precisely because it has few threads to begin with.
- The throughput effect of capping arenas is **unclear** — some runs
  improved, one went 1517 → 1258 MB/s. It sits inside the run-to-run
  variance, so it should not be assumed free: fewer arenas does mean more
  threads sharing each arena lock.

### 3. Preallocated read buffer

`Vec::with_capacity(end - start)`, capped so a bogus `end` cannot force a
huge allocation. A/B on the same build and script, toggling only this:

| Config | Peak RSS |
|---|---|
| preallocated | ~345 MB |
| grown incrementally | ~420 MB |

Worth ~75 MB (~18%), and it affects **memory only** — CPU time was
unchanged, because the number of bytes copied is identical either way.

The mechanism is *not* over-allocation. Rust's `Vec` doubles on growth, but
for an 8 MiB read arriving in ~64 KiB chunks the doubling ladder lands
exactly on 8 MiB — measured final capacity is 8 MiB either way, with no
slack. What preallocation avoids is the **ladder of intermediate
allocations** (64 KiB, 128 KiB, 256 KiB … 4 MiB) that each get allocated,
copied out of, and freed during a single read. Those freed odd-sized blocks
land in glibc arena free lists where they are poor fits for the next 8 MiB
request, so they accumulate as fragmentation — the same arena effect
described in §2. Preallocating issues exactly one uniformly-sized allocation
per read, which the allocator recycles cleanly.

Note this is workload-dependent rather than a universal win: an isolated
micro-benchmark where 16 threads each fill an 8 MiB `Vec` as fast as
possible showed preallocation using *more* peak RSS (81 MiB vs 46 MiB),
because reserving up front makes all threads' allocations peak
simultaneously instead of ramping. The gain here comes from the
long-lived, network-paced growth pattern, not from `with_capacity` being
inherently cheaper.

The final copy from the Rust `Vec` into Python `bytes` remains.
`PyBytes::new_with` would avoid it but requires holding the GIL while
streaming chunks, which defeats the async design.

## Measurement tooling

No sampling profiler was used — `perf` is installed but blocked on this host
(`kernel.perf_event_paranoid=4` needs a root sysctl change). Findings came
from counters plus controlled A/B runs:

| Tool | What it gave |
|---|---|
| `/usr/bin/time` | throughput, `maxresident` (peak RSS), CPU% |
| `/usr/bin/time -v` | **voluntary context switches** — the signal that isolated the thread-hop cost |
| glibc `malloc_info()` via `ctypes` | live **arena count** and malloc system bytes |
| `/proc/self/status` `VmRSS` | in-process RSS at chosen points, not just the peak |
| monkeypatched `cat_file_range` | call count and max concurrent in-flight reads |
| [rust/bench_rust_read](bench_rust_read) | Rust-only baseline, isolating SDK cost from binding cost |
| `RUST_LOG=reqwest=trace,hyper=debug` | confirmed the JSON/REST wire protocol |
| `cargo tree -i <crate>` | showed which crates pull in `tonic` vs `reqwest` |

More important than any single tool was **changing one variable at a time
against the same build**: a flag to toggle only the thread hop,
`GCSFS_RUST_WORKER_THREADS` for only thread count, `MALLOC_ARENA_MAX` for
only the allocator, and `block_size` to vary call count at fixed byte count.

## Takeaways

- The Rust SDK backend is a **drop-in, correctness-verified** alternative
  read path, ~2x the throughput of the current aiohttp path at comparable
  peak memory and ~2.2x the CPU per byte.
- The performance characteristics are dominated by **how Rust is bound to
  Python**, not by the SDK: the thread-hop and allocator effects above were
  each worth more than anything in the I/O code itself.
- The Python layer is still the ceiling — the same SDK runs 2-4x faster
  without it. Reducing per-chunk Python involvement is where further gains
  are.
- Throughput numbers carry real run-to-run variance and should be treated as
  directional rather than as benchmark guarantees.
- **Wire protocol:** `read_object()` uses the JSON REST API over `reqwest`,
  not gRPC. The crate builds an internal gRPC client
  (`with_grpc_subchannel_count()`), but it serves only the unstable,
  allowlist-gated bidi streaming APIs (`open_object()` / `send_and_read()`,
  behind the `unstable-stream` feature and a
  `google_cloud_unstable_storage_bidi` cfg flag). A gRPC data path is a
  possible future avenue but was out of scope.

## Not yet covered

- Wiring the Rust backend into `_cat_file_concurrent`'s chunk-splitting
  strategy directly (today it still goes through gcsfs's Python-level
  splitting into per-chunk `_cat_file_sequential` calls).
- CI wheel builds / cross-platform packaging for `gcsfs-rust-backend`.
- Correctness/integration tests in the pytest suite, beyond the manual
  range-comparison checks run here.
- Evaluating mimalloc/jemalloc to remove the `MALLOC_ARENA_MAX` dependency.
