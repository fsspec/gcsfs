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
  object), measured with runs interleaved against the current path so both
  see the same network: **~1676 MB/s vs ~802 MB/s (~2x)**, at **~40% less
  peak memory** (~232 MB vs ~391 MB). CPU is ~300% vs ~81%; because it also
  finishes ~2x sooner, that is ~1.8x the CPU *per byte*.
- Rust throughput is **much noisier** than fsspec's (coefficient of variation
  14.5% vs 3.2%) because fsspec is pinned by its own single-threaded event
  loop while rust is fast enough to inherit the network's variability. Quote
  it as a range, not a point value; the memory figures are the stable ones.
- Getting there required four things in the binding layer, not in the SDK:
  a **native Python awaitable** (via `pyo3-async-runtimes`) instead of an
  `asyncio.to_thread` hop, **bounded Tokio worker threads**, a
  **preallocated read buffer**, and **streaming straight into the destination
  `bytes`** so no intermediate `Vec` copy is needed.
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
## Build and run the Rust backend

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
measured with `/usr/bin/time`, with the shipped defaults plus
`MALLOC_ARENA_MAX=4`.

The `rust (zero-copy)` and `fsspec` runs are **interleaved** — alternating
backends so both see the same network conditions. Sequential batches are not
comparable, because GCS throughput drifts and the first run of a batch is
consistently slower. Reproduce with
[rust/bench/compare_backends.sh](bench/compare_backends.sh).

| Backend | Run | Throughput | Peak RSS | CPU% |
|---|---|---|---|---|
| rust (zero-copy) | 1 | 1282.1 MB/s | 234,500 KB (~229 MB) | 282% |
| rust (zero-copy) | 2 | 1531.1 MB/s | 234,556 KB (~229 MB) | 307% |
| rust (zero-copy) | 3 | 1694.5 MB/s | 240,792 KB (~235 MB) | 365% |
| rust (zero-copy) | 4 | 1962.7 MB/s | 230,052 KB (~225 MB) | 410% |
| rust (zero-copy) | 5 | 1745.3 MB/s | 237,628 KB (~232 MB) | 356% |
| **rust (zero-copy)** | **mean** | **~1643 MB/s** | **~230 MB** | **~344%** |
| rust (Vec + copy) | 1 | 1361.0 MB/s | 360,968 KB (~353 MB) | 384% |
| rust (Vec + copy) | 2 | 1443.5 MB/s | 346,004 KB (~338 MB) | 363% |
| rust (Vec + copy) | 3 | 1423.3 MB/s | 356,496 KB (~348 MB) | 342% |
| **rust (Vec + copy)** | **mean** | **~1409 MB/s** | **~346 MB** | **~363%** |
| fsspec | 1 | 870.9 MB/s | 389,232 KB (~380 MB) | 94% |
| fsspec | 2 | 918.7 MB/s | 380,668 KB (~372 MB) | 97% |
| fsspec | 3 | 922.3 MB/s | 389,928 KB (~381 MB) | 96% |
| fsspec | 4 | 897.9 MB/s | 394,824 KB (~386 MB) | 97% |
| fsspec | 5 | 912.3 MB/s | 396,656 KB (~387 MB) | 94% |
| **fsspec** | **mean** | **~904 MB/s** | **~381 MB** | **~96%** |

So the rust backend delivers roughly **1.8x the throughput at ~40% less peak
memory**. `rust (Vec + copy)` is the earlier implementation that assembled
data in a Rust `Vec` before copying it into Python; it was measured
separately (not interleaved), so compare its **memory** against zero-copy
(~346 MB vs ~230 MB) rather than its throughput, which sits inside the noise.

CPU% is `(user + system) / elapsed`, so it is time-normalised: a faster run
shows a higher percentage for the same total work. Since the rust path
finishes ~1.8x sooner, its **CPU per byte** is ~2x fsspec's, not the ~3.6x
the raw percentages suggest.

Correctness was verified against the `http` backend — byte-identical across
zero-length, inverted, 1-byte, unaligned, tail-of-object, past-EOF short
reads, 64 MiB and over-cap ranges, plus 60 randomized offset/length pairs.

**On variance:** the two backends are *not* equally reproducible. Running
them interleaved back-to-back under identical conditions:

| | rust (zero-copy) | fsspec |
|---|---|---|
| runs | 1326.5 / 1951.3 / 1953.1 / 1577.9 / 1569.3 MB/s | 796.7 / 777.1 / 850.7 / 787.5 / 799.9 MB/s |
| mean | ~1676 MB/s | ~802 MB/s |
| coefficient of variation | **14.5%** | **3.2%** |

Rust is ~4.5x more variable. Since interleaving exposes both to the same
network conditions, this is not shared noise. The likely explanation is that
**fsspec is its own bottleneck and rust is not**: fsspec runs at ~81% CPU on
a single-threaded event loop, pinned near a self-imposed ceiling, so it
returns ~800 MB/s almost regardless of available bandwidth. Rust spreads
across ~300% CPU, is not CPU-bound, and therefore takes whatever the
connection offers — inheriting the network's variability instead of masking
it. A warm-up effect is also consistently visible: the first run of a batch
is the slowest.

Practical consequence: quote the rust throughput as a **range or mean over
many runs**, never a single number, and prefer the memory figures when a
precise claim is needed — peak RSS held to ~5% spread (231-244 MB) across
every run. A rigorous benchmark would use distinct objects per run, discard
a warm-up run, and report percentiles.
Treat throughput figures as directional; a rigorous benchmark would use
distinct objects per run and report percentiles over many trials.
every run. Treat throughput figures as directional; a rigorous benchmark would
use distinct objects per run, discard a warm-up run, and report percentiles over many trials.

### Multi-process scaling: 1 to 48 workers reading distinct 10 GiB files

Measured with [rust/bench/compare_processes.sh](bench/compare_processes.sh) and [rust/bench/bench_multiprocess.py](bench/bench_multiprocess.py). Each worker process independently opens and reads a distinct 10 GiB file (`gs://princer-bucket/test_10g/file_{i}.bin`) in 8 MiB chunks. All workers synchronize via a process barrier to start reading simultaneously. Aggregate and per-process memory (VmRSS) are sampled every 50ms across the process tree.

Runs are executed sequentially per configuration (alternating between rust and fsspec backends).

| Workers / Files | Total Data Read | Backend | Elapsed Time | Aggregate Throughput | Speedup | Peak Agg RSS | Per-Proc RSS | Total CPU% | Total CPU Time |
|:---:|:---:|:---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| **1** | 10 GiB | **rust** | **7.67s** | **1335.8 MB/s** | **1.65x** | **259.7 MB** | **259.7 MB** | 285.0% | 21.8s |
| | | fsspec | 12.63s | 810.6 MB/s | 1.00x | 375.0 MB | 375.0 MB | 97.5% | 12.3s |
| **2** | 20 GiB | **rust** | **6.24s** | **3283.3 MB/s** | **2.37x** | **520.6 MB** | **260.4 MB** | 721.3% | 45.0s |
| | | fsspec | 14.79s | 1384.8 MB/s | 1.00x | 764.5 MB | 395.6 MB | 182.4% | 27.0s |
| **4** | 40 GiB | **rust** | **6.48s** | **6320.8 MB/s** | **2.54x** | **1.04 GB** | **260.6 MB** | 1452.1% | 94.1s |
| | | fsspec | 16.44s | 2491.9 MB/s | 1.00x | 1.49 GB | 395.3 MB | 341.4% | 56.1s |
| **8** | 80 GiB | **rust** | **16.32s** | **5020.7 MB/s** | **1.07x** | **2.08 GB** | **261.1 MB** | 1254.8% | 204.8s |
| | | fsspec | 17.48s | 4686.6 MB/s | 1.00x | 2.92 GB | 394.9 MB | 683.7% | 119.5s |
| **16** | 160 GiB | **rust** | **17.99s** | **9106.2 MB/s** | **1.14x** | **4.17 GB** | **268.5 MB** | 2527.7% | 454.7s |
| | | fsspec | 20.48s | 8000.1 MB/s | 1.00x | 5.62 GB | 454.8 MB | 1430.0% | 292.9s |
| **32** | 320 GiB | **rust** | **20.23s** | **16198.7 MB/s** | **1.42x** | **8.36 GB** | **276.0 MB** | 4708.9% | 952.5s |
| | | fsspec | 28.79s | 11380.9 MB/s | 1.00x | 10.65 GB | 406.5 MB | 2859.0% | 823.1s |
| **48** | 480 GiB | **rust** | **26.97s** | **18227.5 MB/s** | **1.44x** | **12.53 GB** | **268.6 MB** | 5570.2% | 1502.1s |
| | | fsspec | 38.74s | 12687.3 MB/s | 1.00x | 15.89 GB | 427.0 MB | 4058.7% | 1572.4s |

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

### 4. Streaming straight into the destination `bytes` — no intermediate copy

**In plain terms.** Data was being assembled in a Rust buffer and then copied
wholesale into a Python one, so every byte was handled twice and both buffers
were alive at once. Now the Python buffer is created first and the network
data is written directly into it.

**The detail.** When the exact length is known (both `start` and `end` given,
range ≤ 64 MiB — which covers **1280 of 1280 calls** in a typical gcsfs read),
`read_range_async` allocates the destination `bytes` up front via
`PyBytes_FromStringAndSize(NULL, len)`, hands the raw buffer pointer to the
read task, and streams chunks into Python-owned memory with
`copy_nonoverlapping`. Otherwise it falls back to the `Vec` path.

Measured against the previous copying implementation:

| Implementation | Throughput | Peak RSS | CPU% |
|---|---|---|---|
| `Vec` + copy into `bytes` | ~1409 MB/s | ~346 MB | ~363% |
| stream directly into `bytes` | ~1424 MB/s | **~232 MB** | ~300% |

The **memory saving (~114 MB, ~33%) is the real result** — it reproduced on
every run. The throughput and CPU differences fall inside the run-to-run
variance and should not be read as a speedup. That matches expectations: a
memcpy runs at ~29.6 GB/s here, so the copy itself was only ~0.36s of CPU per
10 GiB. The win is from no longer holding two buffers per in-flight read.

Three safety obligations, all handled:

- **Bounds.** Each chunk is checked against the remaining capacity; a
  response larger than requested is rejected rather than overrunning the
  Python allocation.
- **Short reads.** If the server returns fewer bytes than requested (range
  past EOF), returning the original buffer would expose its uninitialized
  tail to Python — a heap-disclosure bug. That case instead returns a
  correctly-sized copy of only the bytes actually written.
- **Send-ness.** The raw pointer is wrapped in a `BufPtr` newtype with
  `unsafe impl Send`, sound because exactly one task writes to it and the
  object is never visible to Python until the write completes.

Verified byte-identical to the `http` backend across zero-length, inverted,
1-byte, unaligned, tail-of-object, past-EOF short read, 64 MiB and over-cap
ranges, plus 60 randomized offset/length pairs.

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
  read path, ~2x the throughput of the current aiohttp path at ~40% lower
  peak memory and ~1.8x the CPU per byte.
- The performance characteristics are dominated by **how Rust is bound to
  Python**, not by the SDK: the thread-hop and allocator effects above were
  each worth more than anything in the I/O code itself.
- The Python layer is still the ceiling — the same SDK runs 2-4x faster
  without it. Reducing per-chunk Python involvement is where further gains
  are.
- Throughput numbers carry real run-to-run variance — and notably *more* for
  rust than for fsspec (CV 14.5% vs 3.2%) — so they should be treated as
  directional rather than as benchmark guarantees. Peak RSS is the stable,
  quotable metric here.
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
