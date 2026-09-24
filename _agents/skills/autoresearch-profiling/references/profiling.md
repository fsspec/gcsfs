# Profiling Recipes for `autoresearch`

Use these three complementary profilers before starting an `autoresearch` loop and after each kept change. Never commit temporary profiling output files (`*.prof`, `*.raw`, `mprofile_*.dat`) or `@profile` decorators.

---

## 1. Python `cProfile` & `pstats` (Deterministic Call & CPU Profiling)

Reference: <https://docs.python.org/3/library/profile.html>

Best for counting exact function calls (`ncalls`), finding high self-time (`tottime`), and inspecting caller/callee relationships (`cumtime`).

### Run on a script or single pytest target

```bash
python -m cProfile -o run.prof -m pytest gcsfs/tests/perf/microbenchmarks/read/test_read.py -k "read_seq" --run-benchmarks
```

### Bracket only the hot region (exclude setup/import noise)

When setup (e.g. bucket population or model loading) dominates startup, wrap only the target call temporarily during diagnosis:

```python
import cProfile, pstats

with cProfile.Profile() as pr:
    target_operation()
pstats.Stats(pr).strip_dirs().sort_stats("tottime").print_stats(25)
```

### Analyze `run.prof` with `pstats` (one-liner)

```bash
python -c "
import pstats
s = pstats.Stats('run.prof').strip_dirs()
print('=== TOP BY TOTTIME (in-function CPU) ===')
s.sort_stats('tottime').print_stats(20)
print('=== TOP BY CUMULATIVE TIME ===')
s.sort_stats('cumtime').print_stats(20)
print('=== TOP BY CALL COUNT (ncalls) ===')
s.sort_stats('ncalls').print_stats(20)
"
```

To see what triggers a hot function: `s.print_callers('hot_function_name')` or `s.print_callees('caller_name')`.

---

## 2. `memory_profiler` & `mprof` (Peak RSS & Line-by-Line Memory)

Reference: <https://github.com/pythonprofilers/memory_profiler>

Best for reducing `mem_max` (microbenchmarks) or `memory_usage_peak_bytes` (subsystembenchmarks), spotting buffer copies, and tracking child-process memory spikes.

### Time-based process & child memory (`mprof`)

`run.py` spawns `pytest` and worker subprocesses. Always pass `--include-children` (or `--multiprocess`):

```bash
# Record memory usage across parent + children (default 0.1s sampling interval)
mprof run --include-children --multiprocess --output mprof.dat \
  python gcsfs/tests/perf/microbenchmarks/run.py \
  --group=write --config=write_seq_fixed_duration --regional-bucket="$REGIONAL_BUCKET"

# Print peak RSS
mprof peak mprof.dat

# Clean up data file after inspecting
rm -f mprof.dat
```

### Line-by-line memory allocation (`@profile`)

To find which line inside a function duplicates a large buffer (e.g. `bytes.join`, slicing, or uncollected references):

1. Temporarily add `@profile` above the suspect function (or `from memory_profiler import profile`).
2. Run with `-m memory_profiler` on a minimal reproduction script or single test:
   ```bash
   python -m memory_profiler repro_script.py
   ```
3. Inspect the `Increment` column (MiB added per line) and `Occurrences`, then **remove `@profile`** before committing to the `autoresearch` loop.

---

## 3. `py-spy` (Low-Overhead Sampling, Async Wait & Subprocess Profiling)

Reference: <https://github.com/benfred/py-spy>

Best for multi-process benchmarks, `asyncio`/`aiohttp` I/O wait stalls (`--idle`), and C/C++ extensions (`--native`) without perturbing benchmark timings.

### Key flags for `gcsfs` and benchmark suites

- **`--subprocesses`**: Required when wrapping `run.py`, because `run.py` spawns `pytest` which spawns worker processes (`torch.multiprocessing` / `DataLoader`).
- **`--idle`**: Required for async/network workloads (`gcsfs` runs coroutines on a background event-loop thread). Without `--idle`, `py-spy` only records on-CPU frames and drops `await` / lock wait time. Take **both** a default (on-CPU) and `--idle` (wall-clock) profile.
- **`--native`**: Unwinds C/C++ extensions (`grpc`, `protobuf` upb, `pyarrow`, `torch`).

### Wrap a microbenchmark or subsystembenchmark

```bash
py-spy record -o profile.raw -f raw --subprocesses --idle -- \
  python gcsfs/tests/perf/microbenchmarks/run.py \
  --group=write --config=write_seq_fixed_duration --regional-bucket="$REGIONAL_BUCKET"
```

### Attach to running workers (including Ray workers)

Ray workers (`ray::*`) are children of `raylet`, not `pytest`, so `--subprocesses` on `run.py` will miss them. Attach by PID (note: while spawning a child via `py-spy record -- <cmd>` works unprivileged under Linux default `kernel.yama.ptrace_scope=1`, attaching to an existing non-child PID `-p <pid>` requires `CAP_SYS_PTRACE` or `sudo sysctl kernel.yama.ptrace_scope=0` with user approval):

```bash
pgrep -af 'ray::'   # Locate active Ray worker PID
py-spy record -o ray_worker.raw -f raw --idle -p <worker_pid>
```

### Summarize raw folded stacks (`-f raw`)

```bash
# Top 25 hottest stack traces
awk '{n=$NF; $NF=""; print n"\t"$0}' profile.raw | sort -rn | head -25

# Attribute samples by package
for p in gcsfs fsspec aiohttp google/cloud/storage grpc google/protobuf torch ray pyarrow; do
  printf '%-22s %s\n' "$p" "$(awk -v p="$p" '$0 ~ p {s+=$NF} END{print s+0}' profile.raw)"
done | sort -k2 -rn
```

---

## 4. Mapping Profiler Findings to `autoresearch` Hypotheses

| Profiler Signal | Root Cause Pattern | Atomic `autoresearch` Hypothesis |
| :--- | :--- | :--- |
| High `ncalls` in `cProfile` (`pstats`) | Redundant per-chunk metadata lookup, repeated parsing, or tiny read/write calls | Cache/memoize result, hoist out of loop, or batch into fewer calls |
| High `tottime` in `cProfile` / on-CPU `py-spy` | Expensive Python loop, checksum, serialization, or buffer concatenation | Replace with `memoryview` slicing, vectorized/C-extension path, or pre-allocated buffer |
| High `--idle` share in `py-spy` vs low on-CPU | Sequential `await` / RPC round-trips or thread lock contention | Increase concurrency (`gather`/prefetch), widen pipeline window, or narrow lock scope |
| High `Increment` in `memory_profiler` / `mprof peak` | Full payload copies (`b"".join`, `data[:]` slice on `bytes`) | Switch to `memoryview`, stream chunks directly, or release buffer references early |
