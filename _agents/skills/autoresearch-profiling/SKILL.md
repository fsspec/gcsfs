---
name: autoresearch-profiling
description: Use when asked to improve throughput, latency, CPU, memory, or request metrics, profile Python workloads with cProfile, memory_profiler, or py-spy, or run autonomous performance optimization on microbenchmarks or subsystembenchmarks.
---

# Profile-Guided Autoresearch (`autoresearch-profiling`)

Combine the unchanged [`autoresearch`](~/.gemini/config/skills/autoresearch/SKILL.md) autonomous loop with deterministic and sampling profilers so every iteration targets a measured bottleneck rather than a guess.

- **Iteration & state**: `autoresearch` (`modify → verify → guard → keep/revert → log`). Never modify the `autoresearch` skill or hand-roll custom loops.
- **Bottleneck discovery**: `py-spy`, `cProfile` (`pstats`), and `memory_profiler` (`mprof`).

## Quick Start

```bash
# 1. Verify tools in target Python env
python -c "import cProfile, pstats, memory_profiler" && py-spy --version && mprof --version

# 2. Profile baseline before changing code (pick tool from matrix below)
py-spy record -o baseline.raw -f raw --subprocesses --idle -- <benchmark_cmd>

# 3. Hand off to $autoresearch with a Verify command that outputs the bare metric on its last line
```

## Profiler Selection Matrix

| Target Metric / Symptom | Tool | Best Mode / Command | Why |
| :--- | :--- | :--- | :--- |
| Wall-clock latency / throughput (async, network, multi-process) | `py-spy` | `py-spy record -f raw --subprocesses --idle` | Captures both on-CPU frames and `await`/lock/I/O wait states across child processes with minimal overhead |
| Native C/C++/Rust extensions (`grpc`, `protobuf`, `torch`, `pyarrow`) | `py-spy` | `py-spy record -f raw --native` | Unwinds native C/C++ stacks alongside Python frames |
| CPU time & exact call counts (`ncalls`, `tottime`, `cumtime`) | `cProfile` + `pstats` | `python -m cProfile -o out.prof` | Deterministic call-graph accounting; exposes redundant calls ($O(N^2)$ loops, repeated parsing/serialization) |
| Peak RSS & child-process memory over time | `memory_profiler` (`mprof`) | `mprof run --include-children --multiprocess` | Tracks peak memory (`mprof peak`) across parent and spawned worker processes |
| Line-by-line allocation / copy spikes in a hot function | `memory_profiler` | `python -m memory_profiler` (`@profile`) | Pinpoints exact lines duplicating buffers or retaining large objects |

See [references/profiling.md](references/profiling.md) for copy-pasteable commands and analysis one-liners.

## Workflow

- [ ] **Step 1: Preflight & Environment Check**
  - Confirm `autoresearch` (`~/.gemini/config/skills/autoresearch/SKILL.md`), `cProfile`, `memory_profiler`, and `py-spy` are installed.
  - Confirm editable install so edits affect execution: `(cd /tmp && python -c "import gcsfs; print(gcsfs.__file__)")` must point inside the workspace checkout.
  - Measure baseline noise floor (run the `Verify` command 3 times unchanged) before starting the loop.
- [ ] **Step 2: Profile Baseline Before Hypothesizing**
  - Run the appropriate profiler from the matrix above on the unmodified benchmark.
  - Extract top hotspots (`tottime`/`cumtime`/`ncalls` in `pstats`, folded stacks in `py-spy`, or peak RSS in `mprof`).
  - Formulate **one atomic hypothesis** naming the function/line, mechanism, and expected metric direction.
- [ ] **Step 3: Configure and Invoke `autoresearch`**
  - Define `Goal`, `Scope` (production source only; **never** edit `gcsfs/tests/**` benchmark harnesses), `Metric`, `Direction` (`higher_is_better` or `lower_is_better`), `Verify` (command + python extractor printing a single number on the last line), and `Guard` (unit/emulator tests).
  - For `gcsfs` microbenchmarks and subsystembenchmarks, use the exact `Verify` and `Guard` templates in [references/benchmarks.md](references/benchmarks.md).
  - Invoke `$autoresearch` to execute the modify → verify → keep/discard loop.
- [ ] **Step 4: Re-Profile After Kept Wins or Plateaus**
  - Whenever a commit is kept (or 3 consecutive iterations discard), re-run the profiler on the updated tree.
  - Bottlenecks shift as hot paths shrink—use the fresh profile to drive the next `autoresearch` hypothesis.
  - Remove any temporary `@profile` decorators or `cProfile` snippets before committing.

## References

- [references/profiling.md](references/profiling.md) — Recipes for `py-spy`, `cProfile`/`pstats`, and `memory_profiler` (`mprof`), plus translating profiles into `autoresearch` hypotheses.
- [references/benchmarks.md](references/benchmarks.md) — Running and extracting any perf metric from `microbenchmarks` and `subsystembenchmarks` for `autoresearch`.
