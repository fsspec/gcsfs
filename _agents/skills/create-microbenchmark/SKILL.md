---
name: create-microbenchmark
description: >-
  Use this skill to write a new microbenchmark for a given method name. It validates if the microbenchmark already exists, and if not, generates the necessary boilerplate files in gcsfs/tests/perf/microbenchmarks.
---

# Create Microbenchmark

This skill instructs the agent on how to properly add a new microbenchmark to the `gcsfs/tests/perf/microbenchmarks` directory.

## Procedure

1. **Identify the Method Name**
   Extract the method name you need to benchmark from the user's prompt.

2. **Validation: Check for Existing Benchmarks and Dependencies**
   Perform validation checks before generating the code:
   - **Direct Benchmarks:** Use `find_by_name` or `list_dir` to check if a directory `gcsfs/tests/perf/microbenchmarks/<method-name>` or file `test_<method-name>.py` already exists for the requested method.
   - **Internal Dependencies:** Inspect the source code of the requested method to see if it internally calls another method that already has its own benchmark.
   - **Proceeding:** If you discover that a benchmark already exists (either for the requested method itself or for one of its internal dependencies), **do not abort**. Simply **inform the user** about the existing benchmark(s), and then **proceed** to generate the new microbenchmark as requested.

3. **Scaffold the New Microbenchmark**
   If no existing benchmark is found, use the `write_to_file` tool to create the directory `gcsfs/tests/perf/microbenchmarks/<method-name>` and the four following required files:

   *   `configs.yaml`: Define the benchmark scenarios (e.g., different depths, folder counts, and file numbers).
   *   `parameters.py`: Define a dataclass describing your benchmark parameters (e.g., `<Method>BenchmarkParameters`), subclassing an appropriate base parameters class (e.g., from `listing.parameters` or `IOParameters`).
   *   `configs.py`: Create the Configurator class and a getter function like `get_<method-name>_benchmark_cases()`.
   *   `test_<method-name>.py`: Implement the benchmark tests. Decorate your generic tests with `@pytest.mark.parametrize` (indirectly pulling parameters from the configurator output) and use methods such as `run_single_threaded` or `run_multi_threaded` from `gcsfs.tests.perf.microbenchmarks.runner`.

4. **Verify Independence**
   Ensure your code uses proper module paths to `gcsfs.tests.perf.microbenchmarks.*`. Notice that `run.py` dynamically recognizes benchmark groups based on subdirectories and requires no manual changes.
