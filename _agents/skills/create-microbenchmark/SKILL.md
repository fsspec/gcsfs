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
   - **Direct Benchmark Check:** Use `find_by_name` or `list_dir` to check if a directory `gcsfs/tests/perf/microbenchmarks/<method_name>` or file `test_<method_name>.py` already exists. If it does, **abort** the procedure and inform the user that a benchmark for this method already exists.
   - **Internal Dependencies Check:** If the direct benchmark does not exist, inspect the source code of the requested method to see if it internally calls another method that already has its own benchmark.
   - **Confirm Proceeding:** If a benchmark for an internal dependency is found, you must **ask the user for confirmation** whether it is okay to proceed given the internal dependency is already benchmarked. Wait for their approval before generating the new microbenchmark. If no internal dependency benchmarks are found, proceed directly.

3. **Scaffold the New Microbenchmark**
   Use the `write_to_file` tool to create the directory `gcsfs/tests/perf/microbenchmarks/<method_name>` and the four following required files:

   *   `configs.yaml`: Define the benchmark scenarios containing `common` and `scenarios` sections (e.g., different depths, folder counts, and file numbers).
   *   `parameters.py`: Define a dataclass describing your benchmark parameters (e.g., `<Method>BenchmarkParameters`), subclassing an appropriate base parameters class (e.g., `ListingBenchmarkParameters` from `gcsfs.tests.perf.microbenchmarks.listing.parameters` or `IOBenchmarkParameters` from `gcsfs.tests.perf.microbenchmarks.parameters`).
   *   `configs.py`: Create the Configurator class subclassing `BaseBenchmarkConfigurator` from `gcsfs.tests.perf.microbenchmarks.configs`, and a getter function like `get_<method_name>_benchmark_cases()`.
   *   `test_<method_name>.py`: Implement the benchmark tests. Decorate your generic tests with `@pytest.mark.parametrize` (indirectly pulling parameters from the configurator output) and use methods such as `run_single_threaded` or `run_multi_threaded` from `gcsfs.tests.perf.microbenchmarks.runner`.

4. **Verify Independence**
   Ensure your code uses proper module paths to `gcsfs.tests.perf.microbenchmarks.*`. Notice that `run.py` dynamically recognizes benchmark groups based on subdirectories and requires no manual changes.
