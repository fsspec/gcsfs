# GCSFS Microbenchmarks

## Introduction

This document describes the microbenchmark suite for `gcsfs`. These benchmarks are designed to measure the performance of various I/O operations under different conditions. They are built using `pytest` and the `pytest-benchmark` plugin to provide detailed performance metrics for single-threaded, multi-threaded, and multi-process scenarios.

## Prerequisites

Before running the benchmarks, ensure you have the necessary packages installed. The required packages are listed in `gcsfs/tests/perf/microbenchmarks/requirements.txt`.

You can install them using pip:

```bash
pip install -r gcsfs/tests/perf/microbenchmarks/requirements.txt
```

This will install `pytest`, `pytest-benchmark`, and other necessary dependencies. For more information on `pytest-benchmark`, you can refer to its official documentation. [1]

## Read Benchmarks

The read benchmarks are located in `gcsfs/tests/perf/microbenchmarks/read/` and are designed to test read performance with various configurations.

### Parameters

The read benchmarks are defined by the `ReadBenchmarkParameters` class in `read/parameters.py`. Key parameters include:

*   `name`: The name of the benchmark configuration.
*   `num_files`: The number of files to use, this is always num_processes x num_threads.
*   `pattern`: Read pattern, either sequential (`seq`) or random (`rand`).
*   `num_threads`: Number of threads for multi-threaded tests.
*   `num_processes`: Number of processes for multi-process tests.
*   `block_size_bytes`: The block size for gcsfs file buffering. Defaults to `16MB`.
*   `chunk_size_bytes`: The size of each read operation. Defaults to `16MB`.
*   `file_size_bytes`: The total size of each file.
*   `rounds`: The total number of pytest-benchmark rounds for each parameterized test. Defaults to `10`.


To ensure that the results are stable and not skewed by outliers, each benchmark is run for a set number of rounds.
By default, this is set to 10 rounds, but it can be configured via `rounds` parameter if needed. This helps in providing a more accurate and reliable performance profile.

### Configurations

The base configurations in `read/configs.py` are simplified to just `read_seq` and `read_rand`. Decorators are then used to generate a full suite of test cases by creating variations for parallelism, file sizes, and bucket types.

The benchmarks are split into three main test functions based on the execution model:

*   `test_read_single_threaded`: Measures baseline performance of read operations.
*   `test_read_multi_threaded`: Measures performance with multiple threads.
*   `test_read_multi_process`: Measures performance using multiple processes, each with its own set of threads.

These tests are parameterized using a set of decorators that generate the final benchmark cases:

*   `@with_processes`: Creates variants for different process counts, configured via `GCSFS_BENCHMARK_PROCESSES`.
*   `@with_threads`: Creates variants for different thread counts, configured via `GCSFS_BENCHMARK_THREADS`.
*   `@with_file_sizes`: Creates variants for different file sizes, configured via `GCSFS_BENCHMARK_FILE_SIZES`.
*   `@with_bucket_types`: Creates variants for different GCS bucket types (e.g., regional, zonal).

### Running Benchmarks with `pytest`

You can use `pytest` to run the benchmarks directly. The `-k` option is useful for filtering tests by name.

**Examples:**

Run all read benchmarks:
```bash
pytest gcsfs/tests/perf/microbenchmarks/read/
```

Run only single-threaded read benchmarks:
```bash
pytest -k "test_read_single_threaded" gcsfs/tests/perf/microbenchmarks/read/test_read.py
```

Run multi-process benchmarks for a specific configuration (e.g., 4 processes, 4 threads):
```bash
pytest -k "read_seq_4procs_4threads" gcsfs/tests/perf/microbenchmarks/read/
```

Run a specific benchmark configuration by setting `GCSFS_BENCHMARK_FILTER`. This is useful for targeting a single configuration defined in `read/configs.py`.
```bash
export GCSFS_BENCHMARK_FILTER="read_seq_1thread"
pytest -k "read_seq_4procs_4threads" gcsfs/tests/perf/microbenchmarks/read/
```

## Function-level Fixture: `gcsfs_benchmark_read_write`

A function-level `pytest` fixture named `gcsfs_benchmark_read_write` (defined in `conftest.py`) is used to set up and tear down the environment for the benchmarks.

### Setup and Teardown

*   **Setup**: Before a benchmark function runs, this fixture creates the specified number of files with the configured size in a temporary directory within the test bucket. It uses `os.urandom()` to write data in chunks to avoid high memory usage.
*   **Teardown**: After the benchmark completes, the fixture recursively deletes the temporary directory and all the files created during the setup phase.

Here is how the fixture is used in a test:

```python
@pytest.mark.parametrize(
    "gcsfs_benchmark_read_write",
    single_threaded_cases,
    indirect=True,
    ids=lambda p: p.name,
)
def test_read_single_threaded(benchmark, gcsfs_benchmark_read_write):
    gcs, file_paths, params = gcsfs_benchmark_read_write
    # ... benchmark logic ...
```

### Environment Variables
To run the benchmarks, you need to configure your environment.
The orchestrator script (`run.py`) sets these for you, but if you are running `pytest` directly, you will need to export them.

*   `GCSFS_TEST_BUCKET`: The name of a regional GCS bucket.
*   `GCSFS_ZONAL_TEST_BUCKET`: The name of a zonal GCS bucket.
*   `GCSFS_HNS_TEST_BUCKET`: The name of an HNS-enabled GCS bucket.

You must also set the following environment variables to ensure that the benchmarks run against the live GCS API and that experimental features are enabled.

```bash
export STORAGE_EMULATOR_HOST="https://storage.googleapis.com"
export GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT="true"
```

### `settings.py`

The `gcsfs/tests/perf/settings.py` file defines how benchmark parameters can be configured through environment variables:

*   `GCSFS_BENCHMARK_FILTER`: A comma-separated list of names to filter which benchmark configurations to run.
*   `GCSFS_BENCHMARK_FILE_SIZES`: A comma-separated list of file sizes in MB (e.g., "128,1024"). Defaults to "1024".
*   `GCSFS_BENCHMARK_THREADS`: A comma-separated list of thread counts to test (e.g., "1,4,8,16"). Defaults to "16".
*   `GCSFS_BENCHMARK_PROCESSES`: A comma-separated list of process counts to test (e.g., "1,4"). Defaults to "1".
*   `GCSFS_BENCHMARK_CHUNK_SIZE_MB`: The size of each read operation in MB. Defaults to "64".
*   `GCSFS_BENCHMARK_BLOCK_SIZE_MB`: The block size for gcsfs file buffering in MB. Defaults to "64".
*   `GCSFS_BENCHMARK_ROUNDS`: The number of rounds for each benchmark test. Defaults to "10".

## Orchestrator Script (`run.py`)

An orchestrator script, `run.py`, is provided to simplify running the benchmark suite. It wraps `pytest`, sets up the necessary environment variables, and generates a summary report.

### Parameters

The script accepts several command-line arguments:

*   `--group`: The benchmark group to run (e.g., `read`).
*   `--config`: The name of a specific benchmark configuration to run (e.g., `read_seq`).
*   `--name`: A keyword to filter tests by name (passed to `pytest -k`).
*   `--regional-bucket`: Name of the Regional GCS bucket.
*   `--zonal-bucket`: Name of the Zonal GCS bucket.
*   `--hns-bucket`: Name of the HNS GCS bucket.
*   `--log`: Set to `true` to enable `pytest` console logging.
*   `--log-level`: Sets the log level (e.g., `INFO`, `DEBUG`).

**Important Notes:**
*   You must provide at least one bucket name (`--regional-bucket`, `--zonal-bucket`, or `--hns-bucket`).

Run the script with `--help` to see all available options:
```bash
python gcsfs/tests/perf/microbenchmarks/run.py --help
```

### Examples

Here are some examples of how to use the orchestrator script from the root of the `gcsfs` repository:

Run all available benchmarks against a regional bucket with default settings. This is the simplest way to trigger all tests across all groups (e.g., read, write):
```bash
python gcsfs/tests/perf/microbenchmarks/run.py --regional-bucket your-regional-bucket
```

Run only the `read` group benchmarks against a regional bucket with the default 128MB file size:
```bash
python gcsfs/tests/perf/microbenchmarks/run.py --group read --regional-bucket your-regional-bucket
```

Run only the single-threaded sequential read benchmark with 256MB and 512MB file sizes:
```bash
python gcsfs/tests/perf/microbenchmarks/run.py \
  --group read \
  --config "read_seq_1thread" \
  --regional-bucket your-regional-bucket
```

Run all read benchmarks against both a regional and a zonal bucket:
```bash
python gcsfs/tests/perf/microbenchmarks/run.py \
  --group read \
  --regional-bucket your-regional-bucket \
  --zonal-bucket your-zonal-bucket
```

### Script Output

The script will create a timestamped directory in `gcsfs/tests/perf/microbenchmarks/__run__/` containing the JSON and CSV results, and it will print a summary table to the console.

#### JSON File (`results.json`)

The `results.json` file will contain a structured representation of the benchmark results.
The exact content can vary depending on the pytest-benchmark version and the tests run, but it typically includes:
*   machine_info: Details about the system where the benchmarks were run (e.g., Python version, OS, CPU).
*   benchmarks: A list of individual benchmark results, each containing:
    *   name: The name of the benchmark test.
    *   stats: Performance statistics like min, max, mean, stddev, rounds, iterations, ops (operations per second), q1, q3 (quartiles).
    *   options: Configuration options used for the benchmark (e.g., min_rounds, max_time).
    *   extra_info: Any additional information associated with the benchmark.

#### CSV File (`results.csv`)
The CSV file provides a detailed performance profile of gcsfs operations, allowing for analysis of how different factors like threading, process parallelism, and access patterns affect I/O throughput.
This file is a summarized view of the results generated in the JSON file and for each test run, the file records detailed performance statistics, including:
*   Minimum, maximum, mean, and median execution times in secs.
*   Standard deviation and percentile values (p90, p95, p99) for timing.
*   The maximum throughput achieved, measured in Megabytes per second (MB/s).


#### Summary Table
The script also puts out a nice summary table like below, for quick glance at results.

| Bucket Type | Group | Pattern | Files | Threads | Processes | File Size (MB) | Chunk Size (MB) | Block Size (MB) | Min Latency (s) | Mean Latency (s) | Max Throughput (MB/s) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| regional | read | seq | 1 | 1 | 1 | 128.00 | 16.00 | 16.00 | 0.6391 | 0.7953 | 200.2678 |
| regional | read | rand | 1 | 1 | 1 | 128.00 | 16.00 | 16.00 | 0.6537 | 0.7843 | 195.8066 |
