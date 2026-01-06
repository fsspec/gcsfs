# GCSFS Microbenchmarks

## Introduction

GCSFS microbenchmarks are a suite of performance tests designed to evaluate the efficiency and latency of various Google Cloud Storage file system operations, including read, write, listing, delete, and rename.

These benchmarks are built using the `pytest` and `pytest-benchmark` frameworks. Each benchmark test is a parameterized pytest case, where the parameters are dynamically configured at runtime from YAML configuration files. This allows for flexible and extensive testing scenarios without modifying the code.

An orchestrator script (`run.py`) is provided to execute specific or all benchmarks, manage the test environment, and generate detailed reports in CSV format along with a summary table.

## How to install

To run the microbenchmarks, you need to install the required dependencies. You can do this using pip:

```bash
pip install -r requirements.txt
```

Ensure you have the necessary Google Cloud credentials set up to access the GCS buckets used in the tests.

## Parameters

The benchmarks use a set of parameter classes to define the configuration for each test case.

*   **Base Parameters**: Common to all benchmarks.
    *   `name`: Unique name for the benchmark case.
    *   `bucket_name`: The GCS bucket used.
    *   `bucket_type`: Type of bucket (regional, zonal, hns).
    *   `threads`: Number of threads.
    *   `processes`: Number of processes.
    *   `files`: Number of files involved.
    *   `rounds`: Number of iterations for the benchmark.

*   **IO Parameters**: Common to Read and Write operations.
    *   `file_size_bytes`: Size of the file.
    *   `chunk_size_bytes`: Size of chunks for I/O operations.

*   **Read Parameters**: Specific to Read operations (extends IO Parameters).
    *   `pattern`: Read pattern ("seq" for sequential, "rand" for random).
    *   `block_size_bytes`: Block size for GCSFS file buffering.

*   **Listing Parameters**: Specific to Listing, Delete, and Rename operations.
    *   `depth`: Directory depth.
    *   `folders`: Number of folders.
    *   `pattern`: Listing pattern (e.g., "ls", "find").

## Configuration

Configuration values are stored in YAML files (e.g., `configs.yaml`) located within each benchmark's directory. These files define:

*   **Common**: Shared settings like bucket types, file sizes, or rounds.
*   **Scenarios**: Specific test scenarios defining variations in threads, processes, patterns, etc.

## Configurators

Configurators are Python classes (e.g., `ReadConfigurator`, `ListingConfigurator`) responsible for parsing the YAML configuration files and converting them into a list of parameter objects (`BenchmarkParameters`). These objects are then consumed by the test files to generate parameterized test cases.

## Benchmark File

The benchmark files (e.g., `test_read.py`, `test_listing.py`) contain the actual test logic. They call the respective configurator to retrieve the list of benchmark cases (parameters).

Each test function is decorated with `@pytest.mark.parametrize` to run multiple variations based on the generated parameters. The benchmarks support three execution modes:

1.  **Single-threaded**: Runs the operation in the main thread.
2.  **Multi-threaded**: Uses `ThreadPoolExecutor` to run operations concurrently within a single process.
3.  **Multi-process**: Uses `multiprocessing` to run operations across multiple processes, each potentially using multiple threads.

## Orchestrator Script

The `run.py` script is the central entry point for executing benchmarks. It handles environment setup, test execution via `pytest`, and report generation.

### Command Line Options

| Option | Description | Required |
| :--- | :--- | :--- |
| `--group` | The benchmark group to run (e.g., `read`, `write`, `listing`). Runs all groups if not specified. | No |
| `--config` | Specific scenario names to run (e.g., `read_seq`, `list_flat`). Accepts multiple values. | No |
| `--regional-bucket` | Name of the regional GCS bucket. | Yes* |
| `--zonal-bucket` | Name of the zonal GCS bucket. | Yes* |
| `--hns-bucket` | Name of the HNS GCS bucket. | Yes* |
| `--log` | Enable console logging (`true` or `false`). Default: `false`. | No |
| `--log-level` | Logging level (e.g., `INFO`, `DEBUG`). Default: `DEBUG`. | No |

*\* At least one bucket type must be provided.*

### Usage Examples

**1. Run all benchmarks**
Runs every available benchmark against a regional bucket.
```bash
python gcsfs/tests/perf/microbenchmarks/run.py --regional-bucket=<BUCKET_NAME>
```

**2. Run a specific group**
Runs only the tests in the `read` directory.
```bash
python gcsfs/tests/perf/microbenchmarks/run.py --group=read --regional-bucket=<BUCKET_NAME>
```

**3. Run specific scenarios**
Runs only the scenarios named `read_seq` and `read_rand`. This is useful for targeting specific configurations defined in the YAML files.
```bash
python gcsfs/tests/perf/microbenchmarks/run.py --config=read_seq,read_rand --regional-bucket=<BUCKET_NAME>
```

**4. Run with multiple bucket types**
Runs benchmarks against both regional and zonal buckets.
```bash
python gcsfs/tests/perf/microbenchmarks/run.py --group=write --regional-bucket=<REGIONAL_BUCKET> --zonal-bucket=<ZONAL_BUCKET>
```

**5. Run with logging enabled**
Enables detailed logging to the console during execution.
```bash
python gcsfs/tests/perf/microbenchmarks/run.py --group=delete --regional-bucket=<BUCKET_NAME> --log=true --log-level=INFO
```

## Output

The orchestrator script generates output in a structured format:

*   **Directory**: Results are saved in a timestamped folder under `__run__` (e.g., `__run__/DDMMYYYY-HHMMSS/`).
*   **JSON**: A raw JSON file generated by `pytest-benchmark` containing detailed statistics.
*   **CSV**: A processed CSV report containing key metrics such as min/max/mean latency, throughput, and resource usage (CPU, Memory).
