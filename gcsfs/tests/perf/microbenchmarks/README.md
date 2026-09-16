# GCSFS Microbenchmarks

## Introduction

GCSFS microbenchmarks are a suite of performance tests designed to evaluate the efficiency and latency of various Google Cloud Storage file system operations, including read, cat, write, put, listing, walk, delete, rename, open, glob, and cat_ranges.

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
    *   `runtime`: Duration in seconds for the benchmark to run.

*   **Listing Parameters**: Specific to Listing, Delete, Rename, and Info operations.
    *   `depth`: Directory depth.
    *   `folders`: Number of folders.
    *   `pattern`: Listing pattern (e.g., "ls", "find", "walk").

*   **Info Parameters**: Specific to Info operations (extends Listing Parameters).
    *    `target_type`: The type of target to query: "bucket", "folder", or "file".

*   **Open Parameters**: Specific to Open operations.
    *    `folders`: Number of folders to distribute files into.

*   **Cat Parameters**: Specific to whole-object reads (extends IO Parameters).
    *    `pattern`: `whole` (`cat_file(path)`), `ranged` (`cat_file(path, start=, end=)`), or `batch` (`cat(paths)`).
    *    `concurrency`: Value passed as `cat_file(concurrency=...)`. Unset means the gcsfs default, which is what production code gets.
    *    Sizes for this group are configured in **bytes** (`file_sizes_bytes`), not MB — the objects it cares about are as small as 8 bytes.

*   **Cat Ranges Parameters**: Specific to `cat_ranges` operations.
    *   `num_ranges`: Number of byte ranges requested across files.
    *   `batch_size`: Batch size for concurrent range operations.
    *   `max_gap`: Maximum gap in bytes between adjacent ranges to coalesce into a single request (`0` for contiguous/overlapping ranges only).

## Request counting

Latency alone does not catch **request amplification**. An extra metadata
round-trip per read is a few milliseconds: invisible in throughput on a
multi-GB object, and lost in network noise on a small one. This is how
[fsspec/gcsfs#1048](https://github.com/fsspec/gcsfs/issues/1048) shipped —
`cat_file()` on a small object quietly went from one HTTP round-trip to three
(object GET + objects.list + download) and no benchmark moved.

So benchmarks that know how many filesystem operations they perform pass
`operations_per_round` to `run_single_threaded` / `run_multi_threaded`. That
counts the HTTP calls gcsfs issues and reports them as a **`Requests/Op`**
column next to latency:

| Column | Meaning |
| :--- | :--- |
| `requests_per_op` | Round-trips per filesystem operation. A whole-object read should be `1`. |
| `requests_download` | Object media downloads (`?alt=media`). |
| `requests_object_get` | Object metadata GETs (`storage.objects.get`). |
| `requests_list` | Object listings (`storage.objects.list`). |
| `requests_other` | Everything else (writes, deletes, bucket calls). |

Two caveats:

*   Only the JSON API is counted. Zonal buckets serve reads over gRPC via the
    multi-range downloader, which does not go through `_call`, so their counts
    read as ~0. Compare latency there, not counts.
*   Counting happens in-process, so multi-process cases are not counted.

The budget is also pinned in CI, without needing a bucket: `test_request_counter.py`
stubs the transport and asserts that a whole-object read costs one round-trip.
It currently `xfail`s against #1048 and will flip to `XPASS` once that is fixed.

## Configuration

Configuration values are stored in YAML files (e.g., `configs.yaml`) located within each benchmark's directory. These files define:

*   **Common**: Shared settings like bucket types, file sizes, rounds, or runtime.
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
| `--group` | The benchmark group to run (e.g., `read`, `cat`, `write`, `put`, `listing`, `info`, `open`, `glob`, `cat_ranges`). Runs all groups if not specified. | No |
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

## Benchmark Comparison (`compare.py`)

Compare two benchmark JSON runs to detect regressions:

```bash
python gcsfs/tests/perf/microbenchmarks/compare.py base.json pr.json --threshold=5.0
```

* `--threshold`: Percentage degradation failure threshold (default: `5.0`%). Exits with code `1` on regression.
* `--output-markdown`: Writes a Markdown summary table for PR comments or CI summaries.

## PR Performance Testing

* **Trigger**: Apply the `execute-perf-test` label to the PR and comment `/gcbrun`.
* **Label Filtering**: If the `execute-perf-test` label is not present on the PR, the pipeline exits early in seconds without creating VMs or buckets, conserving cloud resources and CI quota.
* **Environment**: Executes in Cloud Build on a GCP VM against real Regional, Zonal, and HNS buckets.
* **Comparison**: Runs benchmarks on both base (`main`) and PR branches; fails if any metric degrades by more than **5%**.
