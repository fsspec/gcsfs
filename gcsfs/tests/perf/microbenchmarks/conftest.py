import logging
import os
import statistics
import uuid
from typing import Any, Callable, List

import pytest

MB = 1024 * 1024

try:
    # This import is used to check if the pytest-benchmark plugin is installed.
    import pytest_benchmark  # noqa: F401

    benchmark_plugin_installed = True
except ImportError:
    benchmark_plugin_installed = False


@pytest.fixture
def gcsfs_benchmark_read_write(extended_gcs_factory, request):
    """
    A fixture that creates temporary files for a benchmark run and cleans
    them up afterward.

    It uses the `BenchmarkParameters` object from the test's parametrization
    to determine how many files to create and of what size.
    """
    params = request.param
    gcs = extended_gcs_factory(block_size=params.block_size_bytes)

    prefix = f"{params.bucket_name}/benchmark-files-{uuid.uuid4()}"
    file_paths = [f"{prefix}/file_{i}" for i in range(params.num_files)]

    logging.info(
        f"Setting up benchmark '{params.name}': creating {params.num_files} file(s) "
        f"of size {params.file_size_bytes / 1024 / 1024:.2f} MB each."
    )

    # Define a 16MB chunk size for writing
    chunk_size = 16 * 1024 * 1024
    chunks_to_write = params.file_size_bytes // chunk_size
    remainder = params.file_size_bytes % chunk_size

    # Create files by writing random chunks to avoid high memory usage
    for path in file_paths:
        logging.info(f"Creating file {path}.")
        with gcs.open(path, "wb") as f:
            for _ in range(chunks_to_write):
                f.write(os.urandom(chunk_size))
            if remainder > 0:
                f.write(os.urandom(remainder))

    yield gcs, file_paths, params

    # --- Teardown ---
    logging.info(f"Tearing down benchmark '{params.name}': deleting files.")
    try:
        gcs.rm(prefix, recursive=True)
    except Exception as e:
        logging.error(f"Failed to clean up benchmark files: {e}")


if benchmark_plugin_installed:

    def pytest_benchmark_generate_json(config, benchmarks, machine_info, commit_info):
        """
        Hook to post-process benchmark results before generating the JSON report.
        """
        for bench in benchmarks:
            if "timings" in bench.get("extra_info", {}):
                bench.stats.data = bench.extra_info["timings"]
                bench.stats.min = bench.extra_info["min_time"]
                bench.stats.max = bench.extra_info["max_time"]
                bench.stats.mean = bench.extra_info["mean_time"]
                bench.stats.median = bench.extra_info["median_time"]
                bench.stats.stddev = bench.extra_info["stddev_time"]
                bench.stats.rounds = bench.extra_info["rounds"]

                del bench.extra_info["timings"]
                del bench.extra_info["min_time"]
                del bench.extra_info["max_time"]
                del bench.extra_info["mean_time"]
                del bench.extra_info["median_time"]
                del bench.extra_info["stddev_time"]


def publish_benchmark_extra_info(
    benchmark: Any, params: Any, benchmark_group: str
) -> None:
    """
    Helper function to publish benchmark parameters to the extra_info property.
    """
    benchmark.extra_info["num_files"] = params.num_files
    benchmark.extra_info["file_size"] = params.file_size_bytes
    benchmark.extra_info["chunk_size"] = params.chunk_size_bytes
    benchmark.extra_info["block_size"] = params.block_size_bytes
    benchmark.extra_info["pattern"] = params.pattern
    benchmark.extra_info["threads"] = params.num_threads
    benchmark.extra_info["rounds"] = params.rounds
    benchmark.extra_info["bucket_name"] = params.bucket_name
    benchmark.extra_info["bucket_type"] = params.bucket_type
    benchmark.extra_info["processes"] = params.num_processes
    benchmark.group = benchmark_group


def publish_multi_process_benchmark_extra_info(
    benchmark: Any, round_durations_s: List[float], params: Any
) -> None:
    """
    Calculate statistics for multi-process benchmarks and publish them
    to extra_info.
    """
    if not round_durations_s:
        return

    min_time = min(round_durations_s)
    max_time = max(round_durations_s)
    mean_time = statistics.mean(round_durations_s)
    median_time = statistics.median(round_durations_s)
    stddev_time = (
        statistics.stdev(round_durations_s) if len(round_durations_s) > 1 else 0.0
    )

    # Build the results table as a single multi-line string to log it cleanly.
    results_table = (
        f"\n{'-' * 90}\n"
        f"{'Name (time in s)':<50s} {'Min':>8s} {'Max':>8s} {'Mean':>8s} {'Rounds':>8s}\n"
        f"{'-' * 90}\n"
        f"{params.name:<50s} {min_time:>8.4f} {max_time:>8.4f} {mean_time:>8.4f} {params.rounds:>8d}\n"
        f"{'-' * 90}"
    )
    logging.info(f"Multi-process benchmark results:{results_table}")

    benchmark.extra_info["timings"] = round_durations_s
    benchmark.extra_info["min_time"] = min_time
    benchmark.extra_info["max_time"] = max_time
    benchmark.extra_info["mean_time"] = mean_time
    benchmark.extra_info["median_time"] = median_time
    benchmark.extra_info["stddev_time"] = stddev_time


def with_processes(base_cases_func: Callable) -> Callable:
    """
    A decorator that generates benchmark cases for different process counts.

    It reads process counts from the BENCHMARK_PROCESSES setting and creates
    variants for each specified count, updating the case name, num_processes,
    and num_files.
    """
    from gcsfs.tests.settings import BENCHMARK_PROCESSES

    def wrapper():
        base_cases = base_cases_func()
        new_cases = []
        for case in base_cases:
            for procs in BENCHMARK_PROCESSES:
                new_case = case.__class__(**case.__dict__)
                new_case.num_processes = procs
                new_case.num_files = new_case.num_threads * procs
                new_case.name = f"{case.name}_{procs}procs"
                new_cases.append(new_case)
        return new_cases

    return wrapper


def with_threads(base_cases_func: Callable) -> Callable:
    """
    A decorator that generates benchmark cases for different thread counts.

    It reads thread counts from the BENCHMARK_THREADS setting and creates
    variants for each specified count, updating the case name and num_threads.
    num_files will be updated by with_processes decorator.
    """
    from gcsfs.tests.settings import BENCHMARK_THREADS

    def wrapper():
        base_cases = base_cases_func()
        new_cases = []
        for case in base_cases:
            for threads in BENCHMARK_THREADS:
                new_case = case.__class__(**case.__dict__)
                new_case.num_threads = threads
                new_case.num_files = threads * new_case.num_processes
                new_case.name = f"{case.name}_{threads}threads"
                new_cases.append(new_case)
        return new_cases

    return wrapper


def with_file_sizes(base_cases_func: Callable) -> Callable:
    """
    A decorator that generates benchmark cases for different file sizes.

    It reads file sizes from the BENCHMARK_FILE_SIZES_MB setting and creates
    variants for each specified size, updating the case name and file size parameter.
    """
    from gcsfs.tests.settings import BENCHMARK_FILE_SIZES_MB

    if not BENCHMARK_FILE_SIZES_MB:
        logging.error("No file sizes defined. Please set GCSFS_BENCHMARK_FILE_SIZES.")
        pytest.fail("No file sizes defined")

    def wrapper():
        base_cases = base_cases_func()
        new_cases = []
        for case in base_cases:
            for size_mb in BENCHMARK_FILE_SIZES_MB:
                new_case = case.__class__(**case.__dict__)
                new_case.file_size_bytes = size_mb * MB
                new_case.name = f"{case.name}_{size_mb}MB_file"
                new_cases.append(new_case)
        return new_cases

    return wrapper


def _get_bucket_name_for_type(bucket_type: str) -> str:
    """Returns the bucket name variable for a given bucket type."""
    from gcsfs.tests.settings import TEST_BUCKET, TEST_HNS_BUCKET, TEST_ZONAL_BUCKET

    if bucket_type == "regional":
        return TEST_BUCKET
    if bucket_type == "zonal":
        return TEST_ZONAL_BUCKET
    if bucket_type == "hns":
        return TEST_HNS_BUCKET
    return ""


def with_bucket_types(bucket_types: List[str]) -> Callable:
    """
    A decorator that generates benchmark cases for different bucket types.

    Args:
        bucket_types: A list of bucket type tags (e.g., "regional", "zonal").
    """

    def decorator(base_cases_func):
        def wrapper():
            base_cases = base_cases_func()
            all_cases = []
            bucket_configs = [
                (_get_bucket_name_for_type(tag), tag) for tag in bucket_types
            ]
            for case in base_cases:
                for bucket_name, bucket_tag in bucket_configs:
                    if bucket_name:  # Only create cases if bucket is specified
                        new_case = case.__class__(**case.__dict__)
                        new_case.bucket_name = bucket_name
                        new_case.bucket_type = bucket_tag
                        new_case.name = f"{case.name}_{bucket_tag}"
                        all_cases.append(new_case)
            return all_cases

        return wrapper

    return decorator
