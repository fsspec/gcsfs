import logging

from gcsfs.tests.perf.microbenchmarks.conftest import (
    with_bucket_types,
    with_file_sizes,
    with_processes,
    with_threads,
)
from gcsfs.tests.perf.microbenchmarks.read.parameters import ReadBenchmarkParameters
from gcsfs.tests.settings import BENCHMARK_FILTER

# Base configurations for benchmarks decorated with processes, threads, sizes and bucket types
_base_read_benchmark_cases = [
    # Sequential read
    ReadBenchmarkParameters(name="read_seq", pattern="seq"),
    # Random read
    ReadBenchmarkParameters(name="read_rand", pattern="rand"),
]


@with_bucket_types(["regional", "zonal"])
@with_file_sizes
@with_threads
@with_processes
def _filter_and_decorate_benchmark_cases():
    cases_to_run = _base_read_benchmark_cases
    if BENCHMARK_FILTER:
        filter_names = [name.strip().lower() for name in BENCHMARK_FILTER.split(",")]
        cases_to_run = [
            case for case in cases_to_run if case.name.lower() in filter_names
        ]
    return cases_to_run


def get_read_benchmark_cases():
    all_cases = _filter_and_decorate_benchmark_cases()
    logging.info(
        f"Benchmark cases to be triggered: {', '.join([case.name for case in all_cases])}"
    )
    return all_cases
