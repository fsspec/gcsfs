import itertools
import logging
import os

import yaml

from gcsfs.tests.conftest import BUCKET_NAME_MAP
from gcsfs.tests.perf.microbenchmarks.conftest import MB
from gcsfs.tests.perf.microbenchmarks.write.parameters import WriteBenchmarkParameters
from gcsfs.tests.settings import BENCHMARK_FILTER


def _generate_benchmark_cases():
    """
    Generates benchmark cases by reading the configuration from configs.yaml.
    """
    config_path = os.path.join(os.path.dirname(__file__), "configs.yaml")
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    common_config = config["common"]
    scenarios = config["scenarios"]

    if BENCHMARK_FILTER:
        filter_names = [name.strip().lower() for name in BENCHMARK_FILTER.split(",")]
        scenarios = [s for s in scenarios if s["name"].lower() in filter_names]
    all_cases = []

    for scenario in scenarios:
        procs_list = scenario.get("processes", [1])
        threads_list = scenario.get("threads", [1])
        file_sizes_mb = common_config.get("file_sizes_mb", [1024])
        chunk_sizes_mb = common_config.get("chunk_sizes_mb", [64, 100])
        bucket_types = common_config.get("bucket_types", ["regional"])

        param_combinations = itertools.product(
            procs_list, threads_list, file_sizes_mb, chunk_sizes_mb, bucket_types
        )

        for procs, threads, size_mb, chunk_size_mb, bucket_type in param_combinations:
            bucket_name = BUCKET_NAME_MAP.get(bucket_type)
            if not bucket_name:
                continue

            name = (
                f"{scenario['name']}_{procs}procs_{threads}threads_"
                f"{size_mb}MB_file_{chunk_size_mb}MB_chunk_{bucket_type}"
            )

            params = WriteBenchmarkParameters(
                name=name,
                bucket_name=bucket_name,
                bucket_type=bucket_type,
                num_threads=threads,
                num_processes=procs,
                num_files=threads * procs,
                rounds=common_config.get("rounds", 10),
                chunk_size_bytes=chunk_size_mb * MB,
                file_size_bytes=size_mb * MB,
            )
            all_cases.append(params)

    return all_cases


def get_write_benchmark_cases():
    """
    Returns a list of WriteBenchmarkParameters, optionally filtered by
    the GCSFS_BENCHMARK_FILTER environment variable.
    """
    all_cases = _generate_benchmark_cases()
    logging.info(
        f"Benchmark cases to be triggered: {', '.join([case.name for case in all_cases])}"
    )
    return all_cases
