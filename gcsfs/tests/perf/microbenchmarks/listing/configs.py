import itertools
import logging
import os

import yaml

from gcsfs.tests.conftest import BUCKET_NAME_MAP
from gcsfs.tests.perf.microbenchmarks.listing.parameters import (
    ListingBenchmarkParameters,
)
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
        num_files_list = common_config.get("num_files", [1000])
        bucket_types = common_config.get("bucket_types", ["regional"])

        param_combinations = itertools.product(
            procs_list, threads_list, num_files_list, bucket_types
        )

        for procs, threads, num_files, bucket_type in param_combinations:
            bucket_name = BUCKET_NAME_MAP.get(bucket_type)
            if not bucket_name:
                continue

            depth = (threads * procs) - 1

            name = (
                f"{scenario['name']}_{procs}procs_{threads}threads_"
                f"{num_files}files_{depth + 1}depth_{bucket_type}"
            )

            params = ListingBenchmarkParameters(
                name=name,
                bucket_name=bucket_name,
                bucket_type=bucket_type,
                num_threads=threads,
                num_processes=procs,
                num_files=num_files,
                depth=depth,
                rounds=common_config.get("rounds", 10),
            )
            all_cases.append(params)

    return all_cases


def get_listing_benchmark_cases():
    """
    Returns a list of ListingBenchmarkParameters, optionally filtered by
    the GCSFS_BENCHMARK_FILTER environment variable.
    """
    all_cases = _generate_benchmark_cases()
    if all_cases:
        logging.info(
            f"List Benchmark cases to be triggered: {', '.join([case.name for case in all_cases])}"
        )
    return all_cases
