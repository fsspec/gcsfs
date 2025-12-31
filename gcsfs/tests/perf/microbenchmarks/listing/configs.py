import itertools

from gcsfs.tests.perf.microbenchmarks.configs import BaseBenchmarkConfigurator
from gcsfs.tests.perf.microbenchmarks.listing.parameters import (
    ListingBenchmarkParameters,
)


class ListingConfigurator(BaseBenchmarkConfigurator):
    def build_cases(self, scenario, common_config):
        procs_list = scenario.get("processes", [1])
        threads_list = scenario.get("threads", [1])
        rounds = common_config.get("rounds", 1)
        bucket_types = common_config.get("bucket_types", ["regional"])
        num_files_list = common_config.get("num_files", [10000])
        scenario_depth = scenario.get("depth")

        cases = []
        param_combinations = itertools.product(
            procs_list, threads_list, num_files_list, bucket_types
        )

        for procs, threads, num_files, bucket_type in param_combinations:
            bucket_name = self.get_bucket_name(bucket_type)
            if not bucket_name:
                continue

            if scenario_depth is None:
                depth = (threads * procs) - 1
            else:
                depth = scenario_depth

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
                rounds=rounds,
            )
            cases.append(params)
        return cases


def get_listing_benchmark_cases():
    return ListingConfigurator(__file__).generate_cases()
