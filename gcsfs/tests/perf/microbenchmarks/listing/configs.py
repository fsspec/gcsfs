import itertools

from gcsfs.tests.perf.microbenchmarks.configs import BaseBenchmarkConfigurator
from gcsfs.tests.perf.microbenchmarks.listing.parameters import (
    ListingBenchmarkParameters,
)


class ListingConfigurator(BaseBenchmarkConfigurator):
    param_class = ListingBenchmarkParameters

    def build_cases(self, scenario, common_config):
        procs_list = scenario.get("processes", [1])
        threads_list = scenario.get("threads", [1])
        rounds = common_config.get("rounds", 1)
        bucket_types = common_config.get("bucket_types", ["regional"])
        files_list = common_config.get("files", [10000])
        scenario_depth = scenario.get("depth")
        folders_list = scenario.get("folders", [1])
        pattern = scenario.get("pattern", "ls")

        cases = []
        param_combinations = itertools.product(
            procs_list, threads_list, files_list, bucket_types, folders_list
        )

        for procs, threads, files, bucket_type, folders in param_combinations:
            bucket_name = self.get_bucket_name(bucket_type)
            if not bucket_name:
                continue

            if scenario_depth is None:
                depth = (threads * procs) - 1
            else:
                depth = scenario_depth

            name = (
                f"{scenario['name']}_{procs}procs_{threads}threads_"
                f"{files}files_{depth}depth_{folders}folders_"
                f"{pattern}pattern_{bucket_type}"
            )

            params = self.param_class(
                name=name,
                bucket_name=bucket_name,
                bucket_type=bucket_type,
                threads=threads,
                processes=procs,
                files=files,
                depth=depth,
                folders=folders,
                rounds=rounds,
                pattern=pattern,
            )
            cases.append(params)
        return cases


def get_listing_benchmark_cases():
    return ListingConfigurator(__file__).generate_cases()
