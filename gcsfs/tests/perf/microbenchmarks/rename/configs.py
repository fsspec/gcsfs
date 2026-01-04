import itertools

from gcsfs.tests.perf.microbenchmarks.configs import BaseBenchmarkConfigurator
from gcsfs.tests.perf.microbenchmarks.rename.parameters import RenameBenchmarkParameters


class RenameConfigurator(BaseBenchmarkConfigurator):
    def build_cases(self, scenario, common_config):
        procs_list = scenario.get("processes", [1])
        threads_list = scenario.get("threads", [1])
        rounds = common_config.get("rounds", 1)
        bucket_types = common_config.get("bucket_types", ["regional"])
        num_files_list = common_config.get("num_files", [10000])
        depth = scenario.get("depth", 0)
        num_folders_list = scenario.get("num_folders", [1])

        cases = []
        param_combinations = itertools.product(
            procs_list,
            threads_list,
            num_files_list,
            bucket_types,
            num_folders_list,
        )

        for (
            procs,
            threads,
            num_files,
            bucket_type,
            num_folders,
        ) in param_combinations:
            bucket_name = self.get_bucket_name(bucket_type)
            if not bucket_name:
                continue

            name = (
                f"{scenario['name']}_{procs}procs_{threads}threads_"
                f"{num_files}files_{depth}depth_{num_folders}folders_{bucket_type}"
            )

            params = RenameBenchmarkParameters(
                name=name,
                bucket_name=bucket_name,
                bucket_type=bucket_type,
                num_threads=threads,
                num_processes=procs,
                num_files=num_files,
                depth=depth,
                num_folders=num_folders,
                rounds=rounds,
            )
            cases.append(params)
        return cases


def get_rename_benchmark_cases():
    return RenameConfigurator(__file__).generate_cases()
