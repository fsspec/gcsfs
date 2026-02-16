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
        scenario_depth = scenario.get("depth", 0)
        pattern = scenario.get("pattern", "N/A")

        files_list = self._get_files_list(scenario, common_config)
        folders_list = self._get_folders_list(scenario, common_config)
        extra_iterables = self._get_extra_iterables(scenario, common_config)

        cases = []
        # Combine base iterables with any extra ones from subclasses
        iterables = [
            procs_list,
            threads_list,
            files_list,
            bucket_types,
            folders_list,
        ] + extra_iterables

        param_combinations = itertools.product(*iterables)

        for combination in param_combinations:
            # Unpack base parameters (first 5)
            procs, threads, files, bucket_type, folders, *extra_values = combination

            bucket_name = self.get_bucket_name(bucket_type)
            if not bucket_name:
                continue

            if scenario_depth is None:
                depth = (threads * procs) - 1
            else:
                depth = scenario_depth

            name = self._create_case_name(
                scenario["name"],
                procs,
                threads,
                files,
                depth,
                folders,
                pattern,
                bucket_type,
                *extra_values,
            )

            params = self._create_params(
                name=name,
                bucket_name=bucket_name,
                bucket_type=bucket_type,
                threads=threads,
                processes=procs,
                files=files,
                rounds=rounds,
                depth=depth,
                folders=folders,
                pattern=pattern,
                extra_values=extra_values,
            )
            cases.append(params)
        return cases

    def _get_files_list(self, scenario, common_config):
        return scenario.get("files", [10000])

    def _get_folders_list(self, scenario, common_config):
        return scenario.get("folders", [1])

    def _get_extra_iterables(self, scenario, common_config):
        return []

    def _create_case_name(
        self,
        scenario_name,
        procs,
        threads,
        files,
        depth,
        folders,
        pattern,
        bucket_type,
        *extra_values,
    ):
        name = (
            f"{scenario_name}_{procs}procs_{threads}threads_"
            f"{files}files_{depth}depth_{folders}folders"
        )
        if pattern != "N/A":
            name += f"_{pattern}"
        name += f"_{bucket_type}"
        return name

    def _create_params(
        self,
        name,
        bucket_name,
        bucket_type,
        threads,
        processes,
        files,
        rounds,
        depth,
        folders,
        pattern,
        extra_values,
    ):
        return self.param_class(
            name=name,
            bucket_name=bucket_name,
            bucket_type=bucket_type,
            threads=threads,
            processes=processes,
            files=files,
            rounds=rounds,
            depth=depth,
            folders=folders,
            pattern=pattern,
        )


def get_listing_benchmark_cases():
    return ListingConfigurator(__file__).generate_cases()
