from gcsfs.tests.perf.microbenchmarks.info.parameters import InfoBenchmarkParameters
from gcsfs.tests.perf.microbenchmarks.listing.configs import ListingConfigurator


class InfoConfigurator(ListingConfigurator):
    param_class = InfoBenchmarkParameters

    def _get_folders_list(self, scenario, common_config):
        return common_config.get("folders", [1])

    def _get_files_list(self, scenario, common_config):
        return common_config.get("files", [1])

    def _get_extra_iterables(self, scenario, common_config):
        return [scenario.get("target_types", ["bucket", "folder", "file"])]

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
        target_type = extra_values[0]
        name = (
            f"{scenario_name}_{procs}procs_{threads}threads_"
            f"{files}files_{depth}depth_{folders}folders_{target_type}"
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
        target_type = extra_values[0]
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
            target_type=target_type,
        )


def get_info_benchmark_cases():
    return InfoConfigurator(__file__).generate_cases()
