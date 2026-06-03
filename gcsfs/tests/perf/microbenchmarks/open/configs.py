from gcsfs.tests.perf.microbenchmarks.configs import BaseBenchmarkConfigurator
from gcsfs.tests.perf.microbenchmarks.open.parameters import OpenBenchmarkParameters


class OpenConfigurator(BaseBenchmarkConfigurator):
    param_class = OpenBenchmarkParameters

    def build_cases(self, scenario, common_config):
        cases = []
        bucket_types = common_config.get("bucket_types", ["zonal"])
        files_list = scenario.get("files", [1])
        folders_list = scenario.get("folders", [1])
        threads_list = scenario.get("threads", [1])
        procs_list = scenario.get("processes", [1])
        rounds = scenario.get("rounds", 1)

        for bucket_type in bucket_types:
            bucket_name = self.get_bucket_name(bucket_type)
            if not bucket_name:
                continue

            for files in files_list:
                for folders in folders_list:
                    if "threads" in scenario:
                        for threads in threads_list:
                            name = (
                                f"{scenario['name']}_{threads}threads_"
                                f"{files}files_{folders}folders_{bucket_type}"
                            )
                            cases.append(
                                self._create_params(
                                    name,
                                    bucket_name,
                                    bucket_type,
                                    threads,
                                    1,
                                    files,
                                    rounds,
                                    folders,
                                )
                            )
                    elif "processes" in scenario:
                        for procs in procs_list:
                            name = (
                                f"{scenario['name']}_{procs}procs_"
                                f"{files}files_{folders}folders_{bucket_type}"
                            )
                            cases.append(
                                self._create_params(
                                    name,
                                    bucket_name,
                                    bucket_type,
                                    1,
                                    procs,
                                    files,
                                    rounds,
                                    folders,
                                )
                            )
                    else:
                        name = (
                            f"{scenario['name']}_1procs_1threads_"
                            f"{files}files_{folders}folders_{bucket_type}"
                        )
                        cases.append(
                            self._create_params(
                                name,
                                bucket_name,
                                bucket_type,
                                1,
                                1,
                                files,
                                rounds,
                                folders,
                            )
                        )
        return cases

    def _create_params(
        self,
        name,
        bucket_name,
        bucket_type,
        threads,
        processes,
        files,
        rounds,
        folders,
    ):
        return self.param_class(
            name=name,
            bucket_name=bucket_name,
            bucket_type=bucket_type,
            threads=threads,
            processes=processes,
            files=files,
            rounds=rounds,
            folders=folders,
        )


def get_open_benchmark_cases():
    return OpenConfigurator(__file__).generate_cases()
