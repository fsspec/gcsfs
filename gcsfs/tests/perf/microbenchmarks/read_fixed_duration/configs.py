import itertools

from gcsfs.tests.perf.microbenchmarks.conftest import MB
from gcsfs.tests.perf.microbenchmarks.read.configs import ReadConfigurator

from .parameters import ReadFixedDurationBenchmarkParameters


class ReadFixedDurationConfigurator(ReadConfigurator):
    def build_cases(self, scenario, common_config):
        procs_list = scenario.get("processes", [1])
        threads_list = scenario.get("threads", [1])
        rounds = common_config.get("rounds", 1)
        bucket_types = common_config.get("bucket_types", ["regional"])
        file_sizes_mb = common_config.get("file_sizes_mb", [128])
        block_sizes_mb = common_config.get("block_sizes_mb", [16])
        pattern = scenario.get("pattern", "seq")
        runtime = common_config.get("runtime", 30)
        scenario_files = scenario.get("files")
        scenario_block_sizes_mb = scenario.get("block_sizes_mb")
        if scenario_block_sizes_mb:
            block_sizes_mb = scenario_block_sizes_mb

        cases = []
        param_combinations = itertools.product(
            procs_list, threads_list, file_sizes_mb, block_sizes_mb, bucket_types
        )

        for procs, threads, size_mb, block_size_mb, bucket_type in param_combinations:
            bucket_name = self.get_bucket_name(bucket_type)
            if not bucket_name:
                continue

            name = (
                f"{scenario['name']}_{procs}procs_{threads}threads_"
                f"{size_mb}MB_file_{block_size_mb}MB_block_{bucket_type}"
            )

            if scenario_files is not None:
                files_count = scenario_files
            else:
                files_count = threads * procs

            params = ReadFixedDurationBenchmarkParameters(
                name=name,
                pattern=pattern,
                bucket_name=bucket_name,
                bucket_type=bucket_type,
                threads=threads,
                processes=procs,
                files=files_count,
                rounds=rounds,
                chunk_size_bytes=int(block_size_mb * MB),
                block_size_bytes=int(5 * MB),
                file_size_bytes=int(size_mb * MB),
                runtime=runtime,
            )
            cases.append(params)
        return cases


def get_read_fixed_duration_benchmark_cases():
    return ReadFixedDurationConfigurator(__file__).generate_cases()
