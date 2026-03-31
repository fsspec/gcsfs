import itertools

from gcsfs.tests.perf.microbenchmarks.configs import BaseBenchmarkConfigurator
from gcsfs.tests.perf.microbenchmarks.conftest import MB

from .parameters import ReadBenchmarkParameters


class ReadConfigurator(BaseBenchmarkConfigurator):
    def build_cases(self, scenario, common_config):
        procs_list = scenario.get("processes", [1])
        threads_list = scenario.get("threads", [1])
        rounds = common_config.get("rounds", 1)
        bucket_types = common_config.get("bucket_types", ["regional"])
        file_sizes_mb = common_config.get("file_sizes_mb", [128])
        chunk_sizes_mb = common_config.get("chunk_sizes_mb", [16])
        block_sizes_mb = scenario.get("block_sizes_mb", [5])

        pattern = scenario.get("pattern", "seq")
        runtime = common_config.get("runtime", 30)
        scenario_files = scenario.get("files")

        cases = []
        param_combinations = itertools.product(
            procs_list,
            threads_list,
            file_sizes_mb,
            chunk_sizes_mb,
            block_sizes_mb,
            bucket_types,
        )

        for (
            procs,
            threads,
            size_mb,
            chunk_size_mb,
            block_size_mb,
            bucket_type,
        ) in param_combinations:
            bucket_name = self.get_bucket_name(bucket_type)
            if not bucket_name:
                continue

            name = (
                f"{scenario['name']}_{procs}procs_{threads}threads_"
                f"{size_mb}MB_file_{chunk_size_mb}MB_chunk_{block_size_mb}MB_block_{bucket_type}"
            )

            if scenario_files is not None:
                files_count = scenario_files
            else:
                files_count = threads * procs

            params = ReadBenchmarkParameters(
                name=name,
                pattern=pattern,
                bucket_name=bucket_name,
                bucket_type=bucket_type,
                threads=threads,
                processes=procs,
                files=files_count,
                rounds=rounds,
                chunk_size_bytes=int(chunk_size_mb * MB),
                block_size_bytes=int(block_size_mb * MB),
                file_size_bytes=int(size_mb * MB),
                runtime=runtime,
            )
            cases.append(params)
        return cases


def get_read_benchmark_cases():
    return ReadConfigurator(__file__).generate_cases()
