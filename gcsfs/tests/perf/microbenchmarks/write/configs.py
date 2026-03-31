import itertools

from gcsfs.tests.perf.microbenchmarks.configs import BaseBenchmarkConfigurator
from gcsfs.tests.perf.microbenchmarks.conftest import MB

from .parameters import WriteBenchmarkParameters


class WriteConfigurator(BaseBenchmarkConfigurator):
    def build_cases(self, scenario, common_config):
        procs_list = scenario.get("processes", [1])
        threads_list = scenario.get("threads", [1])
        bucket_types = common_config.get("bucket_types", ["regional"])
        chunk_sizes_mb = common_config.get("chunk_sizes_mb", [64, 100])
        block_sizes_mb = common_config.get("block_sizes_mb", [16])
        scenario_block_sizes_mb = scenario.get("block_sizes_mb")
        if scenario_block_sizes_mb:
            block_sizes_mb = scenario_block_sizes_mb
        runtime = common_config.get("runtime", 30)
        rounds = common_config.get("rounds", 1)

        cases = []
        param_combinations = itertools.product(
            procs_list, threads_list, chunk_sizes_mb, block_sizes_mb, bucket_types
        )

        for (
            procs,
            threads,
            chunk_size_mb,
            block_size_mb,
            bucket_type,
        ) in param_combinations:
            bucket_name = self.get_bucket_name(bucket_type)
            if not bucket_name:
                continue

            name = (
                f"{scenario['name']}_{procs}procs_{threads}threads_"
                f"{chunk_size_mb}MB_chunk_{block_size_mb}MB_block_{bucket_type}_{runtime}s_duration"
            )

            params = WriteBenchmarkParameters(
                name=name,
                bucket_name=bucket_name,
                bucket_type=bucket_type,
                threads=threads,
                processes=procs,
                files=threads * procs,
                rounds=rounds,
                chunk_size_bytes=int(chunk_size_mb * MB),
                block_size_bytes=int(block_size_mb * MB),
                file_size_bytes=0,
                runtime=runtime,
            )
            cases.append(params)
        return cases


def get_write_benchmark_cases():
    return WriteConfigurator(__file__).generate_cases()
