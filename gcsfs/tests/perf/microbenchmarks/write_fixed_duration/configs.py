import itertools

from gcsfs.tests.perf.microbenchmarks.conftest import MB
from gcsfs.tests.perf.microbenchmarks.write.configs import WriteConfigurator

from .parameters import WriteFixedDurationBenchmarkParameters


class WriteFixedDurationConfigurator(WriteConfigurator):
    def build_cases(self, scenario, common_config):
        procs_list = scenario.get("processes", [1])
        threads_list = scenario.get("threads", [1])
        bucket_types = common_config.get("bucket_types", ["regional"])
        chunk_sizes_mb = common_config.get("chunk_sizes_mb", [64, 100])
        runtime = common_config.get("runtime", 30)
        rounds = common_config.get("rounds", 1)

        cases = []
        param_combinations = itertools.product(
            procs_list, threads_list, chunk_sizes_mb, bucket_types
        )

        for procs, threads, chunk_size_mb, bucket_type in param_combinations:
            bucket_name = self.get_bucket_name(bucket_type)
            if not bucket_name:
                continue

            name = (
                f"{scenario['name']}_{procs}procs_{threads}threads_"
                f"{chunk_size_mb}MB_chunk_{bucket_type}_{runtime}s_duration"
            )

            params = WriteFixedDurationBenchmarkParameters(
                name=name,
                bucket_name=bucket_name,
                bucket_type=bucket_type,
                threads=threads,
                processes=procs,
                files=threads * procs,
                rounds=rounds,
                chunk_size_bytes=chunk_size_mb * MB,
                file_size_bytes=0,
                runtime=runtime,
            )
            cases.append(params)
        return cases


def get_write_fixed_duration_benchmark_cases():
    return WriteFixedDurationConfigurator(__file__).generate_cases()
