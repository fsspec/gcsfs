import itertools

from gcsfs.tests.perf.microbenchmarks.configs import BaseBenchmarkConfigurator
from gcsfs.tests.perf.microbenchmarks.conftest import MB
from gcsfs.tests.perf.microbenchmarks.write.parameters import WriteBenchmarkParameters


class WriteConfigurator(BaseBenchmarkConfigurator):
    def build_cases(self, scenario, common_config):
        procs_list = scenario.get("processes", [1])
        threads_list = scenario.get("threads", [1])
        bucket_types = common_config.get("bucket_types", ["regional"])
        file_sizes_mb = common_config.get("file_sizes_mb", [1024])
        chunk_sizes_mb = common_config.get("chunk_sizes_mb", [64, 100])

        cases = []
        param_combinations = itertools.product(
            procs_list, threads_list, file_sizes_mb, chunk_sizes_mb, bucket_types
        )

        for procs, threads, size_mb, chunk_size_mb, bucket_type in param_combinations:
            bucket_name = self.get_bucket_name(bucket_type)
            if not bucket_name:
                continue

            name = (
                f"{scenario['name']}_{procs}procs_{threads}threads_"
                f"{size_mb}MB_file_{chunk_size_mb}MB_chunk_{bucket_type}"
            )

            params = WriteBenchmarkParameters(
                name=name,
                bucket_name=bucket_name,
                bucket_type=bucket_type,
                num_threads=threads,
                num_processes=procs,
                num_files=threads * procs,
                rounds=common_config.get("rounds", 10),
                chunk_size_bytes=chunk_size_mb * MB,
                file_size_bytes=size_mb * MB,
            )
            cases.append(params)
        return cases


def get_write_benchmark_cases():
    return WriteConfigurator(__file__).generate_cases()
