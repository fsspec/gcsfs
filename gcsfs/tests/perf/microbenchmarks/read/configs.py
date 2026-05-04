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

        if pattern == "mixed":
            seq_probabilities = scenario.get("seq_probabilities", [0.5])
            min_chunk_sizes_mb = scenario.get("min_chunk_sizes_mb", [1])
            max_chunk_sizes_mb = scenario.get("max_chunk_sizes_mb", [16])
            chunk_sizes_mb = [None]  # Hide base chunk size
        else:
            seq_probabilities = [None]
            min_chunk_sizes_mb = [None]
            max_chunk_sizes_mb = [None]
            chunk_sizes_mb = common_config.get("chunk_sizes_mb", [16])

        cases = []
        param_combinations = itertools.product(
            procs_list,
            threads_list,
            file_sizes_mb,
            chunk_sizes_mb,
            block_sizes_mb,
            bucket_types,
            seq_probabilities,
            min_chunk_sizes_mb,
            max_chunk_sizes_mb,
        )

        for (
            procs,
            threads,
            size_mb,
            chunk_size_mb,
            block_size_mb,
            bucket_type,
            seq_prob,
            min_chunk_mb,
            max_chunk_mb,
        ) in param_combinations:
            bucket_name = self.get_bucket_name(bucket_type)
            if not bucket_name:
                continue

            name = f"{scenario['name']}_{procs}procs_{threads}threads_{size_mb}MB_file_"
            if pattern == "mixed":
                name += f"mixed_{seq_prob}seq_{min_chunk_mb}to{max_chunk_mb}MB_chunk_"
            else:
                name += f"{chunk_size_mb}MB_chunk_"
            name += f"{block_size_mb}MB_block_{bucket_type}"

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
                chunk_size_bytes=int(chunk_size_mb * MB) if chunk_size_mb else 0,
                block_size_bytes=int(block_size_mb * MB),
                file_size_bytes=int(size_mb * MB),
                runtime=runtime,
                min_chunk_size_bytes=int(min_chunk_mb * MB) if min_chunk_mb else 0,
                max_chunk_size_bytes=int(max_chunk_mb * MB) if max_chunk_mb else 0,
                seq_probability=seq_prob,
            )
            cases.append(params)
        return cases


def get_read_benchmark_cases():
    return ReadConfigurator(__file__).generate_cases()
