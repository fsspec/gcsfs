import itertools
from typing import List

from gcsfs.tests.perf.microbenchmarks.comparison.parameters import (
    ComparisonBenchmarkParameters,
)
from gcsfs.tests.perf.microbenchmarks.configs import BaseBenchmarkConfigurator
from gcsfs.tests.perf.microbenchmarks.conftest import MB


class ComparisonConfigurator(BaseBenchmarkConfigurator):
    """Loads and generates comparison benchmark test cases from configs.yaml."""

    def build_cases(
        self, scenario: dict, common_config: dict
    ) -> List[ComparisonBenchmarkParameters]:
        scenario_name = scenario["name"]
        bucket_types = scenario.get(
            "bucket_types", common_config.get("bucket_types", ["zonal"])
        )
        file_sizes_mb = scenario.get(
            "file_sizes_mb", common_config.get("file_sizes_mb", [1024])
        )
        chunk_sizes_mb = scenario.get(
            "chunk_sizes_mb", common_config.get("chunk_sizes_mb", [50])
        )
        threads_list = scenario.get("threads", common_config.get("threads", [4]))
        processes_list = scenario.get("processes", common_config.get("processes", [1]))
        files_list = scenario.get("files", [1])
        rounds = scenario.get("rounds", common_config.get("rounds", 1))

        cases = []
        param_combinations = itertools.product(
            bucket_types,
            file_sizes_mb,
            chunk_sizes_mb,
            threads_list,
            processes_list,
            files_list,
        )

        for (
            b_type,
            size_mb,
            chunk_mb,
            threads,
            procs,
            files_count,
        ) in param_combinations:
            bucket_name = self.get_bucket_name(b_type)
            if not bucket_name:
                continue
            name = f"{scenario_name}_{b_type}_{size_mb}MB_{threads}threads"
            if procs > 1:
                name += f"_{procs}procs"
            if files_count > 1:
                name += f"_{files_count}files"
            if chunk_mb != 50:
                name += f"_{chunk_mb}MBchunk"

            params = ComparisonBenchmarkParameters(
                name=name,
                scenario=scenario_name,
                bucket_name=bucket_name,
                bucket_type=b_type,
                file_size_bytes=int(size_mb * MB),
                chunk_size_bytes=int(chunk_mb * MB),
                files=files_count,
                threads=threads,
                processes=procs,
                rounds=rounds,
            )
            cases.append(params)
        return cases


def get_comparison_benchmark_cases() -> List[ComparisonBenchmarkParameters]:
    return ComparisonConfigurator(__file__).generate_cases()
