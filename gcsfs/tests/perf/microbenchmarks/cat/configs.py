import itertools
from typing import List

from gcsfs.tests.perf.microbenchmarks.cat.parameters import (
    SUPPORTED_PATTERNS,
    CatBenchmarkParameters,
)
from gcsfs.tests.perf.microbenchmarks.configs import BaseBenchmarkConfigurator


class CatConfigurator(BaseBenchmarkConfigurator):
    """Loads and generates cat benchmark test cases from configs.yaml.

    Sizes are configured in bytes rather than the MB used elsewhere: the
    objects this group cares about are as small as a few bytes, and the point
    of the group is lost if they are rounded into megabytes.
    """

    def build_cases(
        self, scenario: dict, common_config: dict
    ) -> List[CatBenchmarkParameters]:
        scenario_name = scenario["name"]
        bucket_types = scenario.get(
            "bucket_types", common_config.get("bucket_types", ["regional"])
        )
        file_sizes_bytes = scenario.get(
            "file_sizes_bytes", common_config.get("file_sizes_bytes", [8])
        )
        concurrencies = scenario.get(
            "concurrency", common_config.get("concurrency", [None])
        )
        threads_list = scenario.get("threads", common_config.get("threads", [1]))
        files = scenario.get("files", common_config.get("files", 20))
        rounds = scenario.get("rounds", common_config.get("rounds", 5))
        pattern = scenario.get("pattern", "whole")
        # Fail while building cases rather than mid-run: by the time a test
        # body sees a bad pattern the fixture has already uploaded its objects
        # to a real bucket, and the error repeats once per case.
        if pattern not in SUPPORTED_PATTERNS:
            raise ValueError(
                f"Unsupported cat pattern {pattern!r} in scenario "
                f"{scenario_name!r}; expected one of {list(SUPPORTED_PATTERNS)}"
            )

        cases = []
        param_combinations = itertools.product(
            bucket_types,
            file_sizes_bytes,
            concurrencies,
            threads_list,
        )

        for b_type, size_bytes, concurrency, threads in param_combinations:
            bucket_name = self.get_bucket_name(b_type)
            if not bucket_name:
                continue

            conc_tag = "default" if concurrency is None else str(concurrency)
            name = (
                f"{scenario_name}_{b_type}_{size_bytes}B_"
                f"{conc_tag}conc_{threads}threads_{files}files"
            )

            cases.append(
                CatBenchmarkParameters(
                    name=name,
                    bucket_name=bucket_name,
                    bucket_type=b_type,
                    file_size_bytes=int(size_bytes),
                    # cat reads whole objects, so there is no separate chunk size.
                    chunk_size_bytes=int(size_bytes),
                    files=files,
                    threads=threads,
                    processes=1,
                    rounds=rounds,
                    pattern=pattern,
                    concurrency=concurrency,
                )
            )
        return cases


def get_cat_benchmark_cases() -> List[CatBenchmarkParameters]:
    return CatConfigurator(__file__).generate_cases()
