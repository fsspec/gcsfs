import itertools

from gcsfs.tests.perf.microbenchmarks.configs import BaseBenchmarkConfigurator
from gcsfs.tests.perf.microbenchmarks.conftest import MB

from .parameters import CatRangesBenchmarkParameters


class CatRangesConfigurator(BaseBenchmarkConfigurator):
    def build_cases(self, scenario, common_config):
        rounds = common_config.get("rounds", 1)
        bucket_types = scenario.get(
            "bucket_types", common_config.get("bucket_types", ["regional"])
        )
        file_sizes_mb = scenario.get(
            "file_sizes_mb", common_config.get("file_sizes_mb", [256])
        )
        num_ranges_list = scenario.get(
            "num_ranges_list", common_config.get("num_ranges_list", [10])
        )
        pattern = scenario.get("pattern", "rand")
        scenario_files = scenario.get("files", 1)

        raw_chunk_sizes = scenario.get(
            "chunk_sizes_mb", common_config.get("chunk_sizes_mb", [[1]])
        )
        if not raw_chunk_sizes:
            raw_chunk_sizes = [[1]]

        if not isinstance(raw_chunk_sizes[0], list):
            chunk_size_combinations = [raw_chunk_sizes]
        else:
            chunk_size_combinations = raw_chunk_sizes

        batch_sizes = scenario.get(
            "batch_sizes", common_config.get("batch_sizes", [None])
        ) or [None]
        max_gaps = scenario.get("max_gaps", common_config.get("max_gaps", [None])) or [
            None
        ]

        cases = []
        param_combinations = itertools.product(
            file_sizes_mb,
            chunk_size_combinations,
            num_ranges_list,
            batch_sizes,
            max_gaps,
            bucket_types,
        )

        for (
            size_mb,
            chunk_sizes_mb,
            num_ranges,
            batch_size,
            max_gap,
            bucket_type,
        ) in param_combinations:
            bucket_name = self.get_bucket_name(bucket_type)
            if not bucket_name:
                continue

            chunk_sizes_bytes = [int(s * MB) for s in chunk_sizes_mb]
            if len(chunk_sizes_mb) == 1:
                size_label = f"{chunk_sizes_mb[0]}MB_chunk"
                avg_chunk_size_bytes = chunk_sizes_bytes[0]
            else:
                formatted_sizes = "_".join(str(s) for s in chunk_sizes_mb)
                size_label = f"mixed_{formatted_sizes}MB_chunk"
                avg_chunk_size_bytes = int(
                    sum(chunk_sizes_bytes) / len(chunk_sizes_bytes)
                )

            batch_label = f"{batch_size}batch_" if batch_size is not None else ""
            maxgap_label = f"{max_gap}maxgap_" if max_gap is not None else ""

            name = (
                f"{scenario['name']}_{size_mb}MB_file_{scenario_files}files_"
                f"{size_label}_{num_ranges}ranges_{batch_label}{maxgap_label}"
                f"{pattern}_{bucket_type}"
            )

            params = CatRangesBenchmarkParameters(
                name=name,
                bucket_name=bucket_name,
                bucket_type=bucket_type,
                threads=1,
                processes=1,
                files=scenario_files,
                rounds=rounds,
                file_size_bytes=int(size_mb * MB),
                chunk_size_bytes=avg_chunk_size_bytes,
                chunk_sizes_bytes=chunk_sizes_bytes,
                num_ranges=num_ranges,
                max_gap=max_gap,
                batch_size=batch_size,
                pattern=pattern,
            )
            cases.append(params)
        return cases


def get_cat_ranges_benchmark_cases():
    return CatRangesConfigurator(__file__).generate_cases()
