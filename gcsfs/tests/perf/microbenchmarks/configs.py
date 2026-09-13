import logging
import os

import yaml

from gcsfs.tests.conftest import BUCKET_NAME_MAP
from gcsfs.tests.settings import BENCHMARK_CHUNK_SIZES_MB, BENCHMARK_FILTER


def _parse_chunk_sizes(raw):
    """Parse a comma-separated MB list, keeping whole numbers as ints.

    Benchmark names embed this value, so 1 must stay "1MB_chunk" rather than
    becoming "1.0MB_chunk" and breaking comparison against runs that did not
    use the override.
    """
    sizes = []
    for token in raw.split(","):
        token = token.strip()
        if not token:
            continue
        value = float(token)
        sizes.append(int(value) if value.is_integer() else value)
    return sizes


class BaseBenchmarkConfigurator:
    def __init__(self, module_file):
        self.config_path = os.path.join(os.path.dirname(module_file), "configs.yaml")

    def _load_config(self):
        with open(self.config_path, "r") as f:
            config = yaml.safe_load(f)

        common = config["common"]
        scenarios = config["scenarios"]

        if BENCHMARK_FILTER:
            filter_names = [
                name.strip().lower() for name in BENCHMARK_FILTER.split(",")
            ]
            scenarios = [s for s in scenarios if s["name"].lower() in filter_names]

        if BENCHMARK_CHUNK_SIZES_MB:
            chunk_sizes = _parse_chunk_sizes(BENCHMARK_CHUNK_SIZES_MB)
            if chunk_sizes:
                logging.info(f"Overriding chunk_sizes_mb with {chunk_sizes}.")
                common = {**common, "chunk_sizes_mb": chunk_sizes}
                # Scenario-level values take precedence over common, so they have to be
                # replaced too for the override to actually apply everywhere.
                scenarios = [
                    {**s, "chunk_sizes_mb": chunk_sizes} if "chunk_sizes_mb" in s else s
                    for s in scenarios
                ]

        return common, scenarios

    def get_bucket_name(self, bucket_type):
        return BUCKET_NAME_MAP.get(bucket_type)

    def generate_cases(self):
        common_config, scenarios = self._load_config()
        all_cases = []

        for scenario in scenarios:
            cases = self.build_cases(scenario, common_config)
            all_cases.extend(cases)

        if all_cases:
            logging.info(
                f"Benchmark cases to be triggered: {', '.join([case.name for case in all_cases])}"
            )
        return all_cases

    def build_cases(self, scenario, common_config):
        """
        Abstract method to be implemented by subclasses.
        Should return a list of BenchmarkParameters objects.
        """
        raise NotImplementedError
