import logging
import os

import yaml

from gcsfs.tests.conftest import BUCKET_NAME_MAP
from gcsfs.tests.settings import BENCHMARK_FILTER


class BaseBenchmarkConfigurator:
    def __init__(self, module_file):
        self.config_path = os.path.join(os.path.dirname(module_file), "configs.yaml")

    def _load_config(self):
        with open(self.config_path, "r") as f:
            config = yaml.safe_load(f)

        common = config["common"]
        scenarios = config["scenarios"]

        if BENCHMARK_FILTER:
            logging.info(
                f"Filtering the scenarios based on BENCHMARK_FILTER: {BENCHMARK_FILTER}"
            )
            filter_names = [
                name.strip().lower() for name in BENCHMARK_FILTER.split(",")
            ]
            scenarios = [s for s in scenarios if s["name"].lower() in filter_names]

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
