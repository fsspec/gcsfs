from dataclasses import dataclass

from gcsfs.tests.perf.microbenchmarks.parameters import IOBenchmarkParameters


@dataclass
class ComparisonBenchmarkParameters(IOBenchmarkParameters):
    """Parameters defining a single comparison microbenchmark case."""

    # Name of the benchmark scenario from configs.yaml
    scenario: str = ""

    # Engine under test (e.g. gcsfs, gcloud)
    engine: str = ""

    # Operation method under test (e.g. get, put, pipe, cat, cp, etc.)
    method: str = ""
