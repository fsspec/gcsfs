from dataclasses import dataclass

from gcsfs.tests.perf.microbenchmarks.parameters import BaseBenchmarkParameters


@dataclass
class OpenBenchmarkParameters(BaseBenchmarkParameters):
    """
    Parameters for Open benchmarks.
    """

    folders: int
