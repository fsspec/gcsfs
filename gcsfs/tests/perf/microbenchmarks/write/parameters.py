from dataclasses import dataclass

from gcsfs.tests.perf.microbenchmarks.parameters import IOBenchmarkParameters


@dataclass
class WriteBenchmarkParameters(IOBenchmarkParameters):
    """
    Defines the parameters for a write benchmark test cases.
    """

    pass
