from dataclasses import dataclass

from gcsfs.tests.perf.microbenchmarks.parameters import IOBenchmarkParameters


@dataclass
class PipeBenchmarkParameters(IOBenchmarkParameters):
    """
    Defines the parameters for a pipe benchmark test case.
    """

    pass
