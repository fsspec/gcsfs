from dataclasses import dataclass

from gcsfs.tests.perf.microbenchmarks.read.parameters import ReadBenchmarkParameters


@dataclass
class ReadFixedDurationBenchmarkParameters(ReadBenchmarkParameters):
    """
    Defines the parameters for a read benchmark test cases with runtime.
    """

    # Time in seconds the test should run.
    runtime: int
