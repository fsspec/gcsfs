from dataclasses import dataclass

from gcsfs.tests.perf.microbenchmarks.parameters import IOBenchmarkParameters


@dataclass
class WriteFixedDurationBenchmarkParameters(IOBenchmarkParameters):
    """
    Defines the parameters for a write benchmark test cases with runtime.
    """

    # Time in seconds the test should run.
    runtime: int
