from dataclasses import dataclass

from gcsfs.tests.perf.microbenchmarks.write.parameters import WriteBenchmarkParameters


@dataclass
class WriteFixedDurationBenchmarkParameters(WriteBenchmarkParameters):
    """
    Defines the parameters for a write benchmark test cases with runtime.
    """

    # Time in seconds the test should run.
    runtime: int
