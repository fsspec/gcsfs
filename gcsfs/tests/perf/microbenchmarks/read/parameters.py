from dataclasses import dataclass

from gcsfs.tests.perf.microbenchmarks.parameters import IOBenchmarkParameters


@dataclass
class ReadBenchmarkParameters(IOBenchmarkParameters):
    """
    Defines the parameters for a read benchmark test cases with runtime.
    """

    # Read pattern: "seq" for sequential, "rand" for random.
    pattern: str

    # The block size for gcsfs file buffering default to 16MB.
    block_size_bytes: int

    # Time in seconds the test should run.
    runtime: int

    # Size of the MRD pool cache. Default is 16.
    mrd_pool_cache_size: int = 16

    # Size of the MRD pool. Default is 1.
    mrd_pool_size: int = 1
