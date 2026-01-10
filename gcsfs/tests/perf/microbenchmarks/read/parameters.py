from dataclasses import dataclass

from gcsfs.tests.perf.microbenchmarks.parameters import IOBenchmarkParameters


@dataclass
class ReadBenchmarkParameters(IOBenchmarkParameters):
    """
    Defines the parameters for a read benchmark test cases.
    """

    # Read pattern: "seq" for sequential, "rand" for random.
    pattern: str

    # The block size for gcsfs file buffering default to 16MB.
    block_size_bytes: int
