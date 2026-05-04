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

    # Min I/O chunk size
    min_chunk_size_bytes: int

    # Max I/O chunk size
    max_chunk_size_bytes: int

    # The sequential probability, the seek probability would be 1 - seq_probability
    seq_probability: float
