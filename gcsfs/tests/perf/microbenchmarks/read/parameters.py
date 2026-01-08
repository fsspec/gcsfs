from dataclasses import dataclass

from gcsfs.tests.perf.microbenchmarks.conftest import MB


@dataclass
class ReadBenchmarkParameters:
    """
    Defines the parameters for a read benchmark test cases.
    """

    # The name of config
    name: str

    # Read pattern: "seq" for sequential, "rand" for random.
    pattern: str

    # The name of the GCS bucket to use for the benchmark.
    bucket_name: str = ""

    # The type of the bucket, e.g., "regional", "zonal", "hns".
    bucket_type: str = ""

    # Number of threads for multi-threaded tests, default to 1.
    num_threads: int = 1

    # Number of processes for multi-process tests, default to 1.
    num_processes: int = 1

    # Number of files to create for the benchmark.
    num_files: int = 1

    # Number of rounds for the benchmark, default to 10.
    rounds: int = 10

    # The block size for gcsfs file buffering default to 16MB.
    block_size_bytes: int = 16 * MB

    # The size of each read or write operation in bytes default to 16MB.
    chunk_size_bytes: int = 16 * MB

    # Size of each file in bytes
    file_size_bytes: int = 128 * MB
