from dataclasses import dataclass

from gcsfs.tests.perf.microbenchmarks.conftest import MB


@dataclass
class WriteBenchmarkParameters:
    """
    Defines the parameters for a write benchmark test cases.
    """

    # The name of config
    name: str

    # The name of the GCS bucket to use for the benchmark.
    bucket_name: str = ""

    # The type of the bucket, e.g., "regional", "zonal".
    bucket_type: str = ""

    # Number of threads for multi-threaded tests, default to 1.
    num_threads: int = 1

    # Number of processes for multi-process tests, default to 1.
    num_processes: int = 1

    # Number of files to create for the benchmark.
    num_files: int = 1

    # Number of rounds for the benchmark, default to 10.
    rounds: int = 10

    # The size of each write operation in bytes.
    chunk_size_bytes: int = 16 * MB

    # Size of each file in bytes
    file_size_bytes: int = 128 * MB
