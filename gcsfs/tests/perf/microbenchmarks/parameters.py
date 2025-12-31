from dataclasses import dataclass


@dataclass
class BaseBenchmarkParameters:
    """
    Base parameters common to all benchmark types.
    """

    # The name of config
    name: str

    # The name of the GCS bucket to use for the benchmark.
    bucket_name: str

    # The type of the bucket, e.g., "regional", "zonal".
    bucket_type: str

    # Number of threads for multi-threaded tests.
    num_threads: int

    # Number of processes for multi-process tests.
    num_processes: int

    # Number of files to create for the benchmark.
    num_files: int

    # Number of rounds for the benchmark.
    rounds: int


@dataclass
class IOBenchmarkParameters(BaseBenchmarkParameters):
    """
    Parameters common to Read and Write benchmarks.
    """

    # Size of each file in bytes
    file_size_bytes: int

    # The size of each operation (read/write) in bytes.
    chunk_size_bytes: int
