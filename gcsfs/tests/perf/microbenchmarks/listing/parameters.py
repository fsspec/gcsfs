from dataclasses import dataclass


@dataclass
class ListingBenchmarkParameters:
    """
    Defines the parameters for a listing benchmark test cases.
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
    num_files: int = 1000

    # Number of rounds for the benchmark, default to 10.
    rounds: int = 1

    # The nested depth of object structure.
    depth: int = 0
