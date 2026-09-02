from dataclasses import dataclass

from gcsfs.tests.perf.microbenchmarks.parameters import IOBenchmarkParameters


@dataclass
class CatRangesBenchmarkParameters(IOBenchmarkParameters):
    """
    Defines the parameters for a cat_ranges benchmark test case.
    """

    # Number of byte ranges to fetch in a single cat_ranges call
    num_ranges: int

    # Range selection pattern: "seq", "rand", or "mixed"
    pattern: str = "rand"

    # List of chunk sizes (in bytes) used across ranges within a single test case
    chunk_sizes_bytes: list[int] | None = None

    # Maximum gap in bytes for merging adjacent ranges in cat_ranges (defaults to None)
    max_gap: int | None = None

    # Batch size for async concurrent range execution in cat_ranges (defaults to None)
    batch_size: int | None = None
