from dataclasses import dataclass

from gcsfs.tests.perf.microbenchmarks.parameters import BaseBenchmarkParameters


@dataclass
class ListingBenchmarkParameters(BaseBenchmarkParameters):
    """
    Defines the parameters for a listing benchmark test cases.
    """

    # The nested depth of object structure.
    depth: int

    # The number of folders to create.
    folders: int

    # The listing pattern to use: "ls" or "find".
    pattern: str = "ls"
