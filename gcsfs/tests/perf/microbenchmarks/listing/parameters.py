from dataclasses import dataclass

from gcsfs.tests.perf.microbenchmarks.parameters import BaseBenchmarkParameters


@dataclass
class ListingBenchmarkParameters(BaseBenchmarkParameters):
    """
    Defines the parameters for a listing benchmark test cases.
    """

    # The nested depth of object structure.
    depth: int
