from dataclasses import dataclass

from gcsfs.tests.perf.microbenchmarks.listing.parameters import (
    ListingBenchmarkParameters,
)


@dataclass
class RenameBenchmarkParameters(ListingBenchmarkParameters):
    """
    Defines the parameters for rename benchmark test cases.
    """
