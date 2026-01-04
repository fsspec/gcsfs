from dataclasses import dataclass

from gcsfs.tests.perf.microbenchmarks.listing.parameters import (
    ListingBenchmarkParameters,
)


@dataclass
class DeleteBenchmarkParameters(ListingBenchmarkParameters):
    """
    Defines the parameters for delete benchmark test cases.
    """
