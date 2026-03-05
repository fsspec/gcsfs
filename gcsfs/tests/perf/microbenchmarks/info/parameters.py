from dataclasses import dataclass

from gcsfs.tests.perf.microbenchmarks.listing.parameters import (
    ListingBenchmarkParameters,
)


@dataclass
class InfoBenchmarkParameters(ListingBenchmarkParameters):
    """
    Parameters for Info benchmarks.
    """

    # The type of target to query: "bucket", "folder", or "file".
    target_type: str
