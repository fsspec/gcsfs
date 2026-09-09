from dataclasses import dataclass
from typing import Optional

from gcsfs.tests.perf.microbenchmarks.parameters import IOBenchmarkParameters

# The read patterns a case may use. configs.py rejects anything else while
# building cases, and test_cat.py maps each one to the operation it runs.
SUPPORTED_PATTERNS = ("whole", "ranged", "batch")


@dataclass
class CatBenchmarkParameters(IOBenchmarkParameters):
    """Parameters defining a single cat microbenchmark case."""

    # How the objects are read:
    #   "whole"  - cat_file(path), no range, so gcsfs has to work out the size
    #   "ranged" - cat_file(path, start=..., end=...), size already known
    #   "batch"  - cat(paths), the whole set in one call
    pattern: str = "whole"

    # Value passed to cat_file(concurrency=...). None leaves the gcsfs default,
    # which is what production code gets.
    concurrency: Optional[int] = None
