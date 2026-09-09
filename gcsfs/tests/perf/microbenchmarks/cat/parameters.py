from dataclasses import dataclass
from typing import Optional

from gcsfs.tests.perf.microbenchmarks.parameters import IOBenchmarkParameters


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
