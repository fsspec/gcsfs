from dataclasses import dataclass

from gcsfs.tests.perf.microbenchmarks.parameters import BaseBenchmarkParameters


@dataclass
class GlobBenchmarkParameters(BaseBenchmarkParameters):
    """
    Defines the parameters for a glob benchmark test cases.
    """

    # The nested depth of object structure.
    depth: int

    # The number of folders to create.
    folders: int

    # The globbing pattern to use.
    pattern: str
