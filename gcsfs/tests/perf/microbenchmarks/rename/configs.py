from gcsfs.tests.perf.microbenchmarks.listing.configs import ListingConfigurator
from gcsfs.tests.perf.microbenchmarks.rename.parameters import RenameBenchmarkParameters


class RenameConfigurator(ListingConfigurator):
    param_class = RenameBenchmarkParameters


def get_rename_benchmark_cases():
    return RenameConfigurator(__file__).generate_cases()
