from gcsfs.tests.perf.microbenchmarks.delete.parameters import DeleteBenchmarkParameters
from gcsfs.tests.perf.microbenchmarks.listing.configs import ListingConfigurator


class DeleteConfigurator(ListingConfigurator):
    param_class = DeleteBenchmarkParameters


def get_delete_benchmark_cases():
    return DeleteConfigurator(__file__).generate_cases()
