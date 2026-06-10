from gcsfs.tests.perf.microbenchmarks.glob.parameters import GlobBenchmarkParameters
from gcsfs.tests.perf.microbenchmarks.listing.configs import ListingConfigurator


class GlobConfigurator(ListingConfigurator):
    param_class = GlobBenchmarkParameters

    def _create_params(
        self,
        name,
        bucket_name,
        bucket_type,
        threads,
        processes,
        files,
        rounds,
        depth,
        folders,
        pattern,
        extra_values,
    ):
        return self.param_class(
            name=name,
            bucket_name=bucket_name,
            bucket_type=bucket_type,
            threads=threads,
            processes=processes,
            files=files,
            rounds=rounds,
            depth=depth,
            folders=folders,
            pattern=pattern,
        )


def get_glob_benchmark_cases():
    return GlobConfigurator(__file__).generate_cases()
