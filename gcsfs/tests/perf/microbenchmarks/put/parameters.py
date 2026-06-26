from dataclasses import dataclass

from gcsfs.tests.perf.microbenchmarks.parameters import IOBenchmarkParameters


@dataclass
class PutBenchmarkParameters(IOBenchmarkParameters):
    """
    Defines the parameters for a put benchmark test case.

    A put benchmark uploads a local file from disk to GCS, so the relevant
    knobs (``file_size_bytes`` for the local source size and
    ``chunk_size_bytes`` for the resumable upload chunk size) are already
    provided by ``IOBenchmarkParameters``.
    """

    pass
