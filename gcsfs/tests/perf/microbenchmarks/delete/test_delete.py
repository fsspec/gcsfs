import logging
import time

import pytest

from gcsfs.tests.perf.microbenchmarks.delete.configs import get_delete_benchmark_cases
from gcsfs.tests.perf.microbenchmarks.runner import (
    filter_test_cases,
    run_single_threaded,
)

BENCHMARK_GROUP = "delete"


def _delete_op(gcs, path):
    start_time = time.perf_counter()
    gcs.rm(path, recursive=True)
    duration_ms = (time.perf_counter() - start_time) * 1000
    logging.info(f"DELETE : {path} - {duration_ms:.2f} ms.")


all_benchmark_cases = get_delete_benchmark_cases()
single_threaded_cases, _, _ = filter_test_cases(all_benchmark_cases)


@pytest.mark.parametrize(
    "gcsfs_benchmark_listing",
    single_threaded_cases,
    indirect=True,
    ids=lambda p: p.name,
)
def test_delete_recursive(benchmark, gcsfs_benchmark_listing, monitor):
    gcs, _, prefix, params = gcsfs_benchmark_listing

    run_single_threaded(
        benchmark, monitor, params, _delete_op, (gcs, prefix), BENCHMARK_GROUP
    )
