import logging
import time

import pytest

from gcsfs.tests.perf.microbenchmarks.rename.configs import get_rename_benchmark_cases
from gcsfs.tests.perf.microbenchmarks.runner import (
    filter_test_cases,
    run_single_threaded,
)

BENCHMARK_GROUP = "rename"


def _rename_op(gcs, src, dst):
    start_time = time.perf_counter()
    gcs.rename(src, dst, recursive=True)
    duration_ms = (time.perf_counter() - start_time) * 1000
    logging.info(f"RENAME : {src} -> {dst} - {duration_ms:.2f} ms.")


all_benchmark_cases = get_rename_benchmark_cases()
single_threaded_cases, _, _ = filter_test_cases(all_benchmark_cases)


@pytest.mark.parametrize(
    "gcsfs_benchmark_listing",
    single_threaded_cases,
    indirect=True,
    ids=lambda p: p.name,
)
def test_rename_recursive(benchmark, gcsfs_benchmark_listing, monitor):
    gcs, _, prefix, params = gcsfs_benchmark_listing
    prefix_renamed = f"{prefix}_renamed"

    try:
        run_single_threaded(
            benchmark,
            monitor,
            params,
            _rename_op,
            (gcs, prefix, prefix_renamed),
            BENCHMARK_GROUP,
        )
    finally:
        try:
            gcs.rm(prefix_renamed, recursive=True)
        except Exception:
            pass
