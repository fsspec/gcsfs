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


def _rename_files_sequential_op(gcs, file_paths):
    start_time = time.perf_counter()
    for src in file_paths:
        dst = src + "_renamed"
        gcs.rename(src, dst)
    duration_ms = (time.perf_counter() - start_time) * 1000
    num_files = len(file_paths)
    logging.info(f"RENAME FILES SEQ ({num_files} files) - {duration_ms:.2f} ms.")


all_benchmark_cases = get_rename_benchmark_cases()
single_threaded_cases, _, _ = filter_test_cases(all_benchmark_cases)
single_threaded_directory_cases = [
    c for c in single_threaded_cases if "rename_files" not in c.name
]
single_threaded_file_cases = [
    c for c in single_threaded_cases if "rename_files" in c.name
]


@pytest.mark.parametrize(
    "gcsfs_benchmark_rename",
    single_threaded_directory_cases,
    indirect=True,
    ids=lambda p: p.name,
)
def test_rename_recursive(benchmark, gcsfs_benchmark_rename, monitor):
    gcs, _, _, prefix, params = gcsfs_benchmark_rename
    prefix_renamed = f"{prefix}_renamed"

    run_single_threaded(
        benchmark,
        monitor,
        params,
        _rename_op,
        (gcs, prefix, prefix_renamed),
        BENCHMARK_GROUP,
    )


@pytest.mark.parametrize(
    "gcsfs_benchmark_rename",
    single_threaded_file_cases,
    indirect=True,
    ids=lambda p: p.name,
)
def test_rename_files_sequential(benchmark, gcsfs_benchmark_rename, monitor):
    gcs, _, file_paths, prefix, params = gcsfs_benchmark_rename

    run_single_threaded(
        benchmark,
        monitor,
        params,
        _rename_files_sequential_op,
        (gcs, file_paths),
        BENCHMARK_GROUP,
    )
