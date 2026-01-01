import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from gcsfs.tests.perf.microbenchmarks.runner import (
    filter_test_cases,
    run_multi_process,
    run_multi_threaded,
    run_single_threaded,
)
from gcsfs.tests.perf.microbenchmarks.write.configs import get_write_benchmark_cases
from gcsfs.tests.settings import BENCHMARK_SKIP_TESTS

pytestmark = pytest.mark.skipif(
    BENCHMARK_SKIP_TESTS,
    reason="""Skipping benchmark tests.
Set GCSFS_BENCHMARK_SKIP_TESTS=false to run them,
or use the orchestrator script at gcsfs/tests/perf/microbenchmarks/run.py""",
)

BENCHMARK_GROUP = "write"


def _write_op_seq(gcs, path, chunk_size, file_size):
    chunks = file_size // chunk_size
    remainder = file_size % chunk_size
    start_time = time.perf_counter()
    with gcs.open(path, "wb") as f:
        for _ in range(chunks):
            f.write(os.urandom(chunk_size))
        if remainder:
            f.write(os.urandom(remainder))
    duration_ms = (time.perf_counter() - start_time) * 1000
    logging.info(f"SEQ_WRITE : {path} - {duration_ms:.2f} ms.")


all_benchmark_cases = get_write_benchmark_cases()
single_threaded_cases, multi_threaded_cases, multi_process_cases = filter_test_cases(
    all_benchmark_cases
)


@pytest.mark.parametrize(
    "gcsfs_benchmark_write",
    single_threaded_cases,
    indirect=True,
    ids=lambda p: p.name,
)
def test_write_single_threaded(benchmark, gcsfs_benchmark_write, monitor):
    gcs, file_paths, params = gcsfs_benchmark_write

    op_args = (gcs, file_paths[0], params.chunk_size_bytes, params.file_size_bytes)
    run_single_threaded(
        benchmark, monitor, params, _write_op_seq, op_args, BENCHMARK_GROUP
    )


@pytest.mark.parametrize(
    "gcsfs_benchmark_write",
    multi_threaded_cases,
    indirect=True,
    ids=lambda p: p.name,
)
def test_write_multi_threaded(benchmark, gcsfs_benchmark_write, monitor):
    gcs, file_paths, params = gcsfs_benchmark_write

    args_list = [
        (gcs, path, params.chunk_size_bytes, params.file_size_bytes)
        for path in file_paths
    ]

    run_multi_threaded(
        benchmark, monitor, params, _write_op_seq, args_list, BENCHMARK_GROUP
    )


def _process_worker(
    gcs,
    file_paths,
    chunk_size,
    num_threads,
    file_size_bytes,
    process_durations_shared,
    index,
):
    """A worker function for each process to write a list of files."""
    start_time = time.perf_counter()
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [
            executor.submit(_write_op_seq, gcs, path, chunk_size, file_size_bytes)
            for path in file_paths
        ]
        list(futures)
    duration_s = time.perf_counter() - start_time
    process_durations_shared[index] = duration_s


@pytest.mark.parametrize(
    "gcsfs_benchmark_write",
    multi_process_cases,
    indirect=True,
    ids=lambda p: p.name,
)
def test_write_multi_process(
    benchmark, gcsfs_benchmark_write, extended_gcs_factory, request, monitor
):
    _, file_paths, params = gcsfs_benchmark_write
    files_per_process = params.num_files // params.num_processes

    def args_builder(gcs_instance, i, shared_arr):
        start_index = i * files_per_process
        end_index = start_index + files_per_process
        process_files = file_paths[start_index:end_index]
        return (
            gcs_instance,
            process_files,
            params.chunk_size_bytes,
            params.num_threads,
            params.file_size_bytes,
            shared_arr,
            i,
        )

    run_multi_process(
        benchmark,
        monitor,
        params,
        extended_gcs_factory,
        worker_target=_process_worker,
        args_builder=args_builder,
        benchmark_group=BENCHMARK_GROUP,
        request=request,
    )
