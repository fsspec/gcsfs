import logging
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from gcsfs.tests.perf.microbenchmarks.open.configs import get_open_benchmark_cases
from gcsfs.tests.perf.microbenchmarks.runner import (
    filter_test_cases,
    run_multi_process,
    run_multi_threaded,
    run_single_threaded,
)

BENCHMARK_GROUP = "open"


def _open_op(gcs, path):
    start_time = time.perf_counter()
    try:
        f = gcs.open(path, mode="rb")
        f.close()
    except FileNotFoundError:
        pass
    duration_ms = (time.perf_counter() - start_time) * 1000
    logging.debug(f"OPEN : {path} - {duration_ms:.2f} ms.")


def _open_ops(gcs, paths):
    for path in paths:
        _open_op(gcs, path)


all_benchmark_cases = get_open_benchmark_cases()
single_threaded_cases, multi_threaded_cases, multi_process_cases = filter_test_cases(
    all_benchmark_cases
)


@pytest.mark.parametrize(
    "gcsfs_benchmark_open",
    single_threaded_cases,
    indirect=True,
    ids=lambda p: p.name,
)
def test_open_single_threaded(benchmark, gcsfs_benchmark_open, monitor):
    gcs, target_dirs, file_paths, prefix, params = gcsfs_benchmark_open

    run_single_threaded(
        benchmark, monitor, params, _open_ops, (gcs, file_paths), BENCHMARK_GROUP
    )


def _chunk_list(data, n):
    k, m = divmod(len(data), n)
    return [data[i * k + min(i, m) : (i + 1) * k + min(i + 1, m)] for i in range(n)]


@pytest.mark.parametrize(
    "gcsfs_benchmark_open",
    multi_threaded_cases,
    indirect=True,
    ids=lambda p: p.name,
)
def test_open_multi_threaded(benchmark, gcsfs_benchmark_open, monitor):
    gcs, target_dirs, file_paths, prefix, params = gcsfs_benchmark_open

    chunks = _chunk_list(file_paths, params.threads)
    args_list = [(gcs, chunks[i]) for i in range(params.threads)]

    run_multi_threaded(
        benchmark, monitor, params, _open_ops, args_list, BENCHMARK_GROUP
    )


def _process_worker(gcs, paths, threads, process_durations_shared, index):
    """A worker function for each process to run open operations."""
    start_time = time.perf_counter()
    chunks = _chunk_list(paths, threads)
    with ThreadPoolExecutor(max_workers=threads) as executor:
        futures = [executor.submit(_open_ops, gcs, chunks[i]) for i in range(threads)]
        [f.result() for f in futures]
    duration_s = time.perf_counter() - start_time
    process_durations_shared[index] = duration_s


@pytest.mark.parametrize(
    "gcsfs_benchmark_open",
    multi_process_cases,
    indirect=True,
    ids=lambda p: p.name,
)
def test_open_multi_process(
    benchmark, gcsfs_benchmark_open, extended_gcs_factory, request, monitor
):
    gcs, target_dirs, file_paths, prefix, params = gcsfs_benchmark_open

    chunks = _chunk_list(file_paths, params.processes)

    def args_builder(gcs_instance, i, shared_arr):
        return (
            gcs_instance,
            chunks[i],
            params.threads,
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
