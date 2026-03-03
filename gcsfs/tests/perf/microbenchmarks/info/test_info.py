import logging
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from gcsfs.tests.perf.microbenchmarks.info.configs import get_info_benchmark_cases
from gcsfs.tests.perf.microbenchmarks.runner import (
    filter_test_cases,
    run_multi_process,
    run_multi_threaded,
    run_single_threaded,
)

BENCHMARK_GROUP = "info"


def _info_op(gcs, path, pattern="info"):
    start_time = time.perf_counter()
    try:
        if pattern == "info":
            gcs.info(path)
        else:
            raise ValueError(f"Unsupported pattern: {pattern}")
    except FileNotFoundError:
        pass
    duration_ms = (time.perf_counter() - start_time) * 1000
    logging.info(f"{pattern.upper()} : {path} - {duration_ms:.2f} ms.")


def _info_ops(gcs, paths, pattern="info"):
    for path in paths:
        _info_op(gcs, path, pattern)


all_benchmark_cases = get_info_benchmark_cases()
single_threaded_cases, multi_threaded_cases, multi_process_cases = filter_test_cases(
    all_benchmark_cases
)


@pytest.mark.parametrize(
    "gcsfs_benchmark_info",
    single_threaded_cases,
    indirect=True,
    ids=lambda p: p.name,
)
def test_info_single_threaded(benchmark, gcsfs_benchmark_info, monitor):
    gcs, target_dirs, file_paths, prefix, params = gcsfs_benchmark_info

    paths = _get_target_paths(target_dirs, file_paths, params)

    run_single_threaded(
        benchmark,
        monitor,
        params,
        _info_ops,
        (gcs, paths, params.pattern),
        BENCHMARK_GROUP,
    )


@pytest.mark.parametrize(
    "gcsfs_benchmark_info",
    multi_threaded_cases,
    indirect=True,
    ids=lambda p: p.name,
)
def test_info_multi_threaded(benchmark, gcsfs_benchmark_info, monitor):
    gcs, target_dirs, file_paths, prefix, params = gcsfs_benchmark_info

    paths = _get_target_paths(target_dirs, file_paths, params)

    run_multi_threaded(
        benchmark,
        monitor,
        params,
        _info_ops,
        (gcs, paths, params.pattern),
        BENCHMARK_GROUP,
    )


def _get_target_paths(target_dirs, file_paths, params):
    if params.target_type == "bucket":
        return [params.bucket_name]
    elif params.target_type == "folder":
        return target_dirs
    elif params.target_type == "file":
        return file_paths
    else:
        raise ValueError(f"Unsupported target type: {params.target_type}")


def _chunk_list(data, n):
    k, m = divmod(len(data), n)
    return [data[i * k + min(i, m) : (i + 1) * k + min(i + 1, m)] for i in range(n)]


def _process_worker(
    gcs, paths, threads, process_durations_shared, index, pattern="info"
):
    """A worker function for each process to run info operations."""
    start_time = time.perf_counter()
    chunks = _chunk_list(paths, threads)
    with ThreadPoolExecutor(max_workers=threads) as executor:
        futures = [
            executor.submit(_info_ops, gcs, chunks[i], pattern) for i in range(threads)
        ]
        [f.result() for f in futures]
    duration_s = time.perf_counter() - start_time
    process_durations_shared[index] = duration_s


@pytest.mark.parametrize(
    "gcsfs_benchmark_info",
    multi_process_cases,
    indirect=True,
    ids=lambda p: p.name,
)
def test_info_multi_process(
    benchmark, gcsfs_benchmark_info, extended_gcs_factory, request, monitor
):
    gcs, target_dirs, file_paths, prefix, params = gcsfs_benchmark_info

    chunks = _chunk_list(
        _get_target_paths(target_dirs, file_paths, params), params.processes
    )

    def args_builder(gcs_instance, i, shared_arr):
        return (
            gcs_instance,
            chunks[i],
            params.threads,
            shared_arr,
            i,
            params.pattern,
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
