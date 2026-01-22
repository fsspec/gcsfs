import logging
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from gcsfs.tests.perf.microbenchmarks.listing.configs import get_listing_benchmark_cases
from gcsfs.tests.perf.microbenchmarks.runner import (
    filter_test_cases,
    run_multi_process,
    run_multi_threaded,
    run_single_threaded,
)

BENCHMARK_GROUP = "listing"


def _list_op(gcs, path, pattern="ls"):
    start_time = time.perf_counter()
    if pattern == "find":
        items = gcs.find(path)
    else:
        items = gcs.ls(path)
    duration_ms = (time.perf_counter() - start_time) * 1000
    logging.info(
        f"{pattern.upper()} : {path} - {len(items)} items - {duration_ms:.2f} ms."
    )


def _list_dirs(gcs, paths, pattern="ls"):
    for path in paths:
        _list_op(gcs, path, pattern=pattern)


def _chunk_list(data, n):
    k, m = divmod(len(data), n)
    return [data[i * k + min(i, m) : (i + 1) * k + min(i + 1, m)] for i in range(n)]


all_benchmark_cases = get_listing_benchmark_cases()
single_threaded_cases, multi_threaded_cases, multi_process_cases = filter_test_cases(
    all_benchmark_cases
)


@pytest.mark.parametrize(
    "gcsfs_benchmark_listing",
    single_threaded_cases,
    indirect=True,
    ids=lambda p: p.name,
)
def test_listing_single_threaded(benchmark, gcsfs_benchmark_listing, monitor):
    gcs, target_dirs, prefix, params = gcsfs_benchmark_listing

    if params.pattern == "find":
        # For 'find', we want to measure a single recursive operation from the root.
        run_single_threaded(
            benchmark,
            monitor,
            params,
            _list_op,
            (gcs, prefix, params.pattern),
            BENCHMARK_GROUP,
        )
    else:
        run_single_threaded(
            benchmark,
            monitor,
            params,
            _list_dirs,
            (gcs, target_dirs, params.pattern),
            BENCHMARK_GROUP,
        )


@pytest.mark.parametrize(
    "gcsfs_benchmark_listing",
    multi_threaded_cases,
    indirect=True,
    ids=lambda p: p.name,
)
def test_listing_multi_threaded(benchmark, gcsfs_benchmark_listing, monitor):
    gcs, target_dirs, _, params = gcsfs_benchmark_listing

    chunks = _chunk_list(target_dirs, params.threads)
    args_list = [(gcs, chunks[i], params.pattern) for i in range(params.threads)]

    run_multi_threaded(
        benchmark, monitor, params, _list_dirs, args_list, BENCHMARK_GROUP
    )


def _process_worker(
    gcs, target_dirs, threads, process_durations_shared, index, pattern="ls"
):
    """A worker function for each process to list the directory."""
    start_time = time.perf_counter()
    chunks = _chunk_list(target_dirs, threads)
    with ThreadPoolExecutor(max_workers=threads) as executor:
        futures = [
            executor.submit(_list_dirs, gcs, chunks[i], pattern) for i in range(threads)
        ]
        list(futures)
    duration_s = time.perf_counter() - start_time
    process_durations_shared[index] = duration_s


@pytest.mark.parametrize(
    "gcsfs_benchmark_listing",
    multi_process_cases,
    indirect=True,
    ids=lambda p: p.name,
)
def test_listing_multi_process(
    benchmark, gcsfs_benchmark_listing, extended_gcs_factory, request, monitor
):
    _, target_dirs, _, params = gcsfs_benchmark_listing

    def args_builder(gcs_instance, i, shared_arr):
        chunks = _chunk_list(target_dirs, params.processes)
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
