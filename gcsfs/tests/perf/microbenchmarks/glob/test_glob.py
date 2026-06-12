import logging
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from gcsfs.tests.perf.microbenchmarks.glob.configs import get_glob_benchmark_cases
from gcsfs.tests.perf.microbenchmarks.runner import (
    filter_test_cases,
    run_multi_process,
    run_multi_threaded,
    run_single_threaded,
)

BENCHMARK_GROUP = "glob"


def _glob_op(gcs, path, pattern="/*"):
    start_time = time.perf_counter()
    glob_path = f"{path}{pattern}"
    items = gcs.glob(glob_path)
    duration_ms = (time.perf_counter() - start_time) * 1000
    logging.info(
        f"GLOB {pattern} : {glob_path} - {len(items)} items - {duration_ms:.2f} ms."
    )


def _glob_dirs(gcs, paths, pattern="/*"):
    for path in paths:
        _glob_op(gcs, path, pattern=pattern)


def _chunk_list(data, n):
    k, m = divmod(len(data), n)
    return [data[i * k + min(i, m) : (i + 1) * k + min(i + 1, m)] for i in range(n)]


all_benchmark_cases = get_glob_benchmark_cases()
single_threaded_cases, multi_threaded_cases, multi_process_cases = filter_test_cases(
    all_benchmark_cases
)


@pytest.mark.parametrize(
    "gcsfs_benchmark_glob",
    single_threaded_cases,
    indirect=True,
    ids=lambda p: p.name,
)
def test_glob_single_threaded(benchmark, gcsfs_benchmark_glob, monitor):
    gcs, target_dirs, prefix, params = gcsfs_benchmark_glob

    # Some patterns like /** might make more sense to run from prefix,
    # but the task states mapping natively to fsspec on exact literal, wildcards etc.
    # The fixture yields target_dirs, which are the created folders.
    # For now we'll run glob on each target dir.
    if params.pattern == "/**":
        run_single_threaded(
            benchmark,
            monitor,
            params,
            _glob_op,
            (gcs, prefix, params.pattern),
            BENCHMARK_GROUP,
        )
    else:
        run_single_threaded(
            benchmark,
            monitor,
            params,
            _glob_dirs,
            (gcs, target_dirs, params.pattern),
            BENCHMARK_GROUP,
        )


@pytest.mark.parametrize(
    "gcsfs_benchmark_glob",
    multi_threaded_cases,
    indirect=True,
    ids=lambda p: p.name,
)
def test_glob_multi_threaded(benchmark, gcsfs_benchmark_glob, monitor):
    gcs, target_dirs, prefix, params = gcsfs_benchmark_glob

    if params.pattern == "/**":
        run_multi_threaded(
            benchmark,
            monitor,
            params,
            _glob_op,
            [(gcs, prefix, params.pattern)],
            BENCHMARK_GROUP,
        )
    else:
        chunks = _chunk_list(target_dirs, params.threads)
        args_list = [(gcs, chunks[i], params.pattern) for i in range(params.threads)]

        run_multi_threaded(
            benchmark, monitor, params, _glob_dirs, args_list, BENCHMARK_GROUP
        )


def _process_worker(
    gcs,
    target_dirs,
    threads,
    process_durations_shared,
    index,
    pattern="/*",
    prefix=None,
):
    """A worker function for each process to glob the directory."""
    start_time = time.perf_counter()
    if pattern == "/**" and prefix is not None:
        with ThreadPoolExecutor(max_workers=threads) as executor:
            futures = [executor.submit(_glob_op, gcs, prefix, pattern)]
            [f.result() for f in futures]
    else:
        chunks = _chunk_list(target_dirs, threads)
        with ThreadPoolExecutor(max_workers=threads) as executor:
            futures = [
                executor.submit(_glob_dirs, gcs, chunks[i], pattern)
                for i in range(threads)
            ]
            [f.result() for f in futures]
    duration_s = time.perf_counter() - start_time
    process_durations_shared[index] = duration_s


@pytest.mark.parametrize(
    "gcsfs_benchmark_glob",
    multi_process_cases,
    indirect=True,
    ids=lambda p: p.name,
)
def test_glob_multi_process(
    benchmark, gcsfs_benchmark_glob, extended_gcs_factory, request, monitor
):
    _, target_dirs, prefix, params = gcsfs_benchmark_glob

    def args_builder(gcs_instance, i, shared_arr):
        if params.pattern == "/**":
            return (
                gcs_instance,
                [],
                params.threads,
                shared_arr,
                i,
                params.pattern,
                prefix,
            )
        else:
            chunks = _chunk_list(target_dirs, params.processes)
            return (
                gcs_instance,
                chunks[i],
                params.threads,
                shared_arr,
                i,
                params.pattern,
                None,
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
