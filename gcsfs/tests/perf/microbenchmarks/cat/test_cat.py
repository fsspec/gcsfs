"""Whole-object read benchmarks for cat_file() and cat().

Every case here reports ``requests_per_op`` next to latency. That column is the
one that catches request amplification: an extra metadata round-trip per read
is only a few milliseconds, which run-to-run network noise hides, but the count
is exact. See fsspec/gcsfs#1048.
"""

import pytest

from gcsfs.tests.perf.microbenchmarks.cat.configs import get_cat_benchmark_cases
from gcsfs.tests.perf.microbenchmarks.runner import (
    filter_test_cases,
    run_multi_threaded,
    run_single_threaded,
)

BENCHMARK_GROUP = "cat"


def _cat_kwargs(concurrency):
    """Leave gcsfs at its default concurrency unless a case pins one."""
    return {} if concurrency is None else {"concurrency": concurrency}


def _cat_file_whole(gcs, file_paths, file_size, concurrency):
    """Read each object with no explicit range, so gcsfs resolves the size."""
    kwargs = _cat_kwargs(concurrency)
    for path in file_paths:
        gcs.cat_file(path, **kwargs)


def _cat_file_ranged(gcs, file_paths, file_size, concurrency):
    """Read each object with the range supplied, skipping the size lookup."""
    kwargs = _cat_kwargs(concurrency)
    for path in file_paths:
        gcs.cat_file(path, start=0, end=file_size, **kwargs)


def _cat_batch(gcs, file_paths, file_size, concurrency):
    """Read the whole set of objects in one cat() call."""
    gcs.cat(file_paths)


# Must cover every entry in SUPPORTED_PATTERNS; test_configs.py pins that.
CAT_OPERATIONS = {
    "whole": _cat_file_whole,
    "ranged": _cat_file_ranged,
    "batch": _cat_batch,
}


def _build_cat_worker(params, gcs, file_paths):
    # Configured cases are validated in cat/configs.py; this catches a params
    # object built directly, in a test or from the REPL.
    try:
        op = CAT_OPERATIONS[params.pattern]
    except KeyError:
        raise ValueError(f"Unsupported cat pattern: {params.pattern}")
    return op, (gcs, file_paths, params.file_size_bytes, params.concurrency)


def _chunk_list(data, n):
    k, m = divmod(len(data), n)
    return [data[i * k + min(i, m) : (i + 1) * k + min(i + 1, m)] for i in range(n)]


all_benchmark_cases = get_cat_benchmark_cases()
single_threaded_cases, multi_threaded_cases, _ = filter_test_cases(all_benchmark_cases)


@pytest.mark.parametrize(
    "gcsfs_benchmark_cat",
    single_threaded_cases,
    indirect=True,
    ids=lambda p: p.name,
)
def test_cat_single_threaded(benchmark, gcsfs_benchmark_cat, monitor):
    gcs, file_paths, params = gcsfs_benchmark_cat

    op, op_args = _build_cat_worker(params, gcs, file_paths)

    # cat() issues one request per path just as the per-path loops do, so the
    # object count is the operation count for every pattern.
    run_single_threaded(
        benchmark,
        monitor,
        params,
        op,
        op_args,
        BENCHMARK_GROUP,
        operations_per_round=len(file_paths),
    )


@pytest.mark.parametrize(
    "gcsfs_benchmark_cat",
    multi_threaded_cases,
    indirect=True,
    ids=lambda p: p.name,
)
def test_cat_multi_threaded(benchmark, gcsfs_benchmark_cat, monitor):
    gcs, file_paths, params = gcsfs_benchmark_cat

    chunks = _chunk_list(file_paths, params.threads)
    args_list = [_build_cat_worker(params, gcs, chunk)[1] for chunk in chunks]
    op, _ = _build_cat_worker(params, gcs, file_paths)

    run_multi_threaded(
        benchmark,
        monitor,
        params,
        op,
        args_list,
        BENCHMARK_GROUP,
        operations_per_round=len(file_paths),
    )
