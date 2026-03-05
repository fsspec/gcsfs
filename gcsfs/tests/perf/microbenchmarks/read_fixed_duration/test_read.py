import itertools
import random
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from gcsfs.tests.perf.microbenchmarks.read_fixed_duration.configs import (
    get_read_fixed_duration_benchmark_cases,
)
from gcsfs.tests.perf.microbenchmarks.runner import (
    filter_test_cases,
    run_multi_process,
    run_single_threaded_fixed_duration,
)

BENCHMARK_GROUP = "read_fixed_duration"


def _read_op_seq(gcs, file_paths, chunk_size, runtime):
    """Read files sequentially for a fixed duration."""
    total_bytes_read = 0
    start_time = time.perf_counter()
    files_it = itertools.cycle(file_paths)
    while time.perf_counter() - start_time < runtime:
        path = next(files_it)
        with gcs.open(path, "rb") as f:
            while time.perf_counter() - start_time < runtime:
                data = f.read(chunk_size)
                if not data:
                    break
                total_bytes_read += len(data)
    return total_bytes_read


def _read_op_rand(gcs, file_paths, chunk_size, offsets, runtime):
    """Read files from random offsets for a fixed duration."""
    total_bytes_read = 0
    start_time = time.perf_counter()
    files_it = itertools.cycle(file_paths)
    while time.perf_counter() - start_time < runtime:
        path = next(files_it)
        with gcs.open(path, "rb", cache_type="none") as f:
            for offset in offsets:
                if time.perf_counter() - start_time >= runtime:
                    break
                f.seek(offset)
                data = f.read(chunk_size)
                total_bytes_read += len(data)
    return total_bytes_read


def _random_read_worker(gcs, file_paths, chunk_size, offsets, runtime):
    """A worker that reads files from random offsets."""
    local_offsets = list(offsets)
    random.shuffle(local_offsets)
    return _read_op_rand(gcs, file_paths, chunk_size, local_offsets, runtime)


all_benchmark_cases = get_read_fixed_duration_benchmark_cases()
single_threaded_cases, _, multi_process_cases = filter_test_cases(all_benchmark_cases)


@pytest.mark.parametrize(
    "gcsfs_benchmark_read",
    single_threaded_cases,
    indirect=True,
    ids=lambda p: p.name,
)
def test_read_single_threaded(benchmark, gcsfs_benchmark_read, monitor):
    gcs, file_paths, params = gcsfs_benchmark_read

    op = None
    op_args = (gcs, file_paths, params.chunk_size_bytes, params.runtime)
    if params.pattern == "seq":
        op = _read_op_seq
    elif params.pattern == "rand":
        op = _random_read_worker
        offsets = list(range(0, params.file_size_bytes, params.chunk_size_bytes))
        op_args = (gcs, file_paths, params.chunk_size_bytes, offsets, params.runtime)

    run_single_threaded_fixed_duration(
        benchmark, monitor, params, op, op_args, BENCHMARK_GROUP
    )


def _process_worker_fixed_duration(
    gcs,
    file_paths,
    chunk_size,
    threads,
    pattern,
    file_size_bytes,
    process_data_shared,
    index,
    runtime,
):
    """A worker function for each process to read files for a fixed duration."""
    with ThreadPoolExecutor(max_workers=threads) as executor:
        futures = []
        if pattern == "seq":
            futures = [
                executor.submit(_read_op_seq, gcs, file_paths, chunk_size, runtime)
                for _ in range(threads)
            ]
        elif pattern == "rand":
            offsets = list(range(0, file_size_bytes, chunk_size))
            futures = [
                executor.submit(
                    _random_read_worker, gcs, file_paths, chunk_size, offsets, runtime
                )
                for _ in range(threads)
            ]

        results = [f.result() for f in futures]
        total_bytes = sum(results)

    process_data_shared[index] = float(total_bytes)


@pytest.mark.parametrize(
    "gcsfs_benchmark_read",
    multi_process_cases,
    indirect=True,
    ids=lambda p: p.name,
)
def test_read_multi_process(
    benchmark, gcsfs_benchmark_read, extended_gcs_factory, request, monitor
):
    _, file_paths, params = gcsfs_benchmark_read

    def args_builder(gcs_instance, i, shared_arr):
        process_file_paths = list(file_paths)
        random.shuffle(process_file_paths)
        return (
            gcs_instance,
            process_file_paths,
            params.chunk_size_bytes,
            params.threads,
            params.pattern,
            params.file_size_bytes,
            shared_arr,
            i,
            params.runtime,
        )

    run_multi_process(
        benchmark,
        monitor,
        params,
        extended_gcs_factory,
        worker_target=_process_worker_fixed_duration,
        args_builder=args_builder,
        benchmark_group=BENCHMARK_GROUP,
        gcs_kwargs={"block_size": params.block_size_bytes},
        request=request,
    )
