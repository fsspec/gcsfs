import logging
import random
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from gcsfs.tests.perf.microbenchmarks.read.configs import get_read_benchmark_cases
from gcsfs.tests.perf.microbenchmarks.runner import (
    filter_test_cases,
    run_multi_process,
    run_multi_threaded,
    run_single_threaded,
)
from gcsfs.tests.settings import BENCHMARK_SKIP_TESTS

pytestmark = pytest.mark.skipif(
    BENCHMARK_SKIP_TESTS,
    reason="""Skipping benchmark tests.
Set GCSFS_BENCHMARK_SKIP_TESTS=false to run them,
or use the orchestrator script at gcsfs/tests/perf/microbenchmarks/run.py""",
)

BENCHMARK_GROUP = "read"


def _read_op_seq(gcs, path, chunk_size):
    start_time = time.perf_counter()
    with gcs.open(path, "rb") as f:
        while f.read(chunk_size):
            pass
    duration_ms = (time.perf_counter() - start_time) * 1000
    logging.info(f"SEQ_READ : {path} - {duration_ms:.2f} ms.")


def _read_op_rand(gcs, path, chunk_size, offsets):
    start_time = time.perf_counter()
    # Random benchmarks should not prefetch
    with gcs.open(path, "rb", cache_type="none") as f:
        for offset in offsets:
            f.seek(offset)
            f.read(chunk_size)
    duration_ms = (time.perf_counter() - start_time) * 1000
    logging.info(f"RAND_READ : {path} - {duration_ms:.2f} ms.")


def _random_read_worker(gcs, path, chunk_size, offsets):
    """A worker that reads a file from random offsets."""
    local_offsets = list(offsets)
    random.shuffle(local_offsets)
    _read_op_rand(gcs, path, chunk_size, local_offsets)


all_benchmark_cases = get_read_benchmark_cases()
single_threaded_cases, multi_threaded_cases, multi_process_cases = filter_test_cases(
    all_benchmark_cases
)


@pytest.mark.parametrize(
    "gcsfs_benchmark_read",
    single_threaded_cases,
    indirect=True,
    ids=lambda p: p.name,
)
def test_read_single_threaded(benchmark, gcsfs_benchmark_read, monitor):
    gcs, file_paths, params = gcsfs_benchmark_read

    op = None
    op_args = None
    if params.pattern == "seq":
        op = _read_op_seq
        op_args = (gcs, file_paths[0], params.chunk_size_bytes)
    elif params.pattern == "rand":
        offsets = list(range(0, params.file_size_bytes, params.chunk_size_bytes))
        op = _random_read_worker
        op_args = (gcs, file_paths[0], params.chunk_size_bytes, offsets)

    run_single_threaded(benchmark, monitor, params, op, op_args, BENCHMARK_GROUP)


@pytest.mark.parametrize(
    "gcsfs_benchmark_read",
    multi_threaded_cases,
    indirect=True,
    ids=lambda p: p.name,
)
def test_read_multi_threaded(benchmark, gcsfs_benchmark_read, monitor):
    gcs, file_paths, params = gcsfs_benchmark_read

    def workload():
        logging.info("Multi-threaded benchmark: Starting benchmark round.")
        with ThreadPoolExecutor(max_workers=params.num_threads) as executor:
            if params.pattern == "seq":
                # Each thread reads one full file sequentially.
                futures = [
                    executor.submit(_read_op_seq, gcs, path, params.chunk_size_bytes)
                    for path in file_paths
                ]
                list(futures)  # Wait for completion

            elif params.pattern == "rand":

                offsets = list(
                    range(0, params.file_size_bytes, params.chunk_size_bytes)
                )

                if params.num_files == 1:
                    # All threads read the same file randomly.
                    paths_to_read = [file_paths[0]] * params.num_threads
                else:
                    # Each thread reads a different file randomly.
                    paths_to_read = file_paths

                futures = [
                    executor.submit(
                        _random_read_worker, gcs, path, params.chunk_size_bytes, offsets
                    )
                    for path in paths_to_read
                ]
                list(futures)  # Wait for completion

    run_multi_threaded(benchmark, monitor, params, workload, BENCHMARK_GROUP)


def _process_worker(
    gcs,
    file_paths,
    chunk_size,
    num_threads,
    pattern,
    file_size_bytes,
    process_durations_shared,
    index,
):
    """A worker function for each process to read a list of files."""
    start_time = time.perf_counter()
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        if pattern == "seq":
            futures = [
                executor.submit(_read_op_seq, gcs, path, chunk_size)
                for path in file_paths
            ]
        elif pattern == "rand":
            offsets = list(range(0, file_size_bytes, chunk_size))

            futures = [
                executor.submit(_random_read_worker, gcs, path, chunk_size, offsets)
                for path in file_paths
            ]

            # Wait for all threads in the process to complete
            list(futures)
    duration_s = time.perf_counter() - start_time
    process_durations_shared[index] = duration_s


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
    files_per_process = params.num_files // params.num_processes

    def args_builder(gcs_instance, i, shared_arr):
        if params.num_files > 1:
            start_index = i * files_per_process
            end_index = start_index + files_per_process
            process_files = file_paths[start_index:end_index]
        else:  # num_files == 1
            # Each process will have its threads read from the same single file
            process_files = [file_paths[0]] * params.num_threads

        return (
            gcs_instance,
            process_files,
            params.chunk_size_bytes,
            params.num_threads,
            params.pattern,
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
        gcs_kwargs={"block_size": params.block_size_bytes},
        request=request,
    )
