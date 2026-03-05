import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from gcsfs.tests.perf.microbenchmarks.runner import (
    filter_test_cases,
    run_multi_process,
    run_single_threaded_fixed_duration,
)
from gcsfs.tests.perf.microbenchmarks.write_fixed_duration.configs import (
    get_write_fixed_duration_benchmark_cases,
)

BENCHMARK_GROUP = "write_fixed_duration"


def _write_op_seq_fixed_duration(gcs, file_path, chunk_size, runtime):
    """Write to a single file sequentially for a fixed duration."""
    total_bytes_written = 0
    start_time = time.perf_counter()

    # Pre-generate chunk to avoid overhead during write loop
    data_chunk = os.urandom(chunk_size)

    try:
        with gcs.open(file_path, "wb") as f:
            while time.perf_counter() - start_time < runtime:
                f.write(data_chunk)
                total_bytes_written += chunk_size
    except Exception as e:
        logging.error(f"Error writing to {file_path}: {e}")
        raise

    return total_bytes_written


all_benchmark_cases = get_write_fixed_duration_benchmark_cases()
single_threaded_cases, _, multi_process_cases = filter_test_cases(all_benchmark_cases)


@pytest.mark.parametrize(
    "gcsfs_benchmark_write",
    single_threaded_cases,
    indirect=True,
    ids=lambda p: p.name,
)
def test_write_single_threaded(benchmark, gcsfs_benchmark_write, monitor):
    gcs, file_paths, params = gcsfs_benchmark_write

    op_args = (
        gcs,
        file_paths[0],
        params.chunk_size_bytes,
        params.runtime,
    )
    run_single_threaded_fixed_duration(
        benchmark,
        monitor,
        params,
        _write_op_seq_fixed_duration,
        op_args,
        BENCHMARK_GROUP,
    )


def _process_worker_fixed_duration(
    gcs,
    file_paths,
    chunk_size,
    threads,
    process_data_shared,
    index,
    runtime,
):
    """A worker function for each process to write files for a fixed duration."""
    with ThreadPoolExecutor(max_workers=threads) as executor:
        futures = [
            executor.submit(
                _write_op_seq_fixed_duration,
                gcs,
                file_paths[i],
                chunk_size,
                runtime,
            )
            for i in range(threads)
        ]
        results = [f.result() for f in futures]
        total_bytes = sum(results)

    process_data_shared[index] = float(total_bytes)


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
    files_per_process = params.files // params.processes

    def args_builder(gcs_instance, i, shared_arr):
        start_index = i * files_per_process
        end_index = start_index + files_per_process
        process_files = file_paths[start_index:end_index]
        return (
            gcs_instance,
            process_files,
            params.chunk_size_bytes,
            params.threads,
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
        request=request,
    )
