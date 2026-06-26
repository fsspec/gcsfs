import logging
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from gcsfs.tests.perf.microbenchmarks.put.configs import get_put_benchmark_cases
from gcsfs.tests.perf.microbenchmarks.runner import (
    filter_test_cases,
    run_multi_process,
    run_single_threaded,
)

BENCHMARK_GROUP = "put"


def _put_op(gcs, local_path, remote_path, chunk_size):
    """Upload a local file to a single remote path."""
    try:
        gcs.put(local_path, remote_path, chunksize=chunk_size)
    except Exception as e:
        logging.error(f"Error putting {local_path} to {remote_path}: {e}")
        raise


all_benchmark_cases = get_put_benchmark_cases()
single_threaded_cases, _, multi_process_cases = filter_test_cases(all_benchmark_cases)


@pytest.mark.parametrize(
    "gcsfs_benchmark_put",
    single_threaded_cases,
    indirect=True,
    ids=lambda p: p.name,
)
def test_put_single_threaded(benchmark, gcsfs_benchmark_put, monitor):
    gcs, local_path, file_paths, params = gcsfs_benchmark_put

    op_args = (
        gcs,
        local_path,
        file_paths[0],
        params.chunk_size_bytes,
    )
    run_single_threaded(
        benchmark,
        monitor,
        params,
        _put_op,
        op_args,
        BENCHMARK_GROUP,
    )


def _process_worker(
    gcs,
    local_path,
    file_paths,
    chunk_size,
    process_durations_shared,
    index,
):
    """A worker function for each process to upload files concurrently."""
    start_time = time.perf_counter()

    with ThreadPoolExecutor(max_workers=len(file_paths)) as executor:
        futures = [
            executor.submit(
                _put_op,
                gcs,
                local_path,
                remote_path,
                chunk_size,
            )
            for remote_path in file_paths
        ]
        [f.result() for f in futures]

    duration_s = time.perf_counter() - start_time
    process_durations_shared[index] = duration_s


@pytest.mark.parametrize(
    "gcsfs_benchmark_put",
    multi_process_cases,
    indirect=True,
    ids=lambda p: p.name,
)
def test_put_multi_process(
    benchmark, gcsfs_benchmark_put, extended_gcs_factory, request, monitor
):
    _, local_path, file_paths, params = gcsfs_benchmark_put
    files_per_process = params.files // params.processes

    def args_builder(gcs_instance, i, shared_arr):
        start_index = i * files_per_process
        end_index = start_index + files_per_process
        process_files = file_paths[start_index:end_index]
        return (
            gcs_instance,
            local_path,
            process_files,
            params.chunk_size_bytes,
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
