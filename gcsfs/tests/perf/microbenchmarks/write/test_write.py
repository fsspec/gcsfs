import logging
import multiprocessing
import os
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from gcsfs.tests.perf.microbenchmarks.conftest import (
    publish_benchmark_extra_info,
    publish_multi_process_benchmark_extra_info,
    publish_resource_metrics,
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


def _write_op_seq(gcs, path, chunk_size, file_size, data_chunk):
    chunks = file_size // chunk_size
    remainder = file_size % chunk_size
    start_time = time.perf_counter()
    with gcs.open(path, "wb") as f:
        for _ in range(chunks):
            f.write(data_chunk)
        if remainder:
            f.write(data_chunk[:remainder])
    duration_ms = (time.perf_counter() - start_time) * 1000
    logging.info(f"SEQ_WRITE : {path} - {duration_ms:.2f} ms.")


all_benchmark_cases = get_write_benchmark_cases()

single_threaded_cases = [
    p for p in all_benchmark_cases if p.num_threads == 1 and p.num_processes == 1
]
multi_threaded_cases = [
    p for p in all_benchmark_cases if p.num_threads > 1 and p.num_processes == 1
]
multi_process_cases = [p for p in all_benchmark_cases if p.num_processes > 1]


@pytest.mark.parametrize(
    "gcsfs_benchmark_write",
    single_threaded_cases,
    indirect=True,
    ids=lambda p: p.name,
)
def test_write_single_threaded(benchmark, gcsfs_benchmark_write, monitor):
    gcs, file_paths, params = gcsfs_benchmark_write

    publish_benchmark_extra_info(benchmark, params, BENCHMARK_GROUP)
    path = file_paths[0]

    # Pre-generate data chunk to avoid overhead during write loop
    data_chunk = os.urandom(params.chunk_size_bytes)

    op_args = (gcs, path, params.chunk_size_bytes, params.file_size_bytes, data_chunk)

    with monitor() as m:
        benchmark.pedantic(_write_op_seq, rounds=params.rounds, args=op_args)

    publish_resource_metrics(benchmark, m)


@pytest.mark.parametrize(
    "gcsfs_benchmark_write",
    multi_threaded_cases,
    indirect=True,
    ids=lambda p: p.name,
)
def test_write_multi_threaded(benchmark, gcsfs_benchmark_write, monitor):
    gcs, file_paths, params = gcsfs_benchmark_write

    publish_benchmark_extra_info(benchmark, params, BENCHMARK_GROUP)

    data_chunk = os.urandom(params.chunk_size_bytes)

    def run_benchmark():
        logging.info("Multi-threaded benchmark: Starting benchmark round.")
        with ThreadPoolExecutor(max_workers=params.num_threads) as executor:
            futures = [
                executor.submit(
                    _write_op_seq,
                    gcs,
                    path,
                    params.chunk_size_bytes,
                    params.file_size_bytes,
                    data_chunk,
                )
                for path in file_paths
            ]
            list(futures)  # Wait for completion

    with monitor() as m:
        benchmark.pedantic(run_benchmark, rounds=params.rounds)

    publish_resource_metrics(benchmark, m)


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
    # Generate data chunk inside process to avoid pickling large data
    data_chunk = os.urandom(chunk_size)

    start_time = time.perf_counter()
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [
            executor.submit(
                _write_op_seq, gcs, path, chunk_size, file_size_bytes, data_chunk
            )
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
    gcs, file_paths, params = gcsfs_benchmark_write
    publish_benchmark_extra_info(benchmark, params, BENCHMARK_GROUP)

    if multiprocessing.get_start_method(allow_none=True) != "spawn":
        multiprocessing.set_start_method("spawn", force=True)

    process_durations_shared = multiprocessing.Array("d", params.num_processes)
    files_per_process = params.num_files // params.num_processes
    threads_per_process = params.num_threads

    # Create a new gcsfs instance for every process
    worker_gcs_instances = [extended_gcs_factory() for _ in range(params.num_processes)]

    round_durations_s = []
    with monitor() as m:
        for _ in range(params.rounds):
            logging.info("Multi-process benchmark: Starting benchmark round.")
            processes = []

            for i in range(params.num_processes):
                start_index = i * files_per_process
                end_index = start_index + files_per_process
                process_files = file_paths[start_index:end_index]

                p = multiprocessing.Process(
                    target=_process_worker,
                    args=(
                        worker_gcs_instances[i],
                        process_files,
                        params.chunk_size_bytes,
                        threads_per_process,
                        params.file_size_bytes,
                        process_durations_shared,
                        i,
                    ),
                )
                processes.append(p)
                p.start()

            for p in processes:
                p.join()

            round_durations_s.append(max(process_durations_shared[:]))

    publish_multi_process_benchmark_extra_info(benchmark, round_durations_s, params)
    publish_resource_metrics(benchmark, m)

    if request.config.getoption("benchmark_json"):
        benchmark.pedantic(lambda: None, rounds=1, iterations=1, warmup_rounds=0)
