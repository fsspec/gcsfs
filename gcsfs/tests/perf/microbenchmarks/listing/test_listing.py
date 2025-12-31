import logging
import multiprocessing
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from gcsfs.tests.perf.microbenchmarks.conftest import (
    publish_benchmark_extra_info,
    publish_multi_process_benchmark_extra_info,
    publish_resource_metrics,
)
from gcsfs.tests.perf.microbenchmarks.listing.configs import get_listing_benchmark_cases
from gcsfs.tests.settings import BENCHMARK_SKIP_TESTS

pytestmark = pytest.mark.skipif(
    BENCHMARK_SKIP_TESTS,
    reason="""Skipping benchmark tests.
Set GCSFS_BENCHMARK_SKIP_TESTS=false to run them,
or use the orchestrator script at gcsfs/tests/perf/microbenchmarks/run.py""",
)

BENCHMARK_GROUP = "listing"


def _list_op(gcs, path):
    start_time = time.perf_counter()
    items = gcs.ls(path)
    duration_ms = (time.perf_counter() - start_time) * 1000
    logging.info(f"LIST : {path} - {len(items)} items - {duration_ms:.2f} ms.")


def _list_dirs(gcs, paths):
    for path in paths:
        _list_op(gcs, path)


def _chunk_list(data, n):
    k, m = divmod(len(data), n)
    return [data[i * k + min(i, m) : (i + 1) * k + min(i + 1, m)] for i in range(n)]


all_benchmark_cases = get_listing_benchmark_cases()

single_threaded_cases = [
    p for p in all_benchmark_cases if p.num_threads == 1 and p.num_processes == 1
]
multi_threaded_cases = [
    p for p in all_benchmark_cases if p.num_threads > 1 and p.num_processes == 1
]
multi_process_cases = [p for p in all_benchmark_cases if p.num_processes > 1]


@pytest.mark.parametrize(
    "gcsfs_benchmark_listing",
    single_threaded_cases,
    indirect=True,
    ids=lambda p: p.name,
)
def test_listing_single_threaded(benchmark, gcsfs_benchmark_listing, monitor):
    gcs, target_dirs, params = gcsfs_benchmark_listing

    publish_benchmark_extra_info(benchmark, params, BENCHMARK_GROUP)

    with monitor() as m:
        benchmark.pedantic(_list_dirs, rounds=params.rounds, args=(gcs, target_dirs))

    publish_resource_metrics(benchmark, m)


@pytest.mark.parametrize(
    "gcsfs_benchmark_listing",
    multi_threaded_cases,
    indirect=True,
    ids=lambda p: p.name,
)
def test_listing_multi_threaded(benchmark, gcsfs_benchmark_listing, monitor):
    gcs, target_dirs, params = gcsfs_benchmark_listing

    publish_benchmark_extra_info(benchmark, params, BENCHMARK_GROUP)

    def run_benchmark():
        logging.info("Multi-threaded listing benchmark: Starting benchmark round.")
        chunks = _chunk_list(target_dirs, params.num_threads)
        with ThreadPoolExecutor(max_workers=params.num_threads) as executor:
            futures = [
                executor.submit(_list_dirs, gcs, chunks[i])
                for i in range(params.num_threads)
            ]
            list(futures)  # Wait for completion

    with monitor() as m:
        benchmark.pedantic(run_benchmark, rounds=params.rounds)

    publish_resource_metrics(benchmark, m)


def _process_worker(gcs, target_dirs, num_threads, process_durations_shared, index):
    """A worker function for each process to list the directory."""
    start_time = time.perf_counter()
    chunks = _chunk_list(target_dirs, num_threads)
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [
            executor.submit(_list_dirs, gcs, chunks[i]) for i in range(num_threads)
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
    gcs, target_dirs, params = gcsfs_benchmark_listing
    publish_benchmark_extra_info(benchmark, params, BENCHMARK_GROUP)

    if multiprocessing.get_start_method(allow_none=True) != "spawn":
        multiprocessing.set_start_method("spawn", force=True)

    process_durations_shared = multiprocessing.Array("d", params.num_processes)
    worker_gcs_instances = [extended_gcs_factory() for _ in range(params.num_processes)]

    round_durations_s = []
    with monitor() as m:
        for _ in range(params.rounds):
            logging.info("Multi-process listing benchmark: Starting benchmark round.")
            chunks = _chunk_list(target_dirs, params.num_processes)
            processes = []
            for i in range(params.num_processes):
                p = multiprocessing.Process(
                    target=_process_worker,
                    args=(
                        worker_gcs_instances[i],
                        chunks[i],
                        params.num_threads,
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
