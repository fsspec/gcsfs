import logging
import multiprocessing
import os
from concurrent.futures import ThreadPoolExecutor

from gcsfs.tests.perf.microbenchmarks.conftest import (
    publish_benchmark_extra_info,
    publish_fixed_duration_benchmark_extra_info,
    publish_multi_process_benchmark_extra_info,
    publish_resource_metrics,
)


def filter_test_cases(all_cases):
    """Separates cases into single-threaded, multi-threaded, and multi-process."""
    single_threaded = [p for p in all_cases if p.threads == 1 and p.processes == 1]
    multi_threaded = [p for p in all_cases if p.threads > 1 and p.processes == 1]
    multi_process = [p for p in all_cases if p.processes > 1]
    return single_threaded, multi_threaded, multi_process


def run_single_threaded(benchmark, monitor_cls, params, func, args, benchmark_group):
    """Runs a single-threaded benchmark."""
    publish_benchmark_extra_info(benchmark, params, benchmark_group)

    with monitor_cls() as m:
        benchmark.pedantic(func, rounds=params.rounds, args=args)

    publish_resource_metrics(benchmark, m)


def run_single_threaded_fixed_duration(
    benchmark, monitor_cls, params, func, args, benchmark_group
):
    """Runs a single-threaded benchmark for a fixed duration (runtime) over multiple rounds."""
    publish_benchmark_extra_info(benchmark, params, benchmark_group)

    total_bytes_per_round = []
    with monitor_cls() as m:
        for round_num in range(params.rounds):
            logging.info(
                f"Single-threaded {benchmark_group} benchmark: Starting round {round_num + 1}/{params.rounds}."
            )
            # The func itself is expected to run for params.runtime and return total bytes
            bytes_this_round = func(*args)
            total_bytes_per_round.append(bytes_this_round)

    publish_fixed_duration_benchmark_extra_info(
        benchmark, total_bytes_per_round, params
    )
    publish_resource_metrics(benchmark, m)

    # This is to ensure the JSON report is generated correctly by pytest-benchmark
    benchmark.pedantic(lambda: None, rounds=1, iterations=1, warmup_rounds=0)


def run_multi_threaded(
    benchmark, monitor_cls, params, worker_func, args_list, benchmark_group
):
    """
    Runs a multi-threaded benchmark.

    Args:
        worker_func: The function to run in each thread.
        args_list: A list of tuples, where each tuple contains arguments for one thread.
    """
    publish_benchmark_extra_info(benchmark, params, benchmark_group)

    def workload():
        logging.info(
            f"Multi-threaded {benchmark_group} benchmark: Starting benchmark round."
        )
        with ThreadPoolExecutor(max_workers=params.threads) as executor:
            futures = [executor.submit(worker_func, *args) for args in args_list]
            for f in futures:
                f.result()

    with monitor_cls() as m:
        benchmark.pedantic(workload, rounds=params.rounds)

    publish_resource_metrics(benchmark, m)


def run_multi_process(
    benchmark,
    monitor_cls,
    params,
    extended_gcs_factory,
    worker_target,
    args_builder,
    benchmark_group,
    gcs_kwargs=None,
    request=None,
):
    """
    Orchestrates a multi-process benchmark.

    Args:
        worker_target: The function to run in each process.
        args_builder: A function (gcs_instance, process_index, shared_array) -> tuple
                      that returns the arguments for the worker_target.
        gcs_kwargs: Optional dictionary of arguments for the GCS factory (e.g. block_size).
    """
    publish_benchmark_extra_info(benchmark, params, benchmark_group)

    ctx = multiprocessing.get_context("spawn")
    process_data_shared = ctx.Array("d", params.processes)

    # Create GCS instances for workers
    gcs_kwargs = gcs_kwargs or {}
    worker_gcs_instances = [
        extended_gcs_factory(**gcs_kwargs) for _ in range(params.processes)
    ]

    results = []
    with monitor_cls() as m:
        for round_num in range(params.rounds):
            logging.info(
                f"Multi-process {benchmark_group} benchmark: Starting round {round_num + 1}/{params.rounds}."
            )
            processes = []

            total_cores = os.cpu_count() or 0
            old_affinity = os.sched_getaffinity(0)
            affinity_set = False
            if total_cores > 16:
                affinity_cores = set(range(16, total_cores - 10))
                if len(affinity_cores) >= params.processes:
                    os.sched_setaffinity(0, affinity_cores)
                    affinity_set = True

            try:
                for i in range(params.processes):
                    # Build arguments specific to this process (e.g. file slice)
                    p_args = args_builder(
                        worker_gcs_instances[i], i, process_data_shared
                    )
                    p = ctx.Process(target=worker_target, args=p_args)
                    processes.append(p)
                    p.start()
            finally:
                if affinity_set:
                    os.sched_setaffinity(0, old_affinity)

            for p in processes:
                p.join()

            if getattr(params, "runtime", None):
                results.append(int(sum(process_data_shared[:])))
            else:
                results.append(max(process_data_shared[:]))

    if getattr(params, "runtime", None):
        publish_fixed_duration_benchmark_extra_info(benchmark, results, params)
    else:
        publish_multi_process_benchmark_extra_info(benchmark, results, params)

    publish_resource_metrics(benchmark, m)

    # JSON report hook
    if request and request.config.getoption("benchmark_json"):
        benchmark.pedantic(lambda: None, rounds=1, iterations=1, warmup_rounds=0)
