import logging
import multiprocessing
from concurrent.futures import ThreadPoolExecutor

from gcsfs.tests.perf.microbenchmarks.conftest import (
    publish_benchmark_extra_info,
    publish_multi_process_benchmark_extra_info,
    publish_resource_metrics,
)


def filter_test_cases(all_cases):
    """Separates cases into single-threaded, multi-threaded, and multi-process."""
    single_threaded = [
        p for p in all_cases if p.num_threads == 1 and p.num_processes == 1
    ]
    multi_threaded = [
        p for p in all_cases if p.num_threads > 1 and p.num_processes == 1
    ]
    multi_process = [p for p in all_cases if p.num_processes > 1]
    return single_threaded, multi_threaded, multi_process


def run_single_threaded(benchmark, monitor_cls, params, func, args, benchmark_group):
    """Runs a single-threaded benchmark."""
    publish_benchmark_extra_info(benchmark, params, benchmark_group)

    with monitor_cls() as m:
        benchmark.pedantic(func, rounds=params.rounds, args=args)

    publish_resource_metrics(benchmark, m)


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
        with ThreadPoolExecutor(max_workers=params.num_threads) as executor:
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

    if multiprocessing.get_start_method(allow_none=True) != "spawn":
        multiprocessing.set_start_method("spawn", force=True)

    process_durations_shared = multiprocessing.Array("d", params.num_processes)

    # Create GCS instances for workers
    gcs_kwargs = gcs_kwargs or {}
    worker_gcs_instances = [
        extended_gcs_factory(**gcs_kwargs) for _ in range(params.num_processes)
    ]

    round_durations_s = []
    with monitor_cls() as m:
        for _ in range(params.rounds):
            logging.info(
                f"Multi-process {benchmark_group} benchmark: Starting benchmark round."
            )
            processes = []

            for i in range(params.num_processes):
                # Build arguments specific to this process (e.g. file slice)
                p_args = args_builder(
                    worker_gcs_instances[i], i, process_durations_shared
                )

                p = multiprocessing.Process(target=worker_target, args=p_args)
                processes.append(p)
                p.start()

            for p in processes:
                p.join()

            round_durations_s.append(max(process_durations_shared[:]))

    publish_multi_process_benchmark_extra_info(benchmark, round_durations_s, params)
    publish_resource_metrics(benchmark, m)

    # JSON report hook
    if request and request.config.getoption("benchmark_json"):
        benchmark.pedantic(lambda: None, rounds=1, iterations=1, warmup_rounds=0)
