import contextlib
import inspect
import itertools
import logging
import multiprocessing
import os
from concurrent.futures import ThreadPoolExecutor

from gcsfs.tests.perf.microbenchmarks.conftest import (
    publish_benchmark_extra_info,
    publish_fixed_duration_benchmark_extra_info,
    publish_multi_process_benchmark_extra_info,
    publish_request_metrics,
    publish_resource_metrics,
)
from gcsfs.tests.perf.microbenchmarks.request_counter import RequestCounter
from gcsfs.tests.settings import BENCHMARK_CPU_AFFINITY


@contextlib.contextmanager
def _optional_request_counter(operations_per_round):
    """Count gcsfs HTTP calls, but only when the caller knows the op count.

    Without the number of operations the raw total is not comparable between
    cases, so groups that cannot supply it opt out and pay no patching cost.
    """
    if not operations_per_round:
        yield None
        return

    with RequestCounter() as counter:
        yield counter


def filter_test_cases(all_cases):
    """Separates cases into single-threaded, multi-threaded, and multi-process."""
    single_threaded = [p for p in all_cases if p.threads == 1 and p.processes == 1]
    multi_threaded = [p for p in all_cases if p.threads > 1 and p.processes == 1]
    multi_process = [p for p in all_cases if p.processes > 1]
    return single_threaded, multi_threaded, multi_process


def _accepts_round_num(fn) -> bool:
    try:
        sig = inspect.signature(fn)
    except (TypeError, ValueError):
        return False
    positional = [
        p
        for p in sig.parameters.values()
        if p.kind
        in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        )
    ]
    return len(positional) >= 4


def run_single_threaded(
    benchmark,
    monitor_cls,
    params,
    func,
    args,
    benchmark_group,
    total_bytes=None,
    operations_per_round=None,
    round_args_builder=None,
):
    """Runs a single-threaded benchmark.

    Args:
        total_bytes: Optional override for total bytes transferred (e.g. for cat_ranges).
        operations_per_round: Number of filesystem operations ``func`` performs
            per round. When given, the HTTP calls gcsfs issues are counted and
            reported as ``requests_per_op``.
        round_args_builder: Optional ``(round_num) -> tuple`` hook invoked outside
            the timed region before each round (e.g. to rotate ``*_rcu_cold`` paths).
    """
    publish_benchmark_extra_info(
        benchmark, params, benchmark_group, total_bytes=total_bytes
    )

    with monitor_cls() as m, _optional_request_counter(operations_per_round) as counter:
        if round_args_builder is None:
            benchmark.pedantic(func, rounds=params.rounds, args=args)
        else:
            round_counter = itertools.count()
            benchmark.pedantic(
                func,
                setup=lambda: (round_args_builder(next(round_counter)), {}),
                rounds=params.rounds,
            )

    publish_request_metrics(
        benchmark, counter, (operations_per_round or 0) * params.rounds
    )
    publish_resource_metrics(benchmark, m)


def run_single_threaded_fixed_duration(
    benchmark,
    monitor_cls,
    params,
    func,
    args,
    benchmark_group,
    round_args_builder=None,
):
    """Runs a single-threaded benchmark for a fixed duration (runtime) over multiple rounds."""
    publish_benchmark_extra_info(benchmark, params, benchmark_group)

    total_bytes_per_round = []
    with monitor_cls() as m:
        for round_num in range(params.rounds):
            logging.info(
                f"Single-threaded {benchmark_group} benchmark: Starting round {round_num + 1}/{params.rounds}."
            )
            round_args = (
                round_args_builder(round_num)
                if round_args_builder is not None
                else args
            )
            # The func itself is expected to run for params.runtime and return total bytes
            bytes_this_round = func(*round_args)
            total_bytes_per_round.append(bytes_this_round)

    publish_fixed_duration_benchmark_extra_info(
        benchmark, total_bytes_per_round, params
    )
    publish_resource_metrics(benchmark, m)

    # This is to ensure the JSON report is generated correctly by pytest-benchmark
    benchmark.pedantic(lambda: None, rounds=1, iterations=1, warmup_rounds=0)


def run_multi_threaded(
    benchmark,
    monitor_cls,
    params,
    worker_func,
    args_list,
    benchmark_group,
    total_bytes=None,
    operations_per_round=None,
    round_args_builder=None,
):
    """
    Runs a multi-threaded benchmark.

    Args:
        worker_func: The function to run in each thread.
        args_list: A list of tuples, where each tuple contains arguments for one thread.
        total_bytes: Optional override for total bytes transferred.
        operations_per_round: Total filesystem operations performed across all
            threads per round. When given, the HTTP calls gcsfs issues are
            counted and reported as ``requests_per_op``.
        round_args_builder: Optional ``(round_num) -> list[tuple]`` hook invoked
            outside the timed region before each round.
    """
    publish_benchmark_extra_info(
        benchmark, params, benchmark_group, total_bytes=total_bytes
    )

    current_args_list = [args_list]

    def workload():
        logging.info(
            f"Multi-threaded {benchmark_group} benchmark: Starting benchmark round."
        )
        with ThreadPoolExecutor(max_workers=params.threads) as executor:
            futures = [
                executor.submit(worker_func, *args) for args in current_args_list[0]
            ]
            for f in futures:
                f.result()

    with monitor_cls() as m, _optional_request_counter(operations_per_round) as counter:
        if round_args_builder is None:
            benchmark.pedantic(workload, rounds=params.rounds)
        else:
            round_counter = itertools.count()

            def _setup_round():
                current_args_list[0] = round_args_builder(next(round_counter))

            benchmark.pedantic(workload, setup=_setup_round, rounds=params.rounds)

    publish_request_metrics(
        benchmark, counter, (operations_per_round or 0) * params.rounds
    )
    publish_resource_metrics(benchmark, m)


def run_multi_threaded_fixed_duration(
    benchmark,
    monitor_cls,
    params,
    worker_func,
    args_list,
    benchmark_group,
    round_args_builder=None,
):
    """
    Runs a multi-threaded benchmark for a fixed duration and records bytes per round.

    Note: In gcsfs, while the workload submission is multi-threaded (using a ThreadPoolExecutor),
    the actual execution of underlying requests in gcsfs is handled using multi-coroutines
    running in a single background event loop thread.

    Args:
        worker_func: The function to run in each thread. It must return bytes processed.
        args_list: A list of tuples, where each tuple contains arguments for one thread.
        round_args_builder: Optional ``(round_num) -> list[tuple]`` hook invoked
            before each round.
    """
    publish_benchmark_extra_info(benchmark, params, benchmark_group)

    total_bytes_per_round = []
    with monitor_cls() as m:
        for round_num in range(params.rounds):
            logging.info(
                f"Multi-threaded fixed-duration {benchmark_group} benchmark: "
                f"Starting round {round_num + 1}/{params.rounds}."
            )
            round_args_list = (
                round_args_builder(round_num)
                if round_args_builder is not None
                else args_list
            )
            with ThreadPoolExecutor(max_workers=params.threads) as executor:
                futures = [
                    executor.submit(worker_func, *args) for args in round_args_list
                ]
                total_bytes_per_round.append(sum(f.result() for f in futures))

    publish_fixed_duration_benchmark_extra_info(
        benchmark, total_bytes_per_round, params
    )
    publish_resource_metrics(benchmark, m)

    # This is to ensure the JSON report is generated correctly by pytest-benchmark
    benchmark.pedantic(lambda: None, rounds=1, iterations=1, warmup_rounds=0)


def _multiprocess_worker_wrapper(worker_target, args):
    """Wrapper to apply emulator mock inside spawned child processes."""
    from gcsfs.tests.utils import _patch_get_bucket_layout_for_emulator

    patch = _patch_get_bucket_layout_for_emulator()
    if patch:
        with patch:
            worker_target(*args)
    else:
        worker_target(*args)


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
    total_bytes=None,
):
    """
    Orchestrates a multi-process benchmark.

    Args:
        worker_target: The function to run in each process.
        args_builder: A function (gcs_instance, process_index, shared_array) -> tuple
                      that returns the arguments for the worker_target.
        gcs_kwargs: Optional dictionary of arguments for the GCS factory (e.g. block_size).
    """
    publish_benchmark_extra_info(
        benchmark, params, benchmark_group, total_bytes=total_bytes
    )

    ctx = multiprocessing.get_context("forkserver")
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
            for i in range(params.processes):
                process_data_shared[i] = 0.0

            processes = []

            affinity_set = False
            old_affinity = os.sched_getaffinity(0)
            if BENCHMARK_CPU_AFFINITY:
                total_cores = os.cpu_count() or 0
                if total_cores > 16:
                    affinity_cores = set(range(16, total_cores - 10))
                    if len(affinity_cores) >= params.processes:
                        os.sched_setaffinity(0, affinity_cores)
                        affinity_set = True

            try:
                pass_round = _accepts_round_num(args_builder)
                for i in range(params.processes):
                    # Build arguments specific to this process (e.g. file slice)
                    if pass_round:
                        p_args = args_builder(
                            worker_gcs_instances[i], i, process_data_shared, round_num
                        )
                    else:
                        p_args = args_builder(
                            worker_gcs_instances[i], i, process_data_shared
                        )
                    p = ctx.Process(
                        target=_multiprocess_worker_wrapper,
                        args=(worker_target, p_args),
                    )
                    processes.append(p)
                    p.start()
            finally:
                if affinity_set:
                    os.sched_setaffinity(0, old_affinity)

            for i, p in enumerate(processes):
                p.join()
                if p.exitcode != 0:
                    for remaining_p in processes:
                        if remaining_p.is_alive():
                            remaining_p.terminate()
                            remaining_p.join()
                    raise RuntimeError(
                        f"Worker process {i} exited with code {p.exitcode}"
                    )

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
