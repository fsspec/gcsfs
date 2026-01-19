import logging
import multiprocessing
import os
import random
import statistics
import time
import uuid
from typing import Any, List

import pytest
from resource_monitor import ResourceMonitor

MB = 1024 * 1024


def _write_file(gcs, path, file_size, chunk_size):
    chunks_to_write = file_size // chunk_size
    remainder = file_size % chunk_size
    with gcs.open(path, "wb", finalize_on_close=True) as f:
        for _ in range(chunks_to_write):
            f.write(os.urandom(chunk_size))
        if remainder > 0:
            f.write(os.urandom(remainder))

    actual_size = gcs.info(path)["size"]
    if actual_size != file_size:
        raise RuntimeError(
            f"Data integrity check failed for {path}. "
            f"Expected size: {file_size}, Actual size: {actual_size}"
        )


def _prepare_files(gcs, file_paths, file_size=0):
    if file_size > 0:
        chunk_size = min(100 * MB, file_size)
        pool_size = 16
    else:
        chunk_size = 1
        pool_size = min(100, len(file_paths))

    args = [(gcs, path, file_size, chunk_size) for path in file_paths]
    ctx = multiprocessing.get_context("spawn")
    with ctx.Pool(pool_size) as pool:
        try:
            pool.starmap(_write_file, args)
        except RuntimeError as e:
            pytest.fail(str(e))


def _benchmark_io_fixture_helper(
    extended_gcs_factory, params, prefix_tag, create_files=False, gcs_kwargs=None
):
    gcs_kwargs = gcs_kwargs or {}
    gcs = extended_gcs_factory(**gcs_kwargs)

    prefix = f"{params.bucket_name}/{prefix_tag}-{uuid.uuid4()}"
    file_paths = [f"{prefix}/file_{i}" for i in range(params.files)]

    action = "creating" if create_files else "targeting"
    logging.info(
        f"Setting up benchmark '{params.name}': {action} {params.files} file(s) "
        f"of size {params.file_size_bytes / MB:.2f} MB each."
    )

    if create_files:
        start_time = time.perf_counter()
        _prepare_files(gcs, file_paths, params.file_size_bytes)

        duration_ms = (time.perf_counter() - start_time) * 1000
        logging.info(
            f"Benchmark '{params.name}' setup created {params.files} files in {duration_ms:.2f} ms."
        )

    yield gcs, file_paths, params

    # --- Teardown ---
    logging.info(f"Tearing down benchmark '{params.name}': deleting files.")
    try:
        gcs.rm(prefix, recursive=True)
    except Exception as e:
        logging.error(f"Failed to clean up benchmark files: {e}")


@pytest.fixture
def monitor():
    """
    Provides the ResourceMonitor class.
    Usage: with monitor() as m: ...
    """
    return ResourceMonitor


@pytest.fixture
def gcsfs_benchmark_read(extended_gcs_factory, request):
    """
    A fixture that creates temporary files for a benchmark run and cleans
    them up afterward.

    It uses the parameters from the test's parametrization
    to determine how many files to create and of what size.
    """
    params = request.param
    yield from _benchmark_io_fixture_helper(
        extended_gcs_factory,
        params,
        "benchmark-read",
        create_files=True,
        gcs_kwargs={"block_size": params.block_size_bytes},
    )


@pytest.fixture
def gcsfs_benchmark_write(extended_gcs_factory, request):
    """
    A fixture that sets up the environment for a write benchmark run.
    It provides a GCSFS instance and a list of file paths to write to.
    """
    params = request.param
    yield from _benchmark_io_fixture_helper(
        extended_gcs_factory,
        params,
        "benchmark-write",
        create_files=False,
    )


@pytest.fixture
def gcsfs_benchmark_listing(extended_gcs_factory, request):
    """
    A fixture that sets up the environment for a listing benchmark run.
    It creates a directory structure with 0-byte files.
    """
    params = request.param
    gcs = extended_gcs_factory()

    prefix = f"{params.bucket_name}/benchmark-listing-{uuid.uuid4()}"

    target_dirs = [prefix]
    candidates = [(prefix, 0)]

    for i in range(params.folders):
        valid_parents = [p for p in candidates if p[1] <= params.depth]
        parent_path, parent_depth = random.choice(valid_parents)
        new_path = f"{parent_path}/folder_{i}"
        target_dirs.append(new_path)
        candidates.append((new_path, parent_depth + 1))

    file_paths = []
    for i in range(params.files):
        folder = random.choice(target_dirs)
        file_paths.append(f"{folder}/file_{i}")

    logging.info(
        f"Setting up benchmark '{params.name}': creating {params.files} "
        f"files distributed across {len(target_dirs) - 1} folders at depth {params.depth}."
    )

    start_time = time.perf_counter()
    _prepare_files(gcs, file_paths)

    duration_ms = (time.perf_counter() - start_time) * 1000
    logging.info(
        f"Benchmark '{params.name}' setup created {params.files} files in {duration_ms:.2f} ms."
    )

    yield gcs, target_dirs, prefix, params

    # --- Teardown ---
    logging.info(f"Tearing down benchmark '{params.name}': deleting files and folders.")
    try:
        gcs.rm(file_paths)
        if params.bucket_type != "regional":
            # Sort by length descending to delete children first
            for d in sorted(target_dirs, key=len, reverse=True):
                try:
                    gcs.rmdir(d)
                except Exception:
                    pass
        else:
            try:
                gcs.rmdir(prefix)
            except Exception:
                pass
    except Exception as e:
        logging.error(f"Failed to clean up benchmark files: {e}")


def pytest_benchmark_generate_json(config, benchmarks, machine_info, commit_info):
    """
    Hook to post-process benchmark results before generating the JSON report.
    """
    for bench in benchmarks:
        if "timings" in bench.get("extra_info", {}):
            bench.stats.data = bench.extra_info["timings"]
            bench.stats.min = bench.extra_info["min_time"]
            bench.stats.max = bench.extra_info["max_time"]
            bench.stats.mean = bench.extra_info["mean_time"]
            bench.stats.median = bench.extra_info["median_time"]
            bench.stats.stddev = bench.extra_info["stddev_time"]
            bench.stats.rounds = bench.extra_info["rounds"]

            del bench.extra_info["timings"]
            del bench.extra_info["min_time"]
            del bench.extra_info["max_time"]
            del bench.extra_info["mean_time"]
            del bench.extra_info["median_time"]
            del bench.extra_info["stddev_time"]


def publish_benchmark_extra_info(
    benchmark: Any, params: Any, benchmark_group: str
) -> None:
    """
    Helper function to publish benchmark parameters to the extra_info property.
    """
    benchmark.extra_info["files"] = params.files
    benchmark.extra_info["file_size"] = getattr(params, "file_size_bytes", "N/A")
    benchmark.extra_info["chunk_size"] = getattr(params, "chunk_size_bytes", "N/A")
    benchmark.extra_info["block_size"] = getattr(params, "block_size_bytes", "N/A")
    benchmark.extra_info["pattern"] = getattr(params, "pattern", "N/A")
    benchmark.extra_info["threads"] = params.threads
    benchmark.extra_info["rounds"] = params.rounds
    benchmark.extra_info["bucket_name"] = params.bucket_name
    benchmark.extra_info["bucket_type"] = params.bucket_type
    benchmark.extra_info["processes"] = params.processes
    benchmark.extra_info["depth"] = getattr(params, "depth", "N/A")
    benchmark.extra_info["folders"] = getattr(params, "folders", "N/A")

    benchmark.group = benchmark_group


def publish_resource_metrics(benchmark: Any, monitor: ResourceMonitor) -> None:
    """
    Helper function to publish resource monitor results to the extra_info property.
    """
    benchmark.extra_info.update(
        {
            "cpu_max_global": f"{monitor.max_cpu:.2f}",
            "mem_max": f"{monitor.max_mem:.2f}",
            "net_throughput_s": f"{monitor.throughput_s:.2f}",
            "vcpus": monitor.vcpus,
        }
    )


def publish_multi_process_benchmark_extra_info(
    benchmark: Any, round_durations_s: List[float], params: Any
) -> None:
    """
    Calculate statistics for multi-process benchmarks and publish them
    to extra_info.
    """
    if not round_durations_s:
        return

    min_time = min(round_durations_s)
    max_time = max(round_durations_s)
    mean_time = statistics.mean(round_durations_s)
    median_time = statistics.median(round_durations_s)
    stddev_time = (
        statistics.stdev(round_durations_s) if len(round_durations_s) > 1 else 0.0
    )

    # Build the results table as a single multi-line string to log it cleanly.
    results_table = (
        f"\n{'-' * 90}\n"
        f"{'Name (time in s)':<50s} {'Min':>8s} {'Max':>8s} {'Mean':>8s} {'Rounds':>8s}\n"
        f"{'-' * 90}\n"
        f"{params.name:<50s} {min_time:>8.4f} {max_time:>8.4f} {mean_time:>8.4f} {params.rounds:>8d}\n"
        f"{'-' * 90}"
    )
    logging.info(f"Multi-process benchmark results:{results_table}")

    benchmark.extra_info["timings"] = round_durations_s
    benchmark.extra_info["min_time"] = min_time
    benchmark.extra_info["max_time"] = max_time
    benchmark.extra_info["mean_time"] = mean_time
    benchmark.extra_info["median_time"] = median_time
    benchmark.extra_info["stddev_time"] = stddev_time
