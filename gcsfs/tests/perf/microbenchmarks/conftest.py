import logging
import multiprocessing
import os
import shutil
import statistics
import tempfile
import time
import uuid
from typing import Any, List

import pytest
from resource_monitor import ResourceMonitor

MB = 1024 * 1024


def _format_mb(value):
    if value == "N/A":
        return "N/A"
    return f"{float(value) / MB:.2f}"


@pytest.fixture
def gcsfs_benchmark_glob(extended_gcs_factory, request):
    """
    A fixture that sets up the environment for a glob benchmark run.
    It creates a directory structure with 0-byte files.
    """
    params = request.param
    yield from _benchmark_listing_fixture_helper(
        extended_gcs_factory, params, "benchmark-glob", teardown=True
    )


@pytest.fixture
def populate_bucket():
    return False


def _random_chunks(total_size, max_chunk=100 * MB):
    """Yield byte chunks summing to ``total_size``, each at most ``max_chunk``.

    A single 1 MiB random block is generated once and repeated to fill each
    chunk. ``os.urandom`` is CPU-bound (~200 MB/s) and otherwise dominates the
    setup time for multi-GB files; repeating a block is several times faster
    and still produces uncompressible bytes for the network/storage layer (GCS
    does not compress object uploads), so measured throughput is unaffected.
    """
    block = os.urandom(min(1 * MB, total_size))
    block_len = len(block)
    remaining = total_size
    while remaining > 0:
        write_size = min(max_chunk, remaining)
        repeats, remainder = divmod(write_size, block_len)
        yield block * repeats + block[:remainder]
        remaining -= write_size


def _write_file(gcs, path, file_size, chunk_size):
    with gcs.open(path, "wb", finalize_on_close=True) as f:
        for chunk in _random_chunks(file_size, chunk_size):
            f.write(chunk)

    actual_size = gcs.info(path)["size"]
    if actual_size != file_size:
        raise RuntimeError(
            f"Data integrity check failed for {path}. "
            f"Expected size: {file_size}, Actual size: {actual_size}"
        )


def _init_pool_worker():
    """Initializer for spawned pool workers to bypass _get_bucket_type calls on emulator."""
    from gcsfs.tests.utils import _patch_get_bucket_type_for_emulator

    patch = _patch_get_bucket_type_for_emulator()
    if patch:
        patch.start()


def _prepare_files(gcs, file_paths, file_size=0):
    if file_size == 0:
        try:
            gcs.pipe({path: b"" for path in file_paths})
            return
        except Exception as e:
            pytest.fail(f"Failed to pipe files: {e}")

    chunk_size = min(100 * MB, file_size)
    pool_size = 16

    args = [(gcs, path, file_size, chunk_size) for path in file_paths]
    ctx = multiprocessing.get_context("forkserver")
    with ctx.Pool(pool_size, initializer=_init_pool_worker) as pool:
        try:
            pool.starmap(_write_file, args)
        except RuntimeError as e:
            pytest.fail(str(e))


def _prepare_folders(gcs, folder_paths):
    for path in folder_paths:
        gcs.mkdir(path, create_parents=True)


def _write_local_file(path, file_size):
    """Create a local source file of the given size for put benchmarks."""
    with open(path, "wb") as f:
        for chunk in _random_chunks(file_size):
            f.write(chunk)


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

    try:
        if create_files:
            start_time = time.perf_counter()
            _prepare_files(gcs, file_paths, params.file_size_bytes)

            duration_ms = (time.perf_counter() - start_time) * 1000
            logging.info(
                f"Benchmark '{params.name}' setup created {params.files} files in {duration_ms:.2f} ms."
            )

        yield gcs, file_paths, params

    finally:
        # --- Teardown ---
        logging.info(f"Tearing down benchmark '{params.name}': deleting files.")
        try:
            gcs.rm(prefix, recursive=True)
        except Exception as e:
            logging.error(f"Failed to clean up benchmark files: {e!r}")


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
        gcs_kwargs={
            "block_size": params.block_size_bytes,
            "mrd_pool_cache_size": params.mrd_pool_cache_size,
        },
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
        gcs_kwargs={"block_size": params.block_size_bytes},
    )


@pytest.fixture
def gcsfs_benchmark_pipe(extended_gcs_factory, request):
    """
    A fixture that sets up the environment for a pipe benchmark run.
    It provides a GCSFS instance and a list of file paths to pipe to.
    """
    params = request.param
    yield from _benchmark_io_fixture_helper(
        extended_gcs_factory,
        params,
        "benchmark-pipe",
        create_files=False,
    )


def _benchmark_put_fixture_helper(extended_gcs_factory, params, prefix_tag):
    gcs = extended_gcs_factory()

    prefix = f"{params.bucket_name}/{prefix_tag}-{uuid.uuid4()}"
    file_paths = [f"{prefix}/file_{i}" for i in range(params.files)]

    local_dir = tempfile.mkdtemp(prefix="gcsfs-benchmark-put-")
    local_path = os.path.join(local_dir, "source")

    logging.info(
        f"Setting up benchmark '{params.name}': creating local source file of "
        f"size {params.file_size_bytes / MB:.2f} MB at '{local_path}' and "
        f"targeting {params.files} remote destination(s)."
    )

    try:
        start_time = time.perf_counter()
        _write_local_file(local_path, params.file_size_bytes)
        duration_ms = (time.perf_counter() - start_time) * 1000
        logging.info(
            f"Benchmark '{params.name}' setup created local source file in {duration_ms:.2f} ms."
        )

        # NOTE: The source file is written immediately before the benchmark and
        # is shared by every process/round, so it stays resident in the OS page
        # cache. These benchmarks therefore measure upload throughput from a
        # cached source (representative of "upload a file you just produced"),
        # not from cold disk.
        yield gcs, local_path, file_paths, params

        # Verify upload integrity outside the timed region. gcsfs defaults to
        # consistency="none" (no client-side checksum on put), so we assert the
        # uploaded object sizes match the local source. The cache is invalidated
        # first because multi-process uploads run on separate gcs instances.
        gcs.invalidate_cache()
        for path in file_paths:
            actual_size = gcs.info(path)["size"]
            if actual_size != params.file_size_bytes:
                raise RuntimeError(
                    f"Upload integrity check failed for {path}. "
                    f"Expected size: {params.file_size_bytes}, "
                    f"Actual size: {actual_size}"
                )

    finally:
        # --- Teardown ---
        logging.info(
            f"Tearing down benchmark '{params.name}': deleting remote files and local source."
        )
        try:
            gcs.rm(prefix, recursive=True)
        except Exception as e:
            logging.error(f"Failed to clean up benchmark files: {e!r}")
        shutil.rmtree(local_dir, ignore_errors=True)


@pytest.fixture
def gcsfs_benchmark_put(extended_gcs_factory, request):
    """
    A fixture that sets up the environment for a put benchmark run.
    It provides a GCSFS instance, a single local source file to upload, and a
    list of remote destination paths to upload it to.
    """
    params = request.param
    yield from _benchmark_put_fixture_helper(
        extended_gcs_factory, params, "benchmark-put"
    )


def _benchmark_listing_fixture_helper(
    extended_gcs_factory,
    params,
    prefix_tag,
    teardown=True,
    create_folders=False,
    require_file_paths=False,
):
    gcs = extended_gcs_factory()

    prefix = f"{params.bucket_name}/{prefix_tag}-{uuid.uuid4()}"

    # Deterministic folder structure generation
    target_dirs = []

    folders = getattr(params, "folders", 0)
    depth = getattr(params, "depth", 0)
    total_files = getattr(params, "files", 0)
    files_per_folder = int(total_files / folders)

    # Level 0 is the prefix itself
    levels = {0: [prefix]}

    if depth == 0:
        # Flat structure: all folders are direct children of prefix
        current_level_folders = []
        for i in range(folders):
            new_path = f"{prefix}/folder_{i}"
            target_dirs.append(new_path)
            current_level_folders.append(new_path)
        levels[1] = current_level_folders
    else:
        folders_per_level = folders // depth
        remainder = folders % depth

        for d in range(1, depth + 1):
            count = folders_per_level + (1 if d <= remainder else 0)

            parents = levels[d - 1]
            num_parents = len(parents)
            current_level_folders = []

            if num_parents > 0:
                for i in range(count):
                    parent = parents[i % num_parents]
                    new_path = f"{parent}/folder_{d}_{i}"
                    target_dirs.append(new_path)
                    current_level_folders.append(new_path)

            levels[d] = current_level_folders

    try:
        # Create empty folders first if specified
        if create_folders:
            logging.info(
                f"Setting up benchmark '{params.name}': creating {len(target_dirs)} "
                f"folders at depth {depth} with prefix '{prefix}'."
            )
            start_time = time.perf_counter()
            _prepare_folders(gcs, target_dirs)
            duration_ms = (time.perf_counter() - start_time) * 1000
            logging.info(
                f"Benchmark '{params.name}' setup created {len(target_dirs)} folders in {duration_ms:.2f} ms."
            )

        file_paths = []
        for folder in target_dirs:
            for i in range(files_per_folder):
                file_paths.append(f"{folder}/file_{i}")

        params.files = len(file_paths)

        logging.info(
            f"Setting up benchmark '{params.name}': creating {len(file_paths)} "
            f"files at depth {depth} with prefix '{prefix}' distributed across {len(target_dirs)} "
            f"folders and with {files_per_folder} files per folder."
        )

        start_time = time.perf_counter()
        _prepare_files(gcs, file_paths, getattr(params, "file_size_bytes", 0))

        duration_ms = (time.perf_counter() - start_time) * 1000
        logging.info(
            f"Benchmark '{params.name}' setup created {len(file_paths)} files in {duration_ms:.2f} ms."
        )

        if require_file_paths:
            yield gcs, target_dirs, file_paths, prefix, params
        else:
            yield gcs, target_dirs, prefix, params

    finally:
        if teardown:
            # --- Teardown ---
            logging.info(
                f"Tearing down benchmark '{params.name}': deleting files and folders."
            )
            try:
                gcs.rm(f"{prefix}*", recursive=True)
            except Exception as e:
                logging.error(f"Failed to clean up benchmark files: {e!r}")


@pytest.fixture
def gcsfs_benchmark_listing(extended_gcs_factory, request):
    """
    A fixture that sets up the environment for a listing benchmark run.
    It creates a directory structure with 0-byte files.
    """
    params = request.param
    yield from _benchmark_listing_fixture_helper(
        extended_gcs_factory, params, "benchmark-listing", teardown=True
    )


@pytest.fixture
def gcsfs_benchmark_rename(extended_gcs_factory, request):
    """
    A fixture that sets up the environment for a rename benchmark run.
    It creates a directory structure with 0-byte files.
    """
    params = request.param
    yield from _benchmark_listing_fixture_helper(
        extended_gcs_factory,
        params,
        "benchmark-rename",
        teardown=True,
        require_file_paths=True,
    )


@pytest.fixture
def gcsfs_benchmark_delete(extended_gcs_factory, request):
    """
    A fixture that sets up the environment for a delete benchmark run.
    It creates a directory structure with 0-byte files but skips teardown.
    """
    params = request.param
    yield from _benchmark_listing_fixture_helper(
        extended_gcs_factory, params, "benchmark-delete", teardown=False
    )


@pytest.fixture
def gcsfs_benchmark_info(extended_gcs_factory, request):
    """
    A fixture that sets up the environment for a info benchmark run.
    It creates a directory structure with 0-byte files.
    """
    params = request.param
    yield from _benchmark_listing_fixture_helper(
        extended_gcs_factory,
        params,
        "benchmark-info",
        teardown=True,
        create_folders=True,
        require_file_paths=True,
    )


@pytest.fixture
def gcsfs_benchmark_open(extended_gcs_factory, request):
    """
    A fixture that sets up the environment for a open benchmark run.
    It creates a directory structure with 0-byte files.
    """
    params = request.param
    yield from _benchmark_listing_fixture_helper(
        extended_gcs_factory,
        params,
        "benchmark-open",
        teardown=True,
        create_folders=True,
        require_file_paths=True,
    )


def pytest_benchmark_generate_json(config, benchmarks, machine_info, commit_info):
    """
    Hook to post-process benchmark results before generating the JSON report.
    """
    for bench in benchmarks:
        if "runs" in bench.get("extra_info", {}):
            bench.stats.data = bench.extra_info["runs"]
            bench.stats.min = bench.extra_info["min_run"]
            bench.stats.max = bench.extra_info["max_run"]
            bench.stats.mean = bench.extra_info["mean_run"]
            bench.stats.median = bench.extra_info["median_run"]
            bench.stats.stddev = bench.extra_info["stddev_run"]
            bench.stats.rounds = bench.extra_info["rounds"]

            del bench.extra_info["runs"]
            del bench.extra_info["min_run"]
            del bench.extra_info["max_run"]
            del bench.extra_info["mean_run"]
            del bench.extra_info["median_run"]
            del bench.extra_info["stddev_run"]


def publish_benchmark_extra_info(
    benchmark: Any, params: Any, benchmark_group: str
) -> None:
    """
    Helper function to publish benchmark parameters to the extra_info property.
    """
    benchmark.extra_info["files"] = params.files
    benchmark.extra_info["file_size"] = getattr(params, "file_size_bytes", "N/A")

    c_size = getattr(params, "chunk_size_bytes", 0)
    benchmark.extra_info["chunk_size"] = c_size if c_size > 0 else "N/A"

    min_c = getattr(params, "min_chunk_size_bytes", 0)
    max_c = getattr(params, "max_chunk_size_bytes", 0)
    benchmark.extra_info["min_chunk_size"] = min_c if min_c > 0 else "N/A"
    benchmark.extra_info["max_chunk_size"] = max_c if max_c > 0 else "N/A"

    prob = getattr(params, "seq_probability", None)
    benchmark.extra_info["seq_probability"] = prob if prob is not None else "N/A"

    benchmark.extra_info["block_size"] = getattr(params, "block_size_bytes", "N/A")
    benchmark.extra_info["pattern"] = getattr(params, "pattern", "N/A")
    benchmark.extra_info["runtime"] = getattr(params, "runtime", "N/A")
    benchmark.extra_info["threads"] = params.threads
    benchmark.extra_info["rounds"] = params.rounds
    benchmark.extra_info["bucket_name"] = params.bucket_name
    benchmark.extra_info["bucket_type"] = params.bucket_type
    benchmark.extra_info["processes"] = params.processes
    benchmark.extra_info["depth"] = getattr(params, "depth", "N/A")
    benchmark.extra_info["folders"] = getattr(params, "folders", "N/A")
    benchmark.extra_info["target_type"] = getattr(params, "target_type", "N/A")
    benchmark.extra_info["mrd_pool_cache_size"] = getattr(
        params, "mrd_pool_cache_size", "N/A"
    )
    benchmark.extra_info["mrd_pool_size"] = getattr(params, "mrd_pool_size", "N/A")

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


def publish_fixed_duration_benchmark_extra_info(
    benchmark: Any, total_bytes_per_round: List[int], params: Any
) -> None:
    """
    Calculate statistics for fixed duration benchmarks (total bytes)
    and publish them to extra_info.
    """
    if not total_bytes_per_round:
        return

    # Calculate statistics for total bytes read
    min_bytes = min(total_bytes_per_round)
    max_bytes = max(total_bytes_per_round)
    mean_bytes = statistics.mean(total_bytes_per_round)
    median_bytes = statistics.median(total_bytes_per_round)
    stddev_bytes = (
        statistics.stdev(total_bytes_per_round)
        if len(total_bytes_per_round) > 1
        else 0.0
    )

    # For pytest-benchmark's internal reporting, we map bytes to the 'runs' fields.
    benchmark.extra_info["runs"] = total_bytes_per_round
    benchmark.extra_info["min_run"] = min_bytes
    benchmark.extra_info["max_run"] = max_bytes
    benchmark.extra_info["mean_run"] = mean_bytes
    benchmark.extra_info["median_run"] = median_bytes
    benchmark.extra_info["stddev_run"] = stddev_bytes


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

    benchmark.extra_info["runs"] = round_durations_s
    benchmark.extra_info["min_run"] = min_time
    benchmark.extra_info["max_run"] = max_time
    benchmark.extra_info["mean_run"] = mean_time
    benchmark.extra_info["median_run"] = median_time
    benchmark.extra_info["stddev_run"] = stddev_time
