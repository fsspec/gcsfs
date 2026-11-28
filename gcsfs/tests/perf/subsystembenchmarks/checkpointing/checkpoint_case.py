import contextlib
import logging
import time

import fsspec
import psutil

from gcsfs.tests.perf.subsystembenchmarks._common import nodes
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.driver import CheckpointDriver
from gcsfs.tests.perf.subsystembenchmarks.dataloading import rcu
from gcsfs.tests.perf.subsystembenchmarks.dataloading.driver import assert_fsspec_gcsfs


def _terminate_process_tree(
    pid: int, grace_s: float = 10.0, *, reap_root: bool = True
) -> None:
    try:
        parent = psutil.Process(pid)
        children = parent.children(recursive=True)
    except psutil.NoSuchProcess:
        return
    for proc in children + [parent]:
        try:
            proc.terminate()
        except psutil.NoSuchProcess:
            pass
    procs = children + ([parent] if reap_root else [])
    _, alive = psutil.wait_procs(procs, timeout=grace_s)
    for proc in alive:
        try:
            proc.kill()
        except psutil.NoSuchProcess:
            pass
    psutil.wait_procs(alive, timeout=grace_s)


def _run_setup_process(driver: CheckpointDriver, prefix: str, params) -> None:
    """Runs driver.setup in an isolated OS process so single-node RAM is reclaimed."""
    # Checkpoint generation uses ~400 GB of RAM. PyTorch's allocators and the gloo
    # multiprocessing backend leak memory and semaphores back to the parent process.
    # If we ran setup() natively, the leaked memory combined with the ~400 GB needed
    # for the subsequent read benchmark (driver.run) would exceed the VM's 732 GB limit
    # and cause an OOM crash. The OS forcefully reclaims all memory when this process joins.
    import multiprocessing as mp

    ctx = mp.get_context("spawn")
    p = ctx.Process(target=driver.setup, args=(prefix, params))
    p.start()
    try:
        p.join()
    finally:
        if p.is_alive():
            _terminate_process_tree(p.pid, reap_root=False)
            p.join(timeout=10.0)
            if p.is_alive():
                p.kill()
                p.join()
    if p.exitcode != 0:
        raise RuntimeError(f"driver.setup failed with exitcode {p.exitcode}")


def run_checkpoint_case(
    benchmark, monitor, params, driver: CheckpointDriver, *, bucket_ctx=None
):
    """Full per-case lifecycle for CheckpointDriver."""
    from gcsfs.tests.perf.subsystembenchmarks._common.benchmark_publish import (
        publish_case_metadata,
        publish_resource_metrics,
        publish_round_stats,
    )
    from gcsfs.tests.perf.subsystembenchmarks.dataloading.bucket import (
        BucketSpec,
        bucket_name_of,
        case_bucket,
        case_bucket_name,
    )

    node_group = nodes.NodeGroup.from_env()
    is_multinode = node_group is not None
    is_leader = node_group is None or node_group.is_leader

    if is_leader:
        spec = BucketSpec.from_env() if bucket_ctx is None else None
        ctx_fn = bucket_ctx or case_bucket
        bucket_cm = (
            ctx_fn(spec, params.name, deterministic=True)
            if is_multinode
            else ctx_fn(spec, params.name)
        )
    else:
        spec = BucketSpec.from_env()
        bucket_cm = contextlib.nullcontext(
            case_bucket_name(spec.prefix, params.name, deterministic=True)
        )

    with bucket_cm as bucket:
        # bucket_ctx may yield a raw name or a full URI depending on the branch/version.
        # Extract just the bucket name so we can safely construct our own prefix.
        bucket_name = bucket_name_of(bucket) or bucket
        params.bucket_name = bucket_name
        prefix = f"gs://{bucket_name}/checkpoint/"
        assert_fsspec_gcsfs(prefix)

        if is_multinode:
            # Multi-node setups delegate heavy work to launcher.run_torchrun (separate OS
            # subprocess tree) and must run in-parent so launcher.next_rdzv_port() stays in
            # lockstep across setup, run, and verify on every pod.
            driver.setup(prefix, params)
        else:
            _run_setup_process(driver, prefix, params)

        if is_leader and "read" in params.scenario:
            from gcsfs.tests.perf.subsystembenchmarks.dataloading import rapid_cache

            rcu.assert_storage_class(prefix, params.bucket_type)
            rapid_cache.warm_if_needed(prefix, params.bucket_type)
            rcu.warm_if_needed(prefix, params.bucket_type)

        result = None
        m = None
        window_start = 0.0
        window_end = 0.0
        try:
            if is_leader:
                window_start = time.time()
                with monitor() as m:
                    result = driver.run(prefix, params)
                window_end = time.time()
            else:
                result = driver.run(prefix, params)
            if getattr(result, "verify", None) is not None:
                result.verify()
        finally:
            if result is not None and getattr(result, "close", None) is not None:
                result.close()

        if not is_leader:
            return

        if "write" in params.scenario:
            # Saves run on *_rcu_z only, where run.py turns on RAPID writes.
            rcu.assert_storage_class(prefix, params.bucket_type)

        # Measure actual GCS files
        physical_size_bytes = 0
        try:
            fs, fs_path = fsspec.core.url_to_fs(prefix)
            logging.info("Listing GCS checkpoint prefix %s:", prefix)
            all_files = fs.find(fs_path, detail=True)
            for path, info in all_files.items():
                logging.info("  File: %s, Size: %d bytes", path, info["size"])

            # Sum up the checkpoint size
            checkpoint_files = {
                p: info for p, info in all_files.items() if driver.is_checkpoint_file(p)
            }
            physical_size_bytes = sum(
                info["size"] for info in checkpoint_files.values()
            )
            logging.info("Physical checkpoint size: %d bytes", physical_size_bytes)
        except Exception as e:
            logging.error("Could not measure physical checkpoint size: %s", e)
            raise

        result_window = getattr(result, "window", None)
        if result_window is not None:
            window_start, window_end = result_window

        # Publish metrics to pytest-benchmark extra_info
        publish_case_metadata(benchmark, params, window_start, window_end)
        benchmark.extra_info.update(
            {
                "workload_family": "checkpointing",
                "checkpoint_physical_size_bytes": physical_size_bytes,
                "checkpoint_strategy": params.strategy,
                "model_id": params.model_id,
            }
        )
        benchmark.extra_info.update(params.extra_columns())
        if result.extra_columns:
            benchmark.extra_info.update(result.extra_columns)
            if "dcp_read_bytes_fetched_total" in result.extra_columns:
                benchmark.extra_info["dcp_read_amplification_planned"] = (
                    result.extra_columns["dcp_read_bytes_fetched_total"]
                    / physical_size_bytes
                    if physical_size_bytes > 0
                    else 0.0
                )

        durations = result.durations
        bytes_transferred = getattr(result, "bytes_transferred", None)
        if bytes_transferred is not None:
            size_for_throughput = bytes_transferred
        elif "read" in params.scenario:
            size_for_throughput = physical_size_bytes * driver.read_count(params)
        else:
            size_for_throughput = physical_size_bytes

        throughput = (
            sum(size_for_throughput / d for d in durations) / len(durations)
            if size_for_throughput > 0 and durations and all(durations)
            else 0.0
        )
        if "read" in params.scenario:
            benchmark.extra_info["checkpoint_read_throughput_mean_bytes_per_second"] = (
                throughput
            )
        elif "write" in params.scenario:
            benchmark.extra_info[
                "checkpoint_write_throughput_mean_bytes_per_second"
            ] = throughput
        else:
            raise ValueError(f"Unknown scenario {params.scenario}")
        publish_round_stats(benchmark, durations)
        resource_metrics = getattr(result, "resource_metrics", None)
        publish_resource_metrics(
            benchmark, resource_metrics if resource_metrics is not None else m
        )
        benchmark.pedantic(lambda: None, rounds=1, iterations=1, warmup_rounds=0)
