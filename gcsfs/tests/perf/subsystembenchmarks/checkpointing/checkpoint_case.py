import logging
import time

import fsspec

from gcsfs.tests.perf.subsystembenchmarks.checkpointing.driver import CheckpointDriver
from gcsfs.tests.perf.subsystembenchmarks.dataloading.driver import assert_fsspec_gcsfs


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
        case_bucket,
    )

    bucket_ctx = bucket_ctx or case_bucket

    with bucket_ctx(BucketSpec.from_env(), params.name) as bucket:
        params.bucket_name = bucket
        prefix = f"gs://{bucket}/checkpoint/"
        assert_fsspec_gcsfs(prefix)

        # We run the setup phase (which generates the checkpoint) in an isolated OS process.
        # Checkpoint generation uses ~400 GB of RAM. PyTorch's allocators and the gloo
        # multiprocessing backend leak memory and semaphores back to the parent process.
        # If we ran setup() natively, the leaked memory combined with the ~400 GB needed
        # for the subsequent read benchmark (driver.run) would exceed the VM's 732 GB limit
        # and cause an OOM crash. The OS forcefully reclaims all memory when this process joins.
        import multiprocessing as mp
        ctx = mp.get_context("spawn")
        p = ctx.Process(target=driver.setup, args=(prefix, params))
        p.start()
        p.join()
        if p.exitcode != 0:
            raise RuntimeError(f"driver.setup failed with exitcode {p.exitcode}")

        window_start = time.time()
        with monitor() as m:
            result = driver.run(prefix, params)
        window_end = time.time()

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
                p: info for p, info in all_files.items() if "model.ckpt" in p
            }
            physical_size_bytes = sum(
                info["size"] for info in checkpoint_files.values()
            )
            logging.info("Physical checkpoint size: %d bytes", physical_size_bytes)
        except Exception as e:
            logging.error("Could not measure physical checkpoint size: %s", e)
            raise

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

        durations = result.durations
        size_for_throughput = physical_size_bytes

        # In DDP, FSDP full and Model Parallel full, all ranks read the monolithic checkpoint independently.
        # The effective data transferred from storage is multiplied by the world size.
        if "read" in params.scenario and params.strategy in (
            "ddp",
            "fsdp_full",
            "model_parallel_full",
        ):
            size_for_throughput *= params.world_size

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
        publish_resource_metrics(benchmark, m)
        benchmark.pedantic(lambda: None, rounds=1, iterations=1, warmup_rounds=0)
