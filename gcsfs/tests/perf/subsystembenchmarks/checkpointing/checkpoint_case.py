import logging
import time

import fsspec

from gcsfs.tests.perf.subsystembenchmarks.dataloading.driver import assert_fsspec_gcsfs


def run_checkpoint_case(benchmark, monitor, params, driver, *, bucket_ctx=None):
    """Full per-case lifecycle for CheckpointDriver."""
    from gcsfs.tests.perf.subsystembenchmarks._common.benchmark_publish import (
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

        window_start = time.time()
        with monitor() as m:
            result = driver.run_save(prefix, params)
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
        benchmark.group = params.scenario
        benchmark.extra_info.update(
            {
                "workload_implementation": params.framework,
                "workload_family": "checkpointing",
                "gcs_bucket_name": params.bucket_name,
                "bucket_type": params.bucket_type,
                "measurement_round_count": params.rounds,
                "workload_scenario": params.scenario,
                "config_sweep_axis": params.sweep_axis,
                "model_id": params.model_id,
                "checkpoint_physical_size_bytes": physical_size_bytes,
                "checkpoint_strategy": params.strategy,
                "measurement_window_start_unix_seconds": int(window_start),
                "measurement_window_end_unix_seconds": int(window_end),
            }
        )
        if result.extra_columns:
            benchmark.extra_info.update(result.extra_columns)

        durations = result.durations
        # Calculate write throughput
        benchmark.extra_info["checkpoint_write_throughput_mean_bytes_per_second"] = (
            sum(physical_size_bytes / d for d in durations) / len(durations)
            if physical_size_bytes > 0 and durations and all(durations)
            else 0.0
        )
        publish_round_stats(benchmark, durations)
        publish_resource_metrics(benchmark, m)
        benchmark.pedantic(lambda: None, rounds=1, iterations=1, warmup_rounds=0)
