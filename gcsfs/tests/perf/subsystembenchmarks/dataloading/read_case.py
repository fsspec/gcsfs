"""Per-case benchmark lifecycle runner: bucket management, corpus ingestion, driver timing, and metric publishing."""

import os
import statistics
import time

from gcsfs.tests.perf.subsystembenchmarks.dataloading.driver import assert_fsspec_gcsfs


def publish_common(benchmark, params, manifest, ttfb, window, build_seconds):
    """Publish shared loader parameters, system metadata, and timing metrics onto benchmark extra_info."""
    from gcsfs.tests.perf.subsystembenchmarks._common import env
    from gcsfs.tests.perf.subsystembenchmarks._common.config_loader import (
        requested_sweep_axes,
    )

    sweep_axes = " ".join(requested_sweep_axes()) or "all"
    benchmark.group = params.scenario
    benchmark.extra_info.update(
        {
            "workload_implementation": params.framework,
            "workload_family": "data_loading",
            "gcs_bucket_name": params.bucket_name,
            "bucket_type": params.bucket_type,
            "measurement_round_count": params.rounds,
            "workload_scenario": params.scenario,
            "config_sweep_axis": params.sweep_axis,
            "dataset_format": params.fmt,
            "sample_sequence_length_tokens": params.seq_len,
            "dataset_file_count": manifest["file_count"],
            "dataset_size_bytes": manifest["corpus_bytes"],
            "dataset_sample_count": manifest["sample_count"],
            "batch_size_samples": params.batch_size,
            "dataloader_num_workers": params.num_workers,
            "dataloader_prefetch_factor": params.prefetch_factor,
            "read_access_pattern": params.access,
            "dataset_split_by_node_enabled": params.split_by_node,
            "world_size": params.world_size,
            "parquet_row_group_size_rows": params.row_group_size,
            "time_to_first_batch_seconds": ttfb,
            "dataset_build_time": build_seconds,
            "measurement_window_start_unix_seconds": int(window[0]),
            "measurement_window_end_unix_seconds": int(window[1]),
            "distributed_backend": env.detect_backend(),
            "compute_accelerator_type": env.detect_accelerator(),
            "machine_type": env.machine_type(),
            "benchmark_source_commit_sha": env.benchmark_source_commit_sha(),
            "requirements_override": os.environ.get(
                "GCSFS_SUBSYSTEM_REQUIREMENTS_OVERRIDE", ""
            ),
            "requirements_resolved": os.environ.get(
                "GCSFS_SUBSYSTEM_REQUIREMENTS_RESOLVED", "[]"
            ),
            "config_sweep_axes_requested": sweep_axes,
        }
    )
    benchmark.extra_info.update(params.extra_columns())


def run_read_case(benchmark, monitor, params, driver, *, bucket_ctx=None):
    """Full per-case lifecycle for any ReadDriver."""
    from gcsfs.tests.perf.subsystembenchmarks._common.benchmark_publish import (
        publish_resource_metrics,
        publish_round_stats,
    )
    from gcsfs.tests.perf.subsystembenchmarks.dataloading import datagen
    from gcsfs.tests.perf.subsystembenchmarks.dataloading.bucket import (
        BucketSpec,
        case_bucket,
    )

    if params.fmt not in driver.formats:
        raise ValueError(
            f"{params.framework} driver does not support format {params.fmt!r}; "
            f"supported: {driver.formats}"
        )
    bucket_ctx = bucket_ctx or case_bucket

    with bucket_ctx(BucketSpec.from_env(), params.name) as bucket:
        params.bucket_name = bucket
        prefix = f"gs://{bucket}/data/"
        assert_fsspec_gcsfs(prefix)
        manifest = datagen.ingest_dataset(
            prefix,
            fmt=params.fmt,
            seq_len=params.seq_len,
            file_count=params.file_count,
            rows_per_file=params.rows_per_file,
            row_group_size=params.row_group_size,
        )

        expected_rows = manifest["sample_count"]
        window_start = time.time()
        with monitor() as m:
            result = driver.run_read(prefix, params)
        window_end = time.time()

        for rows in result.rows_per_epoch:
            if rows != expected_rows:
                raise ValueError(
                    f"partial read: got {rows} rows, expected {expected_rows}"
                )
        publish_common(
            benchmark,
            params,
            manifest,
            result.ttfb_seconds,
            (window_start, window_end),
            result.build_seconds,
        )
        benchmark.extra_info.update(result.extra_columns)
        durations = result.durations
        benchmark.extra_info["dataset_read_throughput_mean_bytes_per_second"] = (
            statistics.mean(manifest["corpus_bytes"] / d for d in durations)
            if all(durations)
            else 0.0
        )
        benchmark.extra_info["mean_samples_per_second"] = (
            statistics.mean(r / d for r, d in zip(result.rows_per_epoch, durations))
            if all(durations)
            else 0.0
        )
        publish_round_stats(benchmark, durations)
        publish_resource_metrics(benchmark, m)
        benchmark.pedantic(lambda: None, rounds=1, iterations=1, warmup_rounds=0)
