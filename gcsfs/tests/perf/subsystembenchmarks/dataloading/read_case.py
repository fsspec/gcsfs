"""Per-case benchmark lifecycle runner: bucket management, corpus ingestion, driver timing, and metric publishing."""

import statistics
import time

from gcsfs.tests.perf.subsystembenchmarks.dataloading.driver import assert_fsspec_gcsfs


def publish_common(benchmark, params, manifest, ttfb, window, build_seconds):
    """Publish shared loader parameters, system metadata, and timing metrics onto benchmark extra_info."""
    from gcsfs.tests.perf.subsystembenchmarks._common import env

    benchmark.group = params.scenario
    benchmark.extra_info.update(
        {
            "framework": params.framework,
            "workload_kind": "data_loading",
            "bucket_name": params.bucket_name,
            "bucket_type": params.bucket_type,
            "rounds": params.rounds,
            "scenario": params.scenario,
            "sweep_axis": params.sweep_axis,
            "fmt": params.fmt,
            "seq_len": params.seq_len,
            "file_count": manifest["file_count"],
            "corpus_bytes": manifest["corpus_bytes"],
            "sample_count": manifest["sample_count"],
            "batch_size": params.batch_size,
            "dataloader_num_workers": params.num_workers,
            "prefetch_factor": params.prefetch_factor,
            "access": params.access,
            "split_by_node": params.split_by_node,
            "world_size": params.world_size,
            "row_group_size": params.row_group_size,
            "time_to_first_batch": ttfb,
            "dataset_build_seconds": build_seconds,
            "window_start": int(window[0]),
            "window_end": int(window[1]),
            "backend": env.detect_backend(),
            "accelerator": env.detect_accelerator(),
            "machine_type": env.gce_machine_type(),
            "commit_sha": env.git_commit_sha(),
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
        benchmark.extra_info["dataset_read_throughput_avg_bytes_per_sec"] = (
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
