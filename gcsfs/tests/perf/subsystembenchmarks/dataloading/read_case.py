"""Per-case benchmark lifecycle runner: bucket management, corpus ingestion, driver timing, and metric publishing."""

import functools
import math
import statistics
import time

from gcsfs.tests.perf.subsystembenchmarks.dataloading.driver import assert_fsspec_gcsfs


def publish_common(benchmark, params, manifest, ttfb, window, build_seconds):
    """Publish shared loader parameters, system metadata, and timing metrics onto benchmark extra_info."""
    from gcsfs.tests.perf.subsystembenchmarks._common.benchmark_publish import (
        publish_case_metadata,
    )

    publish_case_metadata(benchmark, params, window[0], window[1])
    benchmark.extra_info.update(
        {
            "workload_family": "data_loading",
            "dataset_format": params.dataset_format,
            "dataset_file_count": manifest["file_count"],
            "dataset_size_bytes": manifest["corpus_bytes"],
            "dataset_sample_count": manifest["sample_count"],
            "batch_size_samples": params.batch_size,
            "dataloader_num_workers": params.num_workers,
            "dataloader_prefetch_factor": params.prefetch_factor,
            "read_access_pattern": params.read_access_pattern,
            "dataset_split_by_node_enabled": params.split_by_node,
            "world_size": params.world_size,
            # Publish None instead of inf so BigQuery CSV load treats it as null.
            "time_to_first_batch_seconds": ttfb if ttfb is not None and math.isfinite(ttfb) else None,
            "dataset_build_time": build_seconds,
        }
    )
    # Optional image count for multimodal corpora.
    if "image_count" in manifest:
        benchmark.extra_info["dataset_image_count"] = manifest["image_count"]
    benchmark.extra_info.update(params.extra_columns())


def run_read_case(benchmark, monitor, params, driver, *, bucket_ctx=None):
    """Full per-case lifecycle for any ReadDriver."""
    from gcsfs.tests.perf.subsystembenchmarks._common.benchmark_publish import (
        publish_resource_metrics,
        publish_round_stats,
    )
    from gcsfs.tests.perf.subsystembenchmarks.dataloading.bucket import (
        BucketSpec,
        bucket_name_of,
        case_bucket,
    )

    if params.fmt not in driver.formats:
        raise ValueError(
            f"{params.framework} driver does not support format {params.fmt!r}; "
            f"supported: {driver.formats}"
        )
    if bucket_ctx is None:
        # Only real GCS buckets require and validate environment configuration.
        bucket_ctx = functools.partial(case_bucket, BucketSpec.from_env())

    with bucket_ctx(params.name) as prefix:
        params.bucket_name = bucket_name_of(prefix)
        assert_fsspec_gcsfs(prefix)
        manifest = params.ingest(prefix)

        expected_rows = manifest["sample_count"]
        window_start = time.time()
        with monitor() as m:
            result = driver.run_read(prefix, params, manifest)
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
            if durations and all(durations)
            else 0.0
        )
        benchmark.extra_info["mean_samples_per_second"] = (
            statistics.mean(r / d for r, d in zip(result.rows_per_epoch, durations))
            if durations and all(durations)
            else 0.0
        )
        publish_round_stats(benchmark, durations)
        publish_resource_metrics(benchmark, m)
        benchmark.pedantic(lambda: None, rounds=1, iterations=1, warmup_rounds=0)
