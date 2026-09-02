"""Ray Data streaming read driver for subsystem benchmarks."""

import logging
import time
from concurrent.futures import ThreadPoolExecutor

import fsspec
import pyarrow.fs
import ray
import torch

from gcsfs.tests.perf.subsystembenchmarks.dataloading.driver import (
    ReadResult,
    assert_fsspec_gcsfs,
    assert_gcsfs_backed,
    reduce_split,
    timestamp,
)


def ensure_ray_initialized():
    """Initializes local Ray cluster on CPU if not already running."""
    if not ray.is_initialized():
        ray.init(
            include_dashboard=False,
            ignore_reinit_error=True,
            logging_level=logging.WARNING,
        )
    ctx = ray.data.DataContext.get_current()
    ctx.enable_progress_bars = False
    # Set 64 MiB target block size to keep blocks streaming and avoid disk spilling.
    ctx.target_max_block_size = 64 * 1024 * 1024


def resolve_parquet_source(prefix):
    """Resolves Parquet file paths and wraps filesystem in PyArrow PyFileSystem."""
    fs, base_path = fsspec.core.url_to_fs(prefix)
    if str(prefix).startswith("gs://"):
        assert_fsspec_gcsfs(prefix)
    paths = sorted(fs.glob(f"{base_path.rstrip('/')}/*.parquet"))
    if not paths:
        raise FileNotFoundError(f"no Parquet files found under {prefix}")
    arrow_fs = pyarrow.fs.PyFileSystem(pyarrow.fs.FSSpecHandler(fs))
    if str(prefix).startswith("gs://"):
        assert_gcsfs_backed(arrow_fs)
    return arrow_fs, paths


def build_dataset(arrow_fs, paths, params):
    """Constructs a Ray Data Dataset backed by PyArrow and gcsfs."""
    file_shuffle = None
    if params.access == "shuffled" and params.file_shuffle:
        file_shuffle = ray.data.FileShuffleConfig(seed=42, reseed_after_execution=True)

    columns = (
        ["tokens", "label"] if params.fmt == "pretok_parquet" else ["text", "label"]
    )
    concurrency = (
        min(params.num_workers, len(paths)) if params.num_workers > 0 else None
    )

    ds = ray.data.read_parquet(
        paths,
        filesystem=arrow_fs,
        shuffle=file_shuffle,
        concurrency=concurrency,
    )
    if columns:
        ds = ds.select_columns(columns)
    return ds


class _TextParquetCollateFn(ray.data.collate_fn.NumpyBatchCollateFn):
    """Collator for text_parquet that preserves string text as a list without tensor error."""

    def __call__(self, batch):
        return {
            "text": list(batch["text"]),
            "label": torch.as_tensor(batch["label"]),
        }


def _rows_in_batch(batch):
    v = batch.get("label")
    if v is None:
        v = next(iter(batch.values()))
    if hasattr(v, "shape"):
        return int(v.shape[0])
    if isinstance(v, list) and len(v) > 0 and hasattr(v[0], "shape"):
        return sum(int(x.shape[0]) for x in v)
    return len(v)


def run_single_rank(dataset, params):
    """Iterates dataset on single consumer rank across rounds."""
    per_epoch = []
    ttfb = None
    collate_fn = _TextParquetCollateFn() if params.fmt == "text_parquet" else None
    for epoch in range(params.rounds):
        begin = timestamp()
        rows = 0
        shuffle_seed = (
            42 + epoch
            if params.access == "shuffled" and params.shuffle_buffer_size > 0
            else None
        )
        batch_iter = iter(
            dataset.iter_torch_batches(
                batch_size=params.batch_size,
                prefetch_batches=params.prefetch_factor,
                local_shuffle_buffer_size=(
                    params.shuffle_buffer_size
                    if params.access == "shuffled" and params.shuffle_buffer_size > 0
                    else None
                ),
                local_shuffle_seed=shuffle_seed,
                collate_fn=collate_fn,
                device=torch.device("cpu"),
                pin_memory=False,
                drop_last=False,
            )
        )
        for batch in batch_iter:
            if epoch == 0 and ttfb is None:
                ttfb = timestamp() - begin
            rows += _rows_in_batch(batch)
        per_epoch.append((begin, timestamp(), rows))
    durations = [end - begin for begin, end, _ in per_epoch]
    rows_list = [r for _, _, r in per_epoch]
    return durations, rows_list, (ttfb if ttfb is not None else float("inf"))


def run_multi_rank(dataset, params):
    """Iterates dataset split equally across world_size consumer ranks concurrently."""
    world_size = params.world_size
    epoch_records = []
    collate_fn = _TextParquetCollateFn() if params.fmt == "text_parquet" else None
    for epoch in range(params.rounds):
        splits = dataset.streaming_split(n=world_size, equal=False)
        shuffle_seed = (
            42 + epoch
            if params.access == "shuffled" and params.shuffle_buffer_size > 0
            else None
        )

        def consume(shard):
            begin = timestamp()
            ttfb = None
            rows = 0
            batch_iter = iter(
                shard.iter_torch_batches(
                    batch_size=params.batch_size,
                    prefetch_batches=params.prefetch_factor,
                    local_shuffle_buffer_size=(
                        params.shuffle_buffer_size
                        if params.access == "shuffled"
                        and params.shuffle_buffer_size > 0
                        else None
                    ),
                    local_shuffle_seed=shuffle_seed,
                    collate_fn=collate_fn,
                    device=torch.device("cpu"),
                    pin_memory=False,
                    drop_last=False,
                )
            )
            for batch in batch_iter:
                if ttfb is None:
                    ttfb = timestamp() - begin
                rows += _rows_in_batch(batch)
            return (
                begin,
                timestamp(),
                rows,
                (ttfb if ttfb is not None else float("inf")),
            )

        with ThreadPoolExecutor(max_workers=world_size) as pool:
            rank_records = list(pool.map(consume, splits))
        epoch_records.append(rank_records)

    results = []
    for r in range(world_size):
        per_epoch = [
            (epoch_records[e][r][0], epoch_records[e][r][1], epoch_records[e][r][2])
            for e in range(params.rounds)
        ]
        rank_ttfb = epoch_records[0][r][3]
        results.append((per_epoch, rank_ttfb))

    return reduce_split(results, params.rounds)


class RayDataReadDriver:
    """Driver for single and multi-rank Ray Data streaming read benchmarks."""

    formats = ("pretok_parquet", "text_parquet")

    def run_read(self, prefix, params, manifest):
        del manifest
        ensure_ray_initialized()
        arrow_fs, paths = resolve_parquet_source(prefix)

        build_start = time.perf_counter()
        dataset = build_dataset(arrow_fs, paths, params)
        build_seconds = time.perf_counter() - build_start

        if params.split_by_node and params.world_size > 1:
            durations, rows_list, ttfb = run_multi_rank(dataset, params)
        else:
            durations, rows_list, ttfb = run_single_rank(dataset, params)

        return ReadResult(durations, rows_list, ttfb, build_seconds)
