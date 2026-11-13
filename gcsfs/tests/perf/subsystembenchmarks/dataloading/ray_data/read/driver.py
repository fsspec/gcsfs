"""Ray Data streaming read driver for subsystem benchmarks."""

import logging
import time

import fsspec
import pyarrow.fs
import ray
import torch

import gcsfs
from gcsfs.tests.perf.subsystembenchmarks.dataloading import device as device_lib
from gcsfs.tests.perf.subsystembenchmarks.dataloading.driver import (
    ReadResult,
    assert_fsspec_gcsfs,
    assert_gcsfs_backed,
    reduce_split,
    timestamp,
)


class _GCSFileSystemWithCacheType(gcsfs.GCSFileSystem):
    """gcsfs whose reads default to ``cache_type``.

    pyarrow's FSSpecHandler opens files with no cache_type, and gcsfs has no
    filesystem-level default for it. Taking it as a constructor argument keeps it when
    Ray pickles the filesystem to its workers.
    """

    def __init__(self, *args, cache_type=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.read_cache_type = cache_type

    def _open(self, path, mode="rb", **kwargs):
        if "r" in mode and self.read_cache_type is not None:
            kwargs.setdefault("cache_type", self.read_cache_type)
        return super()._open(path, mode, **kwargs)


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


def shutdown_ray():
    """Tears down the local Ray cluster so workers and object store memory are released."""
    if ray.is_initialized():
        ray.shutdown()


def resolve_parquet_source(prefix, cache_type=None, block_size=None):
    """Resolves Parquet file paths and wraps filesystem in PyArrow PyFileSystem.

    ``cache_type`` and ``block_size`` set gcsfs's defaults for every file Ray opens;
    None keeps gcsfs's own.
    """
    fs, base_path = fsspec.core.url_to_fs(prefix)
    if str(prefix).startswith("gs://"):
        assert_fsspec_gcsfs(prefix)
    paths = sorted(fs.glob(f"{base_path.rstrip('/')}/*.parquet"))
    if not paths:
        raise FileNotFoundError(f"no Parquet files found under {prefix}")
    if cache_type is not None or block_size is not None:
        if not isinstance(fs, gcsfs.GCSFileSystem):
            raise ValueError(
                f"cache_type/block_size need gcsfs, got {type(fs).__name__}"
            )
        options = dict(fs.storage_options, cache_type=cache_type, block_size=block_size)
        fs = _GCSFileSystemWithCacheType(**options)
    arrow_fs = pyarrow.fs.PyFileSystem(pyarrow.fs.FSSpecHandler(fs))
    if str(prefix).startswith("gs://"):
        assert_gcsfs_backed(arrow_fs)
    return arrow_fs, paths


def build_dataset(arrow_fs, paths, params):
    """Constructs a Ray Data Dataset backed by PyArrow and gcsfs."""
    file_shuffle = None
    if params.access == "shuffled" and params.file_shuffle:
        file_shuffle = ray.data.FileShuffleConfig(seed=42, reseed_after_execution=True)

    concurrency = (
        min(params.num_workers, len(paths)) if params.num_workers > 0 else None
    )

    ds = ray.data.read_parquet(
        paths,
        filesystem=arrow_fs,
        shuffle=file_shuffle,
        concurrency=concurrency,
    )
    # Ray pushes select_columns down into the Parquet scan (read_parquet's own
    # columns= is deprecated), so only the selected column chunks are fetched.
    columns = params.read_columns()
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
    if hasattr(batch, "num_rows"):  # pyarrow batches (feature columns)
        return int(batch.num_rows)
    v = batch.get("label")
    if v is None:
        v = next(iter(batch.values()))
    if hasattr(v, "shape"):
        return int(v.shape[0])
    return len(v)


def _torch_batch_kwargs(params, shuffle_seed, collate_fn, device):
    """iter_torch_batches arguments; on GPU, Ray copies (and pins) natively."""
    shuffled = params.access == "shuffled" and params.shuffle_buffer_size > 0
    return dict(
        batch_size=params.batch_size,
        prefetch_batches=params.prefetch_factor,
        local_shuffle_buffer_size=params.shuffle_buffer_size if shuffled else None,
        local_shuffle_seed=shuffle_seed,
        collate_fn=collate_fn,
        device=device if device is not None else torch.device("cpu"),
        # Ray pins only pure-tensor batches, i.e. without a custom collate.
        pin_memory=device is not None and collate_fn is None,
        drop_last=False,
    )


def _iter_batches(source, params, shuffle_seed, collate_fn, device):
    """Batches from a Dataset or streaming split, delivered to ``device``.

    ``feed`` adds the per-round sync and moves tensors Ray leaves on the host
    (text_parquet's custom collate); already-moved tensors are untouched.

    With feature columns, batches stay pyarrow tables on the host: the case
    measures the projected read, and binary columns have no tensor form.
    """
    if params.has_features:
        shuffled = params.access == "shuffled" and params.shuffle_buffer_size > 0
        return source.iter_batches(
            batch_size=params.batch_size,
            batch_format="pyarrow",
            prefetch_batches=params.prefetch_factor,
            local_shuffle_buffer_size=params.shuffle_buffer_size if shuffled else None,
            local_shuffle_seed=shuffle_seed,
            drop_last=False,
        )
    batches = source.iter_torch_batches(
        **_torch_batch_kwargs(params, shuffle_seed, collate_fn, device)
    )
    return device_lib.feed(batches, device)


def run_single_rank(dataset, params):
    """Iterates dataset on single consumer rank across rounds."""
    device = None if params.has_features else device_lib.rank_device(0)
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
        for batch in _iter_batches(dataset, params, shuffle_seed, collate_fn, device):
            if epoch == 0 and ttfb is None:
                ttfb = timestamp() - begin
            rows += _rows_in_batch(batch)
        per_epoch.append((begin, timestamp(), rows))
    durations = [end - begin for begin, end, _ in per_epoch]
    rows_list = [r for _, _, r in per_epoch]
    return durations, rows_list, (ttfb if ttfb is not None else float("inf"))


@ray.remote(num_cpus=1)
def _consume_shard(shard, params, shuffle_seed, collate_fn):
    """Consumes one streaming split, returning (first_batch, end, rows) timestamps."""
    first_batch = None
    rows = 0
    # Ray sets CUDA_VISIBLE_DEVICES from num_gpus, so rank 0 is this task's GPU.
    # Feature-column batches stay on the host, so those cases take no GPU.
    device = None if params.has_features else device_lib.rank_device(0)
    for batch in _iter_batches(shard, params, shuffle_seed, collate_fn, device):
        if first_batch is None:
            first_batch = timestamp()
        rows += _rows_in_batch(batch)
    return first_batch, timestamp(), rows


def run_multi_rank(dataset, params):
    """Iterates dataset split equally across world_size consumer ranks concurrently."""
    world_size = params.world_size
    epoch_records = []
    collate_fn = _TextParquetCollateFn() if params.fmt == "text_parquet" else None
    num_gpus = 0 if params.has_features else device_lib.ray_task_num_gpus(world_size)
    for epoch in range(params.rounds):
        splits = dataset.streaming_split(n=world_size, equal=False)
        shuffle_seed = (
            42 + epoch
            if params.access == "shuffled" and params.shuffle_buffer_size > 0
            else None
        )

        # Start the clock before dispatch so the time Ray spends scheduling the
        # consumer tasks is charged to the epoch instead of being invisible.
        begin = timestamp()
        futures = [
            _consume_shard.options(num_gpus=num_gpus).remote(
                split, params, shuffle_seed, collate_fn
            )
            for split in splits
        ]
        epoch_records.append(
            [
                (
                    begin,
                    end,
                    rows,
                    (first_batch - begin) if first_batch is not None else float("inf"),
                )
                for first_batch, end, rows in ray.get(futures)
            ]
        )

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

        build_start = time.perf_counter()
        arrow_fs, paths = resolve_parquet_source(
            prefix, params.cache_type, params.block_size
        )
        dataset = build_dataset(arrow_fs, paths, params)
        build_seconds = time.perf_counter() - build_start

        if params.split_by_node and params.world_size > 1:
            durations, rows_list, ttfb = run_multi_rank(dataset, params)
        else:
            durations, rows_list, ttfb = run_single_rank(dataset, params)

        return ReadResult(durations, rows_list, ttfb, build_seconds)
