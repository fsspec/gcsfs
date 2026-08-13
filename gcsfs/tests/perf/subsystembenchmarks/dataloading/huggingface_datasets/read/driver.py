"""HuggingFace datasets streaming read driver for subsystem benchmarks."""

import time

from gcsfs.tests.perf.subsystembenchmarks.dataloading.driver import (
    ReadResult,
    measure_epochs,
    spawn_rank_epochs,
)

_EXT = {"pretok_parquet": "parquet", "text_parquet": "parquet", "pretok_jsonl": "jsonl"}
_BUILDER = {
    "pretok_parquet": "parquet",
    "text_parquet": "parquet",
    "pretok_jsonl": "json",
}


def data_files(prefix, fmt):
    return f"{prefix.rstrip('/')}/shard_*.{_EXT[fmt]}"


def _rows_in_batch(batch):
    v = batch["label"]
    try:
        return int(v.shape[0])  # torch tensor / ndarray
    except AttributeError:
        return len(v)  # list


def _build_dataset(
    prefix, fmt, access, seed, *, shuffle_buffer_size=1000, max_buffer_input_shards=0
):
    """Construct a streaming HuggingFace dataset, optionally configuring shuffle."""
    import datasets

    ds = datasets.load_dataset(
        _BUILDER[fmt],
        data_files=data_files(prefix, fmt),
        split="train",
        streaming=True,
    )
    if access == "shuffled":
        kwargs = dict(seed=seed, buffer_size=shuffle_buffer_size)
        if max_buffer_input_shards:
            kwargs["max_buffer_input_shards"] = max_buffer_input_shards
        ds = ds.shuffle(**kwargs)
    return ds


def _build_loader(ds, *, batch_size, num_workers, prefetch_factor):
    """DataLoader with prefetch and persistent workers."""
    from torch.utils.data import DataLoader

    kwargs = dict(batch_size=batch_size, num_workers=num_workers)
    if num_workers > 0:
        kwargs["prefetch_factor"] = prefetch_factor
        kwargs["persistent_workers"] = True
    return DataLoader(ds, **kwargs)


def run_epochs(
    *,
    prefix,
    fmt,
    access,
    num_workers,
    batch_size,
    prefetch_factor=2,
    rounds=1,
    seed=0,
    shuffle_buffer_size=1000,
    max_buffer_input_shards=0,
):
    """Iterate a persistent DataLoader over dataset for `rounds` epochs.

    Returns (durations, rows_list, ttfb, build_seconds).
    """
    import datasets  # noqa: F401

    build_start = time.perf_counter()
    ds = _build_dataset(
        prefix,
        fmt,
        access,
        seed,
        shuffle_buffer_size=shuffle_buffer_size,
        max_buffer_input_shards=max_buffer_input_shards,
    ).with_format("torch")
    build_seconds = time.perf_counter() - build_start
    loader = _build_loader(
        ds,
        batch_size=batch_size,
        num_workers=num_workers,
        prefetch_factor=prefetch_factor,
    )
    # Reseed shuffle buffer per epoch so workers iterate different orders.
    per_epoch, ttfb = measure_epochs(
        loader,
        rounds,
        _rows_in_batch,
        on_epoch=ds.set_epoch,
    )
    durations = [end - begin for begin, end, _ in per_epoch]
    rows = [count for _, _, count in per_epoch]
    return durations, rows, ttfb, build_seconds


def _split(ds, rank, world_size):
    from datasets.distributed import split_dataset_by_node

    return split_dataset_by_node(ds, rank=rank, world_size=world_size)


def run_rank_epochs(rank, world_size, prefix, params, barrier=None):
    """Run persistent split DataLoader for a single rank across `rounds` epochs.

    Returns (per_epoch_timestamps, ttfb, build_seconds).
    """
    import datasets  # noqa: F401

    build_start = time.perf_counter()
    ds = _build_dataset(
        prefix,
        params.fmt,
        params.access,
        seed=0,
        shuffle_buffer_size=params.shuffle_buffer_size,
        max_buffer_input_shards=params.max_buffer_input_shards,
    )
    ds = _split(ds, rank, world_size).with_format("torch")
    build_seconds = time.perf_counter() - build_start
    loader = _build_loader(
        ds,
        batch_size=params.batch_size,
        num_workers=params.num_workers,
        prefetch_factor=params.prefetch_factor,
    )
    # Barrier absorbs dataset construction skew before round 1.
    per_epoch, ttfb = measure_epochs(
        loader,
        params.rounds,
        _rows_in_batch,
        barrier=barrier,
        on_epoch=ds.set_epoch,
    )
    return per_epoch, ttfb, build_seconds


def _rank_entry(rank, world_size, prefix, params, barrier, queue):
    queue.put(run_rank_epochs(rank, world_size, prefix, params, barrier))


def run_split_epochs(prefix, params):
    """Spawn `world_size` ranks running `run_rank_epochs` and reduce results across ranks."""
    return spawn_rank_epochs(_rank_entry, prefix, params)


class HFReadDriver:
    """Driver for single and multi-rank HuggingFace streaming read benchmarks."""

    formats = ("pretok_parquet", "text_parquet", "pretok_jsonl")

    def run_read(self, prefix, params, manifest):
        del manifest  # HuggingFace reads full shards rather than a sample budget.
        if params.split_by_node:
            durations, rows_list, ttfb, build_seconds = run_split_epochs(prefix, params)
        else:
            durations, rows_list, ttfb, build_seconds = run_epochs(
                prefix=prefix,
                fmt=params.fmt,
                access=params.access,
                num_workers=params.num_workers,
                batch_size=params.batch_size,
                prefetch_factor=params.prefetch_factor,
                rounds=params.rounds,
                shuffle_buffer_size=params.shuffle_buffer_size,
                max_buffer_input_shards=params.max_buffer_input_shards,
            )
        return ReadResult(durations, rows_list, ttfb, build_seconds)
