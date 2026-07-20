"""HuggingFace datasets streaming read driver for subsystem benchmarks."""

import time

from gcsfs.tests.perf.subsystembenchmarks.dataloading.driver import (
    ReadResult,
    reduce_split,
    timestamp,
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
    durations, rows_list = [], []
    ttfb = None
    for epoch in range(rounds):
        # Reseed shuffle buffer per epoch so persistent workers iterate different orders.
        ds.set_epoch(epoch)
        rows = 0
        begin = time.perf_counter()
        for batch in loader:
            if ttfb is None:
                ttfb = time.perf_counter() - begin
            rows += _rows_in_batch(batch)
        durations.append(time.perf_counter() - begin)
        rows_list.append(rows)
    return (
        durations,
        rows_list,
        (ttfb if ttfb is not None else durations[0]),
        build_seconds,
    )


def _split(ds, rank, world_size):
    from datasets.distributed import split_dataset_by_node

    return split_dataset_by_node(ds, rank=rank, world_size=world_size)


def rank_rows(
    prefix,
    fmt,
    rank,
    world_size,
    seed=0,
    *,
    access="sequential",
    max_buffer_input_shards=0,
):
    """Rows one rank yields after split_dataset_by_node."""
    ds = _build_dataset(
        prefix,
        fmt,
        access,
        seed,
        max_buffer_input_shards=max_buffer_input_shards,
    )
    ds = _split(ds, rank, world_size).with_format("torch")
    return sum(1 for _ in ds)


def run_rank_epochs(rank, world_size, prefix, params):
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
    per_epoch, ttfb = [], None
    for epoch in range(params.rounds):
        ds.set_epoch(epoch)
        rows = 0
        begin = timestamp()
        for batch in loader:
            if ttfb is None:
                ttfb = timestamp() - begin
            rows += _rows_in_batch(batch)
        end = timestamp()
        per_epoch.append((begin, end, rows))
    default_ttfb = per_epoch[0][1] - per_epoch[0][0]
    return per_epoch, (ttfb if ttfb is not None else default_ttfb), build_seconds


def _rank_entry(rank, world_size, prefix, params, q):
    per_epoch, ttfb, build_seconds = run_rank_epochs(rank, world_size, prefix, params)
    q.put((per_epoch, ttfb, build_seconds))


def run_split_epochs(prefix, params):
    """Spawn `world_size` ranks running `run_rank_epochs` and reduce results across ranks."""
    import torch.multiprocessing as mp

    ctx = mp.get_context("spawn")
    with ctx.Manager() as manager:
        q = manager.Queue()
        mp.spawn(
            _rank_entry,
            args=(params.world_size, prefix, params, q),
            nprocs=params.world_size,
            join=True,
        )
        results = [q.get() for _ in range(params.world_size)]
    durations, rows_list, ttfb = reduce_split(results, params.rounds)
    # Build time is bounded by the slowest rank.
    build_seconds = max(r[2] for r in results)
    return durations, rows_list, ttfb, build_seconds


class HFReadDriver:
    """Driver for single and multi-rank HuggingFace streaming read benchmarks."""

    formats = ("pretok_parquet", "text_parquet", "pretok_jsonl")

    def run_read(self, prefix, params):
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
