"""WebDataset image read driver for subsystem benchmarks."""

import contextlib
import dataclasses
import os
import shutil
import tempfile
import time

from gcsfs.tests.perf.subsystembenchmarks._common import accelerator
from gcsfs.tests.perf.subsystembenchmarks.dataloading import device as device_lib
from gcsfs.tests.perf.subsystembenchmarks.dataloading.driver import (
    ReadResult,
    measure_epochs,
    run_rank_epochs_distributed,
    run_single_rank_isolated,
)
from gcsfs.tests.perf.subsystembenchmarks.dataloading.webdataset import gcsfs_opener
from gcsfs.tests.perf.subsystembenchmarks.dataloading.webdataset.imagegen import (
    SHARD_EXT,
)

_IMAGE_EXTS = ("jpg", "png", "npy")


def shard_urls(prefix, params):
    """Constructs deterministic shard URLs from prefix and file count."""
    root = prefix.rstrip("/")
    extension = SHARD_EXT[params.fmt]
    return [f"{root}/shard_{i:05d}{extension}" for i in range(params.file_count)]


def collect_sample(sample):
    """Extracts all image payloads and caption text from a sample dictionary."""
    images = [
        value
        for key, value in sample.items()
        if not key.startswith("__") and key.rsplit(".", 1)[-1].lower() in _IMAGE_EXTS
    ]
    return {"images": images, "text": sample.get("txt")}


def identity_collate(batch):
    """Returns batch unstacked to support variable image dimensions."""
    return batch


def rank_targets(expected, world_size):
    """Distributes expected sample count evenly across ranks for resampled streams."""
    base, rem = divmod(expected, world_size)
    return [base + (1 if i < rem else 0) for i in range(world_size)]


@contextlib.contextmanager
def case_cache_root(params):
    """Creates a temporary cache directory for the case, cleaned up on exit."""
    if not params.cache_dir_enabled:
        yield None
        return
    root = tempfile.mkdtemp(prefix="wds_subsystem_cache_")
    try:
        yield root
    finally:
        shutil.rmtree(root, ignore_errors=True)


def rank_cache_dir(cache_root, rank):
    """Per-rank subdirectory within cache_root, or None if caching is disabled."""
    if cache_root is None:
        return None
    path = os.path.join(cache_root, f"rank{rank}")
    os.makedirs(path, exist_ok=True)
    return path


def build_dataset(prefix, params, *, split_by_node, cache_dir=None):
    """Constructs the WebDataset pipeline for one rank."""
    import webdataset as wds

    # Register gcsfs opener for gs:// URLs in this process.
    gcsfs_opener.register()

    kwargs = dict(
        # Raise if a split receives no shards.
        empty_check=True,
        cache_dir=cache_dir,
        workersplitter=wds.split_by_worker,
        # Reseeds per epoch reproducibly.
        detshuffle=True,
        seed=0,
    )
    sample_shuffle_enabled = getattr(
        params, "sample_shuffle_enabled", params.access == "shuffled"
    )
    shard_shuffle_enabled = getattr(
        params,
        "shard_shuffle_enabled",
        sample_shuffle_enabled and getattr(params, "shard_shuffle", True),
    )

    if params.resampled:
        # Disable default node splitting and shard shuffle for resampled streams.
        kwargs["resampled"] = True
        kwargs["nodesplitter"] = None
        kwargs["shardshuffle"] = False
    else:
        kwargs["nodesplitter"] = wds.split_by_node if split_by_node else None
        kwargs["shardshuffle"] = params.file_count if shard_shuffle_enabled else False

    dataset = wds.WebDataset(shard_urls(prefix, params), **kwargs)
    if sample_shuffle_enabled:
        dataset = dataset.shuffle(params.shuffle_buffer_size)
    if params.decode:
        dataset = dataset.decode("torchrgb")
    return dataset.map(collect_sample)


def build_loader(dataset, params, pin_memory=False):
    """Constructs a PyTorch DataLoader over the WebDataset pipeline."""
    from torch.utils.data import DataLoader

    kwargs = dict(
        batch_size=params.batch_size,
        num_workers=params.num_workers,
        collate_fn=identity_collate,
        # Pins decoded image tensors for GPU copies; bytes and strings pass through.
        pin_memory=pin_memory,
    )
    if params.num_workers > 0:
        kwargs["prefetch_factor"] = params.prefetch_factor
        kwargs["persistent_workers"] = True
        # Register gcsfs opener inside spawned worker processes.
        kwargs["worker_init_fn"] = gcsfs_opener.worker_init
    return DataLoader(dataset, **kwargs)


@contextlib.contextmanager
def case_read_env(params):
    """Publish this case's read settings to the opener, then put the env back.

    The environment is the opener's only configuration channel, and a single-rank
    case runs in the caller's process rather than a child -- so left in place these
    would outlive the case and silently reconfigure whatever ran next. DataLoader
    workers are spawned inside the block and keep their own copy.
    """
    settings = {
        gcsfs_opener.READ_MODE_ENV: params.gcs_read_mode,
        gcsfs_opener.READ_CONCURRENCY_ENV: str(params.gcs_read_concurrency),
        gcsfs_opener.READ_BUFFER_ENV: str(params.read_buffer_bytes),
    }
    previous = {key: os.environ.get(key) for key in settings}
    os.environ.update(settings)
    try:
        yield
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def _set_distributed_env(rank, world_size):
    """Sets RANK and WORLD_SIZE environment variables for WebDataset rank discovery."""
    os.environ["RANK"] = str(rank)
    os.environ["WORLD_SIZE"] = str(world_size)


def run_rank_epochs(
    rank,
    world_size,
    prefix,
    params,
    barrier=None,
    cache_root=None,
    sample_count=None,
):
    """Executes measured epochs for one rank, returning (per_epoch, ttfb, build_seconds)."""
    if params.resampled and not sample_count:
        raise ValueError(
            "resampled reads need the corpus sample_count to size each rank's "
            f"budget, got {sample_count!r}; run_read passes it from the manifest"
        )
    with case_read_env(params):
        # Undecoded samples are raw bytes; there is nothing to pin or copy to a GPU.
        device = device_lib.rank_device(rank) if params.decode else None
        build_start = time.perf_counter()
        dataset = build_dataset(
            prefix,
            params,
            split_by_node=params.split_by_node,
            cache_dir=rank_cache_dir(cache_root, rank),
        )
        loader = build_loader(dataset, params, pin_memory=device_lib.pin_memory(device))
        build_seconds = time.perf_counter() - build_start

        target = None
        if params.resampled:
            target = rank_targets(sample_count, world_size)[rank]

        # Barrier synchronizes rank start times across rounds.
        per_epoch, ttfb = measure_epochs(
            device_lib.feed(loader, device),
            params.rounds,
            len,
            barrier=barrier,
            target=target,
        )
    return per_epoch, ttfb, build_seconds


def worker_entry(rank, world_size, prefix, params, barrier, extra):
    """Worker entry point invoked by ``dataloading._worker``."""
    _set_distributed_env(rank, world_size)
    extra = extra or {}
    return run_rank_epochs(
        rank,
        world_size,
        prefix,
        params,
        barrier=barrier,
        cache_root=extra.get("cache_root"),
        sample_count=extra.get("sample_count"),
    )


def run_split_epochs(prefix, params, cache_root, sample_count):
    """Runs world_size worker ranks via torchrun and aggregates epoch results."""
    # Undecoded ranks never touch an accelerator, so they skip TPU launch setup.
    return run_rank_epochs_distributed(
        "wds",
        prefix,
        params,
        extra={"cache_root": cache_root, "sample_count": sample_count},
        prepare_tpu_env=params.decode,
    )


class WebDatasetReadDriver:
    """Driver for single and multi-rank WebDataset image read benchmarks."""

    formats = ("image_tar", "image_tar_gz")

    def run_read(self, prefix, params, manifest):
        sample_count = manifest["sample_count"]
        with case_cache_root(params) as cache_root:
            if params.split_by_node:
                durations, rows, ttfb, build_seconds = run_split_epochs(
                    prefix, params, cache_root, sample_count
                )
            elif params.decode and accelerator.device_kind() == "tpu":
                # Isolate TPU device init inside a subprocess so pytest parent never locks libtpu.
                durations, rows, ttfb, build_seconds = run_single_rank_isolated(
                    "wds",
                    prefix,
                    dataclasses.replace(params, world_size=1),
                    extra={"cache_root": cache_root, "sample_count": sample_count},
                )
            else:
                per_epoch, ttfb, build_seconds = run_rank_epochs(
                    0,
                    1,
                    prefix,
                    params,
                    cache_root=cache_root,
                    sample_count=sample_count,
                )
                durations = [end - begin for begin, end, _ in per_epoch]
                rows = [count for _, _, count in per_epoch]

        return ReadResult(durations, rows, ttfb, build_seconds)
