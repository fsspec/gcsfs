"""WebDataset image read driver for subsystem benchmarks."""

import contextlib
import functools
import os
import shutil
import tempfile
import time

from gcsfs.tests.perf.subsystembenchmarks.dataloading import device as device_lib
from gcsfs.tests.perf.subsystembenchmarks.dataloading.driver import (
    ReadResult,
    measure_epochs,
    spawn_rank_epochs,
)
from gcsfs.tests.perf.subsystembenchmarks.dataloading.webdataset import gcsfs_opener
from gcsfs.tests.perf.subsystembenchmarks.dataloading.webdataset.imagegen import (
    SHARD_EXT,
)

_IMAGE_EXTS = ("jpg", "png", "npy")

# Seconds a rank waits for its DataLoader workers to finish start-up.
WORKER_STARTUP_TIMEOUT_SECONDS = 600

try:
    from torch.utils.data import IterableDataset as _IterableBase
except ImportError:  # Torch-less tooling (e.g. case listing) never builds loaders.
    _IterableBase = object


class _GatedDataset(_IterableBase):
    """Holds each DataLoader worker at the start line until the first timed round.

    On its first ``__iter__`` a worker primes gcsfs, signals ``ready`` and blocks on
    ``go``; the rank starts the clock and sets ``go`` together. Worker spawn,
    imports and auth/connection set-up therefore land in dataset build time rather
    than in round 1, which (unlike later rounds of persistent workers) would
    otherwise carry seconds of start-up unrelated to storage.
    """

    def __init__(self, dataset, go, ready, prime):
        self.dataset = dataset
        self.go = go
        self.ready = ready
        self.prime = prime
        self._primed = False

    def __iter__(self):
        if not self._primed:
            self.prime()
            self._primed = True
            self.ready.release()
        self.go.wait()
        yield from self.dataset


class _StartedLoader:
    """Iterable whose first round reuses the pre-started iterator and opens the gate."""

    def __init__(self, loader, iterator, go):
        self._loader = loader
        self._iterator = iterator
        self._go = go

    def __iter__(self):
        if self._iterator is None:
            return iter(self._loader)
        iterator, self._iterator = self._iterator, None
        self._go.set()
        return iterator


def _start_workers(loader, go, ready, num_workers):
    """Spawn ``loader``'s workers and wait until each has primed and parked at ``go``.

    Creating the iterator hands every worker a prefetch request, which runs the
    gated ``__iter__``: prime, signal ``ready``, block. No object bytes are read
    until the returned iterable's first round sets ``go``.
    """
    iterator = iter(loader)
    for _ in range(num_workers):
        if not ready.acquire(timeout=WORKER_STARTUP_TIMEOUT_SECONDS):
            raise TimeoutError(
                f"DataLoader workers not ready after {WORKER_STARTUP_TIMEOUT_SECONDS}s"
            )
    return _StartedLoader(loader, iterator, go)


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
    if params.resampled:
        # Disable default node splitting and shard shuffle for resampled streams.
        kwargs["resampled"] = True
        kwargs["nodesplitter"] = None
        kwargs["shardshuffle"] = False
    else:
        kwargs["nodesplitter"] = wds.split_by_node if split_by_node else None
        kwargs["shardshuffle"] = (
            params.file_count if params.shard_shuffle_enabled else False
        )

    dataset = wds.WebDataset(shard_urls(prefix, params), **kwargs)
    if params.sample_shuffle_enabled:
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
        gate = None
        if params.num_workers > 0:
            import torch.multiprocessing as torch_mp

            gate = torch_mp.Event(), torch_mp.Semaphore(0)
            dataset = _GatedDataset(
                dataset,
                *gate,
                functools.partial(gcsfs_opener.prime, shard_urls(prefix, params)[0]),
            )
        loader = build_loader(dataset, params, pin_memory=device is not None)
        if gate is not None:
            loader = _start_workers(loader, *gate, params.num_workers)
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


def _rank_entry(rank, world_size, prefix, params, barrier, queue, cache_root, samples):
    _set_distributed_env(rank, world_size)
    queue.put(
        run_rank_epochs(rank, world_size, prefix, params, barrier, cache_root, samples)
    )


def run_split_epochs(prefix, params, cache_root, sample_count):
    """Spawns world_size worker processes and aggregates epoch results."""
    return spawn_rank_epochs(_rank_entry, prefix, params, cache_root, sample_count)


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
