"""Rank and driver plumbing shared by the DCP load and save drivers."""

import contextlib
import functools
import json
import os
import tempfile
import time

import fsspec
import torch
import torch.distributed as dist

from gcsfs.tests.perf.subsystembenchmarks.checkpointing._dist import (
    setup_distributed_env,
    spawn_split,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.driver import CheckpointDriver
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.device import (
    rank_device,
    run_device,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.model import (
    build,
    build_model,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.parallelize import (
    check_model_layout,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.reference import (
    stage_model,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.state import (
    get_fqn_state_dict,
)


@contextlib.contextmanager
def rank_session(rank, world_size, port):
    """Joins the process group and yields this rank's torch.device."""
    device_kind = run_device()
    setup_distributed_env(rank, world_size, port, device=device_kind)
    try:
        torch.set_num_threads(max(1, (os.cpu_count() or 1) // world_size))
        yield rank_device(device_kind, rank)
    finally:
        dist.destroy_process_group()


def build_state(params, layout, seed, weights, device) -> dict:
    return get_fqn_state_dict(*build(params, layout, seed, weights, device))


def synchronize(device: torch.device) -> None:
    if device.type == "cuda":
        torch.cuda.synchronize(device)


def warm_up(prefix: str, device: torch.device) -> None:
    """Builds the fsspec/gcsfs instance and fetches a token before timed rounds."""
    fs, root = fsspec.core.url_to_fs(prefix)
    fs.exists(root)
    synchronize(device)


def timed(device: torch.device, fn, *args, **kwargs) -> tuple[float, float]:
    """Runs fn between barriers; the end time includes queued CUDA work."""
    dist.barrier()
    begin = time.perf_counter()
    fn(*args, **kwargs)
    synchronize(device)
    dist.barrier()
    return begin, time.perf_counter()


def write_stats(stats_dir: str, rank: int, stats: dict) -> None:
    with open(os.path.join(stats_dir, f"rank{rank}.json"), "w") as f:
        json.dump(stats, f)


def read_stats(stats_dir: str) -> list[dict]:
    stats = []
    for name in sorted(os.listdir(stats_dir)):
        with open(os.path.join(stats_dir, name)) as f:
            stats.append(json.load(f))
    return stats


def spawn_with_stats(prefix, params, target_fn, world_size, weights):
    """Spawns target_fn(..., weights=, stats_dir=) and returns (ranks, stats).

    Each rank writes its stats before reporting durations, so they are all on
    disk once spawn_split returns. On failure the ranks are joined and the
    shared weights released.
    """
    ranks = None
    try:
        with tempfile.TemporaryDirectory() as stats_dir:
            ranks = spawn_split(
                prefix,
                params,
                functools.partial(target_fn, weights=weights, stats_dir=stats_dir),
                world_size_override=world_size,
            )
            return ranks, read_stats(stats_dir)
    except BaseException:
        weights.clear()
        if ranks is not None:
            ranks.join()
        raise


class DCPDriver(CheckpointDriver):
    def prepare(self, params, *layouts) -> None:
        """Rejects unsupported layouts before staging the model weights."""
        meta_model = build_model(params.model_id)
        for layout in set(layouts):
            check_model_layout(meta_model, layout)
        stage_model(params.model_id)

    def is_checkpoint_file(self, path: str) -> bool:
        return path.endswith((".distcp", ".metadata"))
