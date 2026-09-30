import functools
import json
import math
import os
import tempfile
import time

import fsspec
import torch
import torch.distributed as dist
import torch.distributed.checkpoint as dcp
from torch.distributed.checkpoint._fsspec_filesystem import FsspecReader, FsspecWriter
from torch.distributed.checkpoint.default_planner import DefaultLoadPlanner
from torch.distributed.checkpoint.planner import LoadItemType, LoadPlan

from gcsfs.tests.perf.subsystembenchmarks.checkpointing._dist import (
    run_split,
    setup_distributed_env,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.driver import (
    CheckpointDriver,
    CheckpointResult,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.model import (
    build_model,
    build_optimizer,
    materialize,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.parallelize import (
    build_mesh,
    parallelize,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.state import (
    expected_bytes_requested,
    get_fqn_state_dict,
    state_checksum,
)

_SETUP_SEED = 0
_RUN_SEED = 1


class _RecordingLoadPlanner(DefaultLoadPlanner):
    """Records how many items and tensor bytes this rank asks DCP to read."""

    def __init__(self):
        super().__init__()
        self.read_items = 0
        self.bytes_requested = 0

    def create_local_plan(self) -> LoadPlan:
        plan = super().create_local_plan()
        md = self.metadata.state_dict_metadata
        self.read_items = len(plan.items)
        self.bytes_requested = sum(
            math.prod(item.lengths) * md[item.dest_index.fqn].properties.dtype.itemsize
            for item in plan.items
            if item.type == LoadItemType.TENSOR
        )
        return plan


def _build(params, mesh, seed):
    model = parallelize(build_model(params), mesh, params)
    materialize(model, seed)
    return model, build_optimizer(model, seed)


def _checksum_path(prefix: str, rank: int):
    fs, root = fsspec.core.url_to_fs(prefix)
    return fs, f"{root.rstrip('/')}/checksums/rank{rank}.json"


def _rank_setup_checkpoint(rank, world_size, port, prefix, params, q):
    setup_distributed_env(rank, world_size, port)
    try:
        torch.set_num_threads(max(1, os.cpu_count() // world_size))
        model, optim = _build(params, build_mesh(params), _SETUP_SEED)
        state = get_fqn_state_dict(model, optim)
        dcp.save(state, storage_writer=FsspecWriter(prefix))

        fs, path = _checksum_path(prefix, rank)
        fs.makedirs(os.path.dirname(path), exist_ok=True)
        fs.pipe_file(path, json.dumps(state_checksum(state)).encode())
        q.put(None)
    finally:
        dist.destroy_process_group()


def _rank_load(rank, world_size, port, prefix, params, q, *, stats_dir):
    setup_distributed_env(rank, world_size, port)
    try:
        torch.set_num_threads(max(1, os.cpu_count() // world_size))
        model, optim = _build(params, build_mesh(params), _RUN_SEED)
        state = get_fqn_state_dict(model, optim)
        bytes_expected = expected_bytes_requested(state)
        # Also builds the gcsfs instance and fetches a token before the timed rounds.
        fs, path = _checksum_path(prefix, rank)
        saved = json.loads(fs.cat_file(path))

        durations = []
        for _ in range(params.rounds):
            planner = _RecordingLoadPlanner()
            dist.barrier()
            begin = time.perf_counter()
            dcp.load(state, storage_reader=FsspecReader(prefix), planner=planner)
            dist.barrier()
            durations.append((begin, time.perf_counter()))

        if planner.bytes_requested != bytes_expected:
            raise RuntimeError(
                f"rank {rank}: DCP requested {planner.bytes_requested} tensor bytes, "
                f"expected {bytes_expected}"
            )

        loaded = state_checksum(get_fqn_state_dict(model, optim))
        if loaded != saved:
            bad = sorted(
                k for k in saved.keys() | loaded.keys() if saved.get(k) != loaded.get(k)
            )
            raise RuntimeError(
                f"rank {rank}: checksum mismatch in {len(bad)} tensors, e.g. {bad[0]}"
            )

        with open(os.path.join(stats_dir, f"rank{rank}"), "w") as f:
            f.write(str(planner.read_items))
        q.put(durations)
    finally:
        dist.destroy_process_group()


class PyTorchCheckpointReadDriver(CheckpointDriver):
    def setup(self, prefix, params):
        run_split(prefix, params, _rank_setup_checkpoint)

    def run(self, prefix, params):
        with tempfile.TemporaryDirectory() as stats_dir:
            durations = run_split(
                prefix, params, functools.partial(_rank_load, stats_dir=stats_dir)
            )
            read_items = []
            for name in os.listdir(stats_dir):
                with open(os.path.join(stats_dir, name)) as f:
                    read_items.append(int(f.read()))

        return CheckpointResult(
            durations=durations,
            extra_columns={"dcp_read_items_per_rank_max": max(read_items)},
        )

    def read_count(self, params) -> int:
        return params.data_parallel_replicate_size

    def is_checkpoint_file(self, path: str) -> bool:
        return path.endswith((".distcp", ".metadata"))
