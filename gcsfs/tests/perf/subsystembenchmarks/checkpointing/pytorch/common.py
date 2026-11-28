"""Rank and driver plumbing shared by the DCP load and save drivers."""

from __future__ import annotations

import contextlib
import dataclasses
import json
import os
import tempfile
import time

import fsspec
import torch
import torch.distributed as dist

from gcsfs.tests.perf.subsystembenchmarks._common import accelerator, launcher
from gcsfs.tests.perf.subsystembenchmarks._common.nodes import NodeGroup
from gcsfs.tests.perf.subsystembenchmarks.checkpointing._dist import (
    setup_distributed_env,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.driver import CheckpointDriver
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.model import (
    build,
    build_model,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.parallelize import (
    check_model_layout,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.reference import (
    ensure_canonical_weights,
    stage_model,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.state import (
    get_fqn_state_dict,
)

WORKER_MODULE = "gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch._worker"


def step_prefix(prefix: str, step: int) -> str:
    return f"{prefix.rstrip('/')}/step-{step}/"


def import_fsspec_filesystem():
    """Return (FsspecReader, FsspecWriter) across PyTorch 2.14+ module paths."""
    try:
        from torch.distributed.checkpoint.fsspec_filesystem import (
            FsspecReader,
            FsspecWriter,
        )
    except ImportError:
        from torch.distributed.checkpoint._fsspec_filesystem import (
            FsspecReader,
            FsspecWriter,
        )
    return FsspecReader, FsspecWriter


@contextlib.contextmanager
def rank_session(
    rank: int | None = None,
    world_size: int | None = None,
    port: int | None = None,
):
    """Joins the process group and yields this rank's torch.device."""
    device = setup_distributed_env(rank, world_size, port)
    try:
        if device.type == "cpu":
            local_ws = int(
                os.environ.get(
                    "LOCAL_WORLD_SIZE",
                    str(world_size or os.environ.get("WORLD_SIZE", "1")),
                )
            )
            torch.set_num_threads(max(1, (os.cpu_count() or 1) // max(1, local_ws)))
        yield device
    finally:
        dist.destroy_process_group()


def build_state(params, layout, seed, weights, device) -> dict:
    return get_fqn_state_dict(*build(params, layout, seed, weights, device))


def warm_up(prefix: str, device: torch.device) -> None:
    """Builds the fsspec/gcsfs instance and fetches a token before timed rounds."""
    fs, root = fsspec.core.url_to_fs(prefix)
    fs.exists(root)
    accelerator.synchronize(device)


def timed(device: torch.device, fn, *args, **kwargs) -> tuple[float, float]:
    """Runs fn between barriers; the end time includes queued accelerator work."""
    dist.barrier()
    begin = time.perf_counter()
    fn(*args, **kwargs)
    accelerator.synchronize(device)
    dist.barrier()
    return begin, time.perf_counter()


def run_worker(
    op: str,
    prefix: str,
    params,
    *,
    world_size: int,
    rdzv_id: str,
    node_group: NodeGroup | None = None,
    expect_result: bool = False,
) -> dict | None:
    """Launch ``_worker.py`` via torchrun and return parsed result.json on leader."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        spec_file = os.path.join(tmp_dir, "spec.json")
        result_file = os.path.join(tmp_dir, "result.json")
        with open(spec_file, "w") as f:
            json.dump({"prefix": prefix, "params": dataclasses.asdict(params)}, f)

        args = ["--op", op, "--spec-file", spec_file]
        if expect_result:
            args.extend(["--result-file", result_file])

        launched = launcher.run_torchrun(
            WORKER_MODULE,
            args,
            world_size=world_size,
            rdzv_id=rdzv_id,
            node_group=node_group,
        )
        if not expect_result or not launched:
            return None
        if node_group is not None and not node_group.is_leader:
            return None
        with open(result_file) as f:
            return json.load(f)


class DCPDriver(CheckpointDriver):
    def prepare(self, params, *layouts) -> None:
        """Rejects unsupported layouts before staging and canonicalizing weights."""
        meta_model = build_model(params.model_id)
        for layout in set(layouts):
            check_model_layout(meta_model, layout)
        stage_model(params.model_id)
        ensure_canonical_weights(params.model_id)

    def is_checkpoint_file(self, path: str) -> bool:
        return path.endswith((".distcp", ".metadata"))
