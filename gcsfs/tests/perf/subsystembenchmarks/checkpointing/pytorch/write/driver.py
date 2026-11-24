import functools

import fsspec
import torch.distributed as dist
import torch.distributed.checkpoint as dcp
from torch.distributed.checkpoint.fsspec_filesystem import FsspecReader, FsspecWriter

from gcsfs.tests.perf.subsystembenchmarks.checkpointing._dist import run_split
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.driver import CheckpointResult
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.common import (
    DCPDriver,
    build_state,
    rank_session,
    spawn_with_stats,
    timed,
    warm_up,
    write_stats,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.model import (
    RUN_SEED,
    SETUP_SEED,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.planners import (
    RecordingLoadPlanner,
    RecordingSavePlanner,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.reference import (
    load_weights,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.state import (
    assert_loaded_state,
    expected_bytes_requested,
)


def _step_prefix(prefix: str, step: int) -> str:
    return f"{prefix.rstrip('/')}/step-{step}/"


def _reader_for(prefix: str) -> FsspecReader:
    if prefix.startswith(("gs://", "gcs://")):
        return FsspecReader(prefix, cache_type="blockcache")
    return FsspecReader(prefix)


def _rank_save(rank, world_size, port, prefix, params, q, *, weights, stats_dir):
    with rank_session(rank, world_size, port) as device:
        layout = params.target_layout()
        state = build_state(params, layout, SETUP_SEED, weights, device)
        warm_up(prefix, device)

        durations = []
        for r in range(params.rounds):
            planner = RecordingSavePlanner()
            writer = FsspecWriter(_step_prefix(prefix, r))
            durations.append(
                timed(device, dcp.save, state, storage_writer=writer, planner=planner)
            )
            if r > 0 and rank == 0:
                prev_fs, prev_root = fsspec.core.url_to_fs(_step_prefix(prefix, r - 1))
                prev_fs.rm(prev_root, recursive=True)
            dist.barrier()

        write_stats(stats_dir, rank, {"bytes_written": planner.bytes_written})
        q.put(durations)


def _rank_verify_saved(rank, world_size, port, step_prefix, params, q, *, weights):
    with rank_session(rank, world_size, port) as device:
        layout = params.target_layout()
        state = build_state(params, layout, RUN_SEED, weights, device)
        planner = RecordingLoadPlanner()
        dcp.load(state, storage_reader=_reader_for(step_prefix), planner=planner)
        assert_loaded_state(
            rank,
            state,
            planner.bytes_requested,
            expected_bytes_requested(state),
            SETUP_SEED,
            weights,
            device,
        )
        q.put(None)


class PyTorchCheckpointWriteDriver(DCPDriver):
    def setup(self, prefix, params):
        self.prepare(params, params.target_layout())

    def run(self, prefix, params):
        weights = load_weights(params.model_id)
        world_size = params.target_layout().world_size
        ranks, stats = spawn_with_stats(prefix, params, _rank_save, world_size, weights)
        try:
            ranks.join()
        except BaseException:
            weights.clear()
            raise

        def _verify() -> None:
            try:
                run_split(
                    _step_prefix(prefix, params.rounds - 1),
                    params,
                    functools.partial(_rank_verify_saved, weights=weights),
                    world_size_override=world_size,
                )
            finally:
                weights.clear()

        return CheckpointResult(
            durations=ranks.durations,
            extra_columns={
                "dcp_write_bytes_per_rank_max": max(s["bytes_written"] for s in stats)
            },
            verify=_verify,
        )
