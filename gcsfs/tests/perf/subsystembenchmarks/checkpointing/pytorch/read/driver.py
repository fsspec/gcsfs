import functools

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
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.reference import (
    load_weights,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.state import (
    assert_loaded_state,
    expected_bytes_requested,
)


def _rank_save_source(rank, world_size, port, prefix, params, q, *, weights):
    with rank_session(rank, world_size, port) as device:
        layout = params.source_layout()
        state = build_state(params, layout, SETUP_SEED, weights, device)
        dcp.save(state, storage_writer=FsspecWriter(prefix))
        q.put(None)


def _rank_load(rank, world_size, port, prefix, params, q, *, weights, stats_dir):
    with rank_session(rank, world_size, port) as device:
        layout = params.target_layout()
        state = build_state(params, layout, RUN_SEED, weights, device)
        bytes_expected = expected_bytes_requested(state)
        warm_up(prefix, device)

        durations = []
        for _ in range(params.rounds):
            planner = RecordingLoadPlanner()
            reader = FsspecReader(prefix)
            durations.append(
                timed(device, dcp.load, state, storage_reader=reader, planner=planner)
            )

        write_stats(
            stats_dir,
            rank,
            {"read_items": planner.read_items, "bytes_fetched": planner.bytes_fetched},
        )
        # Report before the checks below so the parent can close its timed
        # and monitored window while the ranks verify the loaded bytes.
        q.put(durations)
        assert_loaded_state(
            rank,
            state,
            planner.bytes_requested,
            bytes_expected,
            SETUP_SEED,
            weights,
            device,
        )


class PyTorchCheckpointReadDriver(DCPDriver):
    def setup(self, prefix, params):
        source = params.source_layout()
        self.prepare(params, source, params.target_layout())
        weights = load_weights(params.model_id)
        try:
            run_split(
                prefix,
                params,
                functools.partial(_rank_save_source, weights=weights),
                world_size_override=source.world_size,
            )
        finally:
            weights.clear()

    def run(self, prefix, params):
        weights = load_weights(params.model_id)
        ranks, stats = spawn_with_stats(
            prefix, params, _rank_load, params.target_layout().world_size, weights
        )
        fetched = [s["bytes_fetched"] for s in stats]

        def _verify() -> None:
            try:
                ranks.join()
            finally:
                weights.clear()

        return CheckpointResult(
            durations=ranks.durations,
            extra_columns={
                "dcp_read_items_per_rank_max": max(s["read_items"] for s in stats),
                "dcp_read_bytes_fetched_total": sum(fetched),
                "dcp_read_bytes_fetched_per_rank_max": max(fetched),
            },
            verify=_verify,
            bytes_transferred=sum(fetched),
        )
