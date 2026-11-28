"""torchrun worker entrypoint for native PyTorch DCP checkpoint benchmarks."""

from __future__ import annotations

import argparse
import json
import os
import time

import fsspec
import torch.distributed as dist
import torch.distributed.checkpoint as dcp

from gcsfs.tests.perf._common.resource_monitor import ResourceMonitor
from gcsfs.tests.perf.subsystembenchmarks._common import nodes
from gcsfs.tests.perf.subsystembenchmarks.checkpointing._dist import (
    _normalize_local_results,
    reduce_durations,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.common import (
    build_state,
    import_fsspec_filesystem,
    rank_session,
    step_prefix,
    timed,
    warm_up,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.model import (
    RUN_SEED,
    SETUP_SEED,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.parameters import (
    PyTorchCheckpointParameters,
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


def _reader_for(prefix: str):
    FsspecReader, _ = import_fsspec_filesystem()
    if prefix.startswith(("gs://", "gcs://")):
        return FsspecReader(prefix, cache_type="blockcache")
    return FsspecReader(prefix)


def _node_rank() -> int:
    raw = (
        os.environ.get(nodes.NODE_RANK_ENV, "").strip()
        or os.environ.get("GROUP_RANK", "").strip()
    )
    return int(raw) if raw else 0


def _reduce_gathered(
    gathered: list[tuple[int, list[tuple[float, float]], dict]],
    world_size: int,
    rounds: int,
) -> tuple[list[float], list[dict]]:
    by_node: dict[int, list[list[tuple[float, float]]]] = {}
    stats: list[dict] = []
    for n_rank, rank_durations, rank_stats in gathered:
        by_node.setdefault(n_rank, []).append(rank_durations)
        stats.append(rank_stats)

    all_normalized = []
    for n_rank in sorted(by_node):
        all_normalized.extend(_normalize_local_results(by_node[n_rank], rounds))
    return reduce_durations(all_normalized, world_size, rounds), stats


def _write_result(
    result_file: str,
    gathered: list[tuple[int, list[tuple[float, float]], dict]],
    world_size: int,
    rounds: int,
    window_start: float,
    window_end: float,
    monitor: ResourceMonitor,
) -> None:
    durations, stats = _reduce_gathered(gathered, world_size, rounds)
    payload = {
        "durations": durations,
        "window": [window_start, window_end],
        "resource_metrics": {
            "max_cpu": monitor.max_cpu,
            "max_mem": monitor.max_mem,
            "net_recv": monitor.net_recv,
            "net_sent": monitor.net_sent,
            "duration": monitor.duration,
            "vcpus": monitor.vcpus,
        },
        "stats": stats,
    }
    with open(result_file, "w") as f:
        json.dump(payload, f)


def _run_save(
    prefix: str,
    params: PyTorchCheckpointParameters,
    *,
    weights,
    device,
    result_file: str | None,
) -> None:
    rank = dist.get_rank()
    world_size = dist.get_world_size()
    _, FsspecWriter = import_fsspec_filesystem()

    layout = params.target_layout()
    state = build_state(params, layout, SETUP_SEED, weights, device)
    warm_up(prefix, device)
    dist.barrier()

    monitor = ResourceMonitor(pid=os.getppid()) if rank == 0 else None
    window_start = 0.0
    window_end = 0.0
    durations: list[tuple[float, float]] = []
    planner: RecordingSavePlanner | None = None

    if monitor is not None:
        monitor.__enter__()
        window_start = time.time()
    try:
        for r in range(params.rounds):
            planner = RecordingSavePlanner()
            writer = FsspecWriter(step_prefix(prefix, r))
            durations.append(
                timed(device, dcp.save, state, storage_writer=writer, planner=planner)
            )
    finally:
        if monitor is not None:
            window_end = time.time()
            monitor.__exit__(None, None, None)

    # Keep only the last step (verified and sized by the runner). Deleting after the
    # monitored window keeps the DELETE traffic out of the resource metrics.
    if rank == 0:
        for r in range(params.rounds - 1):
            fs, root = fsspec.core.url_to_fs(step_prefix(prefix, r))
            fs.rm(root, recursive=True)

    assert planner is not None
    payload = (_node_rank(), durations, {"bytes_written": planner.bytes_written})
    gathered = [None] * world_size if rank == 0 else None
    dist.gather_object(payload, gathered, dst=0)

    if rank == 0:
        assert result_file is not None and monitor is not None
        _write_result(
            result_file,
            gathered,  # type: ignore[arg-type]
            world_size,
            params.rounds,
            window_start,
            window_end,
            monitor,
        )
    dist.barrier()


def _run_verify_saved(
    prefix: str, params: PyTorchCheckpointParameters, *, weights, device
) -> None:
    rank = dist.get_rank()
    layout = params.target_layout()
    state = build_state(params, layout, RUN_SEED, weights, device)
    planner = RecordingLoadPlanner()
    dcp.load(state, storage_reader=_reader_for(prefix), planner=planner)
    assert_loaded_state(
        rank,
        state,
        planner.bytes_requested,
        expected_bytes_requested(state),
        SETUP_SEED,
        weights,
        device,
    )
    dist.barrier()


def _run_save_source(
    prefix: str, params: PyTorchCheckpointParameters, *, weights, device
) -> None:
    _, FsspecWriter = import_fsspec_filesystem()
    layout = params.source_layout()
    state = build_state(params, layout, SETUP_SEED, weights, device)
    dcp.save(state, storage_writer=FsspecWriter(prefix))
    dist.barrier()


def _run_load(
    prefix: str,
    params: PyTorchCheckpointParameters,
    *,
    weights,
    device,
    result_file: str | None,
) -> None:
    rank = dist.get_rank()
    world_size = dist.get_world_size()
    FsspecReader, _ = import_fsspec_filesystem()

    layout = params.target_layout()
    state = build_state(params, layout, RUN_SEED, weights, device)
    bytes_expected = expected_bytes_requested(state)
    warm_up(prefix, device)
    dist.barrier()

    monitor = ResourceMonitor(pid=os.getppid()) if rank == 0 else None
    window_start = 0.0
    window_end = 0.0
    durations: list[tuple[float, float]] = []
    planner: RecordingLoadPlanner | None = None

    if monitor is not None:
        monitor.__enter__()
        window_start = time.time()
    try:
        for _ in range(params.rounds):
            planner = RecordingLoadPlanner()
            reader = FsspecReader(prefix)
            durations.append(
                timed(device, dcp.load, state, storage_reader=reader, planner=planner)
            )
    finally:
        if monitor is not None:
            window_end = time.time()
            monitor.__exit__(None, None, None)

    assert planner is not None
    payload = (
        _node_rank(),
        durations,
        {"read_items": planner.read_items, "bytes_fetched": planner.bytes_fetched},
    )
    gathered = [None] * world_size if rank == 0 else None
    dist.gather_object(payload, gathered, dst=0)

    if rank == 0:
        assert result_file is not None and monitor is not None
        _write_result(
            result_file,
            gathered,  # type: ignore[arg-type]
            world_size,
            params.rounds,
            window_start,
            window_end,
            monitor,
        )

    assert_loaded_state(
        rank,
        state,
        planner.bytes_requested,
        bytes_expected,
        SETUP_SEED,
        weights,
        device,
    )
    dist.barrier()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--op",
        required=True,
        choices=("save", "verify_saved", "save_source", "load"),
    )
    parser.add_argument("--spec-file", required=True)
    parser.add_argument("--result-file", default=None)
    args = parser.parse_args(argv)

    with open(args.spec_file) as f:
        spec = json.load(f)
    prefix = spec["prefix"]
    params = PyTorchCheckpointParameters(**spec["params"])

    with rank_session() as device:
        with load_weights(params.model_id) as weights:
            if args.op == "save":
                _run_save(
                    prefix,
                    params,
                    weights=weights,
                    device=device,
                    result_file=args.result_file,
                )
            elif args.op == "verify_saved":
                _run_verify_saved(prefix, params, weights=weights, device=device)
            elif args.op == "save_source":
                _run_save_source(prefix, params, weights=weights, device=device)
            elif args.op == "load":
                _run_load(
                    prefix,
                    params,
                    weights=weights,
                    device=device,
                    result_file=args.result_file,
                )


if __name__ == "__main__":
    main()
