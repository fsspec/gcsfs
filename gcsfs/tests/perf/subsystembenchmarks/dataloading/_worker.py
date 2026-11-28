"""Subprocess / torchrun rank entry point for data-loading read drivers.

``driver.run_rank_epochs_distributed`` and ``driver.run_single_rank_isolated``
start this module with ``--entry module:function``; the function runs one rank
and returns ``(per_epoch, ttfb, build_seconds)``. The launcher already gives
every child a clean ``TORCH_TPU_*`` env.
"""

from __future__ import annotations

import argparse
import datetime
from collections.abc import Sequence

from gcsfs.tests.perf.subsystembenchmarks.dataloading import driver


class _DistBarrier:
    """Adapter matching ``driver.await_round``'s ``.wait(timeout=...)`` interface."""

    def wait(self, timeout=None) -> None:
        del timeout  # Timeout is configured on init_process_group.
        import torch.distributed as dist

        dist.barrier()


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Dataloading benchmark rank worker.")
    parser.add_argument(
        "--entry", required=True, help="Rank function as 'module:function'."
    )
    parser.add_argument("--spec-file", required=True, help="Path to JSON spec file.")
    parser.add_argument(
        "--result-file", required=True, help="Path to JSON result output file."
    )
    parser.add_argument(
        "--distributed",
        action="store_true",
        help="Coordinate ranks via a gloo torch.distributed process group.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    args = _parse_args(argv)
    entry = driver.resolve_qualified_name(args.entry)
    prefix, params, extra = driver.read_worker_spec(args.spec_file)

    if not args.distributed:
        per_epoch, ttfb, build_seconds = entry(0, 1, prefix, params, None, extra)
        durations = [end - begin for begin, end, _ in per_epoch]
        rows = [count for _, _, count in per_epoch]
        driver.write_worker_result(
            args.result_file, durations, rows, ttfb, build_seconds
        )
        return

    import torch.distributed as dist

    dist.init_process_group(
        backend="gloo",
        timeout=datetime.timedelta(seconds=driver.ROUND_BARRIER_TIMEOUT_SECONDS),
    )
    try:
        rank = dist.get_rank()
        world_size = dist.get_world_size()
        result = entry(rank, world_size, prefix, params, _DistBarrier(), extra)
        gathered = [None] * world_size if rank == 0 else None
        dist.gather_object(result, gathered, dst=0)
        if rank == 0:
            durations, rows, ttfb = driver.reduce_split(gathered, params.rounds)
            build_seconds = max(item[2] for item in gathered)
            driver.write_worker_result(
                args.result_file, durations, rows, ttfb, build_seconds
            )
    finally:
        dist.destroy_process_group()


if __name__ == "__main__":
    main()
