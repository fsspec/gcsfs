from __future__ import annotations

import os
import time
from typing import TYPE_CHECKING

from gcsfs.tests.perf.subsystembenchmarks._common import accelerator, launcher

if TYPE_CHECKING:
    import torch

# Cap on spawned ranks to prevent OOM on test-c4-highmem.
MAX_RANKS = accelerator.CPU_MAX_RANKS
PORT_BLOCK_SIZE = launcher.PORT_BLOCK_SIZE


def find_free_port() -> int:
    """Finds an available TCP port, delegating to _common.launcher."""
    return launcher.find_free_port()


def run_split(prefix, params, target_fn, world_size_override=None) -> list[float]:
    """Spawns processes to run the distributed benchmark and gathers timing."""
    ranks = spawn_split(prefix, params, target_fn, world_size_override)
    ranks.join()
    return ranks.durations


def _terminate(context, grace_s: float = 10.0) -> None:
    """Terminates worker processes on ``context``, escalating to SIGKILL."""
    procs = []
    for proc in getattr(context, "processes", None) or ():
        try:
            proc.terminate()
        except (OSError, ValueError):
            continue
        procs.append(proc)
    deadline = time.monotonic() + grace_s
    for proc in procs:
        proc.join(max(0.0, deadline - time.monotonic()))

    survivors = []
    for proc in procs:
        if proc.is_alive():
            try:
                proc.kill()
            except (OSError, ValueError):
                continue
            survivors.append(proc)
    if survivors:
        kill_deadline = time.monotonic() + grace_s
        for proc in survivors:
            proc.join(max(0.0, kill_deadline - time.monotonic()))


class SpawnedRanks:
    """Ranks spawned by spawn_split.

    ``durations`` is reduced as soon as every rank has reported, so a caller
    can stop timing or monitoring while the ranks do post-run work (such as
    verifying loaded bytes). ``join`` waits for the ranks to exit and re-raises
    a rank failure; ``close`` terminates any remaining workers and shuts down
    the SyncManager idempotently.
    """

    def __init__(self, durations, context, manager):
        self.durations = durations
        self._context = context
        self._manager = manager
        self._closed = False

    def join(self) -> None:
        if self._closed:
            return
        try:
            while not self._context.join():
                pass
        except BaseException:
            _terminate(self._context)
            raise
        finally:
            self._closed = True
            self._manager.shutdown()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            _terminate(self._context)
        finally:
            self._manager.shutdown()


def spawn_split(prefix, params, target_fn, world_size_override=None) -> SpawnedRanks:
    """Spawns the ranks and returns once every rank has put its result."""
    import queue

    import torch.multiprocessing as mp

    ctx = mp.get_context("spawn")
    world_size = params.world_size
    if world_size_override is not None:
        world_size = world_size_override
    world_size = min(world_size, MAX_RANKS)
    port = find_free_port()

    manager = ctx.Manager()
    context = None
    try:
        q = manager.Queue()
        context = mp.spawn(
            target_fn,
            args=(world_size, port, prefix, params, q),
            nprocs=world_size,
            join=False,
        )
        results = []
        while len(results) < world_size:
            try:
                results.append(q.get(timeout=1))
            except queue.Empty:
                # Raises if a rank died before reporting.
                if context.join(timeout=0):
                    raise RuntimeError("ranks exited without reporting results")
    except BaseException:
        try:
            _terminate(context)
        finally:
            manager.shutdown()
        raise

    return SpawnedRanks(
        reduce_durations(results, world_size, params.rounds), context, manager
    )


def _normalize_local_results(local_results, rounds: int):
    """Rebase local monotonic (begin, end) pairs relative to each round's earliest begin."""
    if not local_results or all(r is None for r in local_results):
        return list(local_results)

    offsets = [min(rank_res[r][0] for rank_res in local_results) for r in range(rounds)]
    return [
        [
            (rank_res[r][0] - offsets[r], rank_res[r][1] - offsets[r])
            for r in range(rounds)
        ]
        for rank_res in local_results
    ]


def reduce_durations(
    results, world_size: int | None = None, rounds: int | None = None
) -> list[float]:
    if not results or all(r is None for r in results):
        return []

    n_ranks = len(results) if world_size is None else world_size
    n_rounds = len(results[0]) if rounds is None else rounds

    # We reduce across ranks for each round.
    durations = []
    for r in range(n_rounds):
        begins = [results[rank][r][0] for rank in range(n_ranks)]
        ends = [results[rank][r][1] for rank in range(n_ranks)]
        durations.append(max(ends) - min(begins))
    return durations


_reduce = reduce_durations


def setup_distributed_env(
    rank: int | None = None,
    world_size: int | None = None,
    port: int | None = None,
    device: str | None = None,
) -> "torch.device":
    """Initializes the distributed process group from torchrun env vars or explicit args."""
    del device  # Deprecated: device kind is resolved via _common.accelerator.
    import torch.distributed as dist

    if rank is None:
        rank = int(os.environ["RANK"])
        local_rank = int(os.environ.get("LOCAL_RANK", str(rank)))
    else:
        os.environ["RANK"] = str(rank)
        local_rank = rank
        os.environ["LOCAL_RANK"] = str(local_rank)

    if world_size is None:
        world_size = int(os.environ["WORLD_SIZE"])
    else:
        os.environ["WORLD_SIZE"] = str(world_size)

    if port is not None:
        os.environ["MASTER_PORT"] = str(port)

    master_addr = os.environ.get("MASTER_ADDR", "").strip() or "localhost"
    os.environ["MASTER_ADDR"] = master_addr
    if master_addr in ("localhost", "127.0.0.1", "::1"):
        os.environ["GLOO_SOCKET_IFNAME"] = "lo"

    bound_device = accelerator.bind_rank(
        local_rank, global_rank=rank, world_size=world_size
    )
    backend = accelerator.process_group_backend(world_size)
    dist.init_process_group(backend, rank=rank, world_size=world_size)
    return bound_device
