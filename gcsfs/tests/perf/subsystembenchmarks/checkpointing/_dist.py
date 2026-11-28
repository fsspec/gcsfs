from __future__ import annotations

import os
import socket
from typing import TYPE_CHECKING

from gcsfs.tests.perf.subsystembenchmarks._common import accelerator

if TYPE_CHECKING:
    import torch

# Concurrent benchmark processes on one VM (cloudbuild/subsystembenchmarks/scripts/run-ci.sh)
# each get a disjoint block of ports via GCSFS_SUBSYSTEM_PORT_BASE. Without it, an
# OS-assigned free port is probed and only bound later by the TCPStore, so two processes
# can pick the same port and one fails with EADDRINUSE.
PORT_BLOCK_SIZE = 16


def _port_is_free(port: int) -> bool:
    # A TCPStore listens dual-stack on [::]:port, which also makes this IPv4 bind fail;
    # binding without SO_REUSEADDR also rejects a port that holds a server-side TIME_WAIT.
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.bind(("", port))
        except OSError:
            return False
    return True


def find_free_port() -> int:
    """Finds an available TCP port for an in-process gloo/Ray MASTER_PORT."""
    base = os.environ.get("GCSFS_SUBSYSTEM_PORT_BASE")
    if base:
        # The first free port in the block. A case's setup and run may both get the
        # same port; that is safe because setup's process group is gone before run
        # probes the block.
        for port in range(int(base), int(base) + PORT_BLOCK_SIZE):
            if _port_is_free(port):
                return port
        raise RuntimeError(
            f"no free port in [{base}, {int(base) + PORT_BLOCK_SIZE}) "
            "(GCSFS_SUBSYSTEM_PORT_BASE)"
        )
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


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


def setup_distributed_env(
    rank: int | None = None,
    world_size: int | None = None,
    port: int | None = None,
) -> "torch.device":
    """Initializes the distributed process group from torchrun env vars or explicit args."""
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
