import os
import socket

# Cap on spawned ranks to prevent OOM on test-c4-highmem.
MAX_RANKS = 16


def find_free_port() -> int:
    """Finds an available TCP port for the Gloo distributed backend."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


def run_split(prefix, params, target_fn, world_size_override=None) -> list[float]:
    """Spawns processes to run the distributed benchmark and gathers timing."""
    ranks = spawn_split(prefix, params, target_fn, world_size_override)
    ranks.join()
    return ranks.durations


class SpawnedRanks:
    """Ranks spawned by spawn_split.

    ``durations`` is reduced as soon as every rank has reported, so a caller
    can stop timing or monitoring while the ranks do post-run work (such as
    verifying loaded bytes). ``join`` waits for the ranks to exit and re-raises
    a rank failure.
    """

    def __init__(self, durations, context, manager):
        self.durations = durations
        self._context = context
        self._manager = manager

    def join(self) -> None:
        try:
            while not self._context.join():
                pass
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
        manager.shutdown()
        raise

    return SpawnedRanks(_reduce(results, world_size, params.rounds), context, manager)


def _reduce(results, world_size, rounds) -> list[float]:
    if all(r is None for r in results):
        return []

    # We reduce across ranks for each round.
    durations = []
    for r in range(rounds):
        begins = [results[rank][r][0] for rank in range(world_size)]
        ends = [results[rank][r][1] for rank in range(world_size)]
        durations.append(max(ends) - min(begins))
    return durations


def setup_distributed_env(rank: int, world_size: int, port: int) -> None:
    """Initializes the CPU distributed environment (gloo) for a multiprocess worker."""
    import torch.distributed as dist

    os.environ["MASTER_ADDR"] = "localhost"
    os.environ["MASTER_PORT"] = str(port)
    os.environ["GLOO_SOCKET_IFNAME"] = "lo"
    os.environ["WORLD_SIZE"] = str(world_size)
    os.environ["RANK"] = str(rank)
    os.environ["LOCAL_RANK"] = str(rank)

    dist.init_process_group("gloo", rank=rank, world_size=world_size)
