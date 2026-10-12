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
    import torch.multiprocessing as mp

    ctx = mp.get_context("spawn")
    world_size = params.world_size
    if world_size_override is not None:
        world_size = world_size_override
    world_size = min(world_size, MAX_RANKS)
    port = find_free_port()

    with ctx.Manager() as manager:
        q = manager.Queue()
        mp.spawn(
            target_fn,
            args=(world_size, port, prefix, params, q),
            nprocs=world_size,
            join=True,
        )
        results = [q.get() for _ in range(world_size)]

    if all(r is None for r in results):
        return []

    # We reduce across ranks for each round.
    durations = []
    for r in range(params.rounds):
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
