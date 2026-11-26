"""Spawn local benchmark ranks, collect their results, and clean up on failure."""

from __future__ import annotations

import time

from gcsfs.tests.perf.subsystembenchmarks._common import accelerator

MIN_OPEN_FILES = 65536


def ensure_open_file_limit(min_nofile: int = MIN_OPEN_FILES) -> None:
    """Raises soft RLIMIT_NOFILE toward hard limit for multi-rank shared-tensor spawns."""
    import resource

    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    target = min_nofile if hard == resource.RLIM_INFINITY else min(hard, min_nofile)
    if soft < target:
        try:
            resource.setrlimit(resource.RLIMIT_NOFILE, (target, hard))
        except (OSError, ValueError):
            pass


def terminate(context, grace_s: float = 10.0) -> None:
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


def spawn_local_ranks(
    spawn_fn,
    spawn_args_fn,
    nprocs: int,
    *,
    world_size: int,
    channel=None,
    scope: str | None = None,
    prepare_launch: bool = True,
):
    """Spawn ``nprocs`` local ranks and collect one result per rank from a queue.

    Returns ``(results, context, manager)``; the caller joins ``context`` and shuts
    ``manager`` down (see :func:`join_local_ranks`). Ranks are terminated if the
    parent is interrupted or a rank dies before reporting. ``prepare_launch=False``
    skips TPU launch setup for spawns whose ranks never use an accelerator.
    """
    import queue

    import torch.multiprocessing as mp

    ensure_open_file_limit()
    if prepare_launch:
        accelerator.prepare_launch(world_size)
    ctx = mp.get_context("spawn")
    manager = ctx.Manager()
    context = None
    try:
        q = manager.Queue()
        context = mp.spawn(
            spawn_fn,
            args=spawn_args_fn(q),
            nprocs=nprocs,
            join=False,
        )
        results = []
        while len(results) < nprocs:
            try:
                results.append(q.get(timeout=1))
            except queue.Empty:
                if channel is not None and scope is not None:
                    channel.check_failed(scope)
                # Raises if a rank died before reporting.
                if context.join(timeout=0):
                    raise RuntimeError("ranks exited without reporting results")
    except BaseException:
        try:
            terminate(context)
        finally:
            manager.shutdown()
        raise
    return results, context, manager


def join_local_ranks(context, manager) -> None:
    """Wait for ranks to exit, terminating them on any error; always shut ``manager`` down."""
    try:
        while not context.join():
            pass
    except BaseException:
        terminate(context)
        raise
    finally:
        manager.shutdown()
