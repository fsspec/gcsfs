from unittest.mock import MagicMock

import pytest

from gcsfs.tests.perf.subsystembenchmarks.checkpointing._dist import (
    MAX_RANKS,
    find_free_port,
    run_split,
    spawn_split,
)


class _FakeContext:
    def __init__(self):
        self.join_calls = 0

    def join(self, timeout=None):
        self.join_calls += 1
        return True


@pytest.fixture
def fake_spawn(monkeypatch):
    calls = []

    def spawn(target_fn, args=(), nprocs=1, join=True):
        q = args[4]
        for rank in range(nprocs):
            q.put([(10.0 + rank, 20.0 + rank), (30.0 - rank, 40.0 + 2 * rank)])
        ctx = _FakeContext()
        calls.append((nprocs, join, ctx))
        return ctx

    monkeypatch.setattr("torch.multiprocessing.spawn", spawn)
    return calls


def _params(world_size):
    params = MagicMock()
    params.world_size = world_size
    params.rounds = 2
    return params


def test_run_split_reduces_max_end_minus_min_begin(fake_spawn):
    durations = run_split("gs://b/ckpt/", _params(4), lambda *a: None)
    assert [c[0] for c in fake_spawn] == [4]
    assert durations == [23.0 - 10.0, 46.0 - 27.0]


def test_run_split_caps_at_max_ranks(fake_spawn):
    run_split("gs://b/ckpt/", _params(MAX_RANKS + 4), lambda *a: None)
    assert [c[0] for c in fake_spawn] == [MAX_RANKS]


def test_run_split_world_size_override(fake_spawn):
    durations = run_split(
        "gs://b/ckpt/", _params(8), lambda *a: None, world_size_override=2
    )
    assert [c[0] for c in fake_spawn] == [2]
    assert durations == [21.0 - 10.0, 42.0 - 29.0]


def test_spawn_split_reports_before_join(fake_spawn):
    ranks = spawn_split("gs://b/ckpt/", _params(4), lambda *a: None)
    ((nprocs, join, ctx),) = fake_spawn
    assert (nprocs, join) == (4, False)
    assert ranks.durations == [23.0 - 10.0, 46.0 - 27.0]
    assert ctx.join_calls == 0
    ranks.join()
    assert ctx.join_calls == 1


def test_find_free_port_delegates_to_launcher(monkeypatch):
    from gcsfs.tests.perf.subsystembenchmarks._common import launcher
    from gcsfs.tests.perf.subsystembenchmarks.checkpointing import _dist

    assert _dist.PORT_BLOCK_SIZE == launcher.PORT_BLOCK_SIZE
    monkeypatch.setattr(launcher, "find_free_port", lambda: 23456)
    assert find_free_port() == 23456


def test_find_free_port_without_a_block_asks_the_os(monkeypatch):
    monkeypatch.delenv("GCSFS_SUBSYSTEM_PORT_BASE", raising=False)
    assert 0 < find_free_port() < 65536


def _sleeping_rank(rank, world_size, port, prefix, params, q):
    import os
    import pathlib
    import time

    (pathlib.Path(prefix) / f"ready-{rank}").write_text(str(os.getpid()))
    time.sleep(600)


def test_spawn_split_terminates_ranks_on_parent_error(tmp_path, monkeypatch):
    import pathlib
    import time

    import psutil
    import torch.multiprocessing as mp

    orig_spawn = mp.spawn
    procs = []
    pids = []

    def tracking_spawn(target_fn, args=(), nprocs=1, join=True):
        ctx = orig_spawn(target_fn, args=args, nprocs=nprocs, join=join)
        procs.extend(ctx.processes)
        q = args[4]

        def interrupt_once_started(timeout=None):
            deadline = time.monotonic() + 30.0
            ready_files = [pathlib.Path(args[2]) / f"ready-{r}" for r in range(nprocs)]
            while not all(p.exists() and p.read_text().strip() for p in ready_files):
                if time.monotonic() > deadline:
                    raise TimeoutError("spawned ranks did not start in time")
                time.sleep(0.02)
            pids.extend(int(p.read_text().strip()) for p in ready_files)
            assert all(psutil.pid_exists(pid) for pid in pids)
            raise KeyboardInterrupt("parent interrupted")

        q.get = interrupt_once_started
        return ctx

    monkeypatch.setattr(mp, "spawn", tracking_spawn)

    try:
        import types

        real_params = types.SimpleNamespace(world_size=2, rounds=1)
        with pytest.raises(KeyboardInterrupt, match="parent interrupted"):
            spawn_split(str(tmp_path), real_params, _sleeping_rank)
        assert len(pids) == 2
        alive = [pid for pid in pids if psutil.pid_exists(pid)]
    finally:
        for proc in procs:
            if proc.is_alive():
                proc.kill()
                proc.join(timeout=5)

    assert alive == []


def test_spawned_ranks_join_terminates_on_error():
    from gcsfs.tests.perf.subsystembenchmarks.checkpointing._dist import SpawnedRanks

    proc = MagicMock()
    proc.is_alive.return_value = False
    ctx = MagicMock()
    ctx.processes = [proc]
    ctx.join.side_effect = RuntimeError("rank 1 failed")
    manager = MagicMock()

    ranks = SpawnedRanks([], ctx, manager)
    with pytest.raises(RuntimeError, match="rank 1 failed"):
        ranks.join()

    proc.terminate.assert_called_once_with()
    proc.join.assert_called_once()
    manager.shutdown.assert_called_once_with()

    # _FakeContext (no .processes attribute) still cleans up cleanly on join error.
    failing_fake = _FakeContext()
    failing_fake.join = MagicMock(side_effect=RuntimeError("boom"))
    fake_mgr = MagicMock()
    with pytest.raises(RuntimeError, match="boom"):
        SpawnedRanks([], failing_fake, fake_mgr).join()
    fake_mgr.shutdown.assert_called_once_with()


def test_terminate_escalates_to_kill_and_tolerates_missing_processes():
    from gcsfs.tests.perf.subsystembenchmarks.checkpointing._dist import _terminate

    _terminate(None)
    _terminate(_FakeContext())
    null_procs = MagicMock()
    null_procs.processes = None
    _terminate(null_procs)

    stubborn = MagicMock()
    stubborn.is_alive.return_value = True
    clean = MagicMock()
    clean.is_alive.return_value = False
    ctx = MagicMock()
    ctx.processes = [stubborn, clean]

    _terminate(ctx, grace_s=0.25)

    stubborn.terminate.assert_called_once_with()
    stubborn.kill.assert_called_once_with()
    assert len(stubborn.join.call_args_list) == 2
    for call in stubborn.join.call_args_list:
        assert 0.0 <= call.args[0] <= 0.25

    clean.terminate.assert_called_once_with()
    clean.join.assert_called_once()
    assert 0.0 <= clean.join.call_args.args[0] <= 0.25
    clean.kill.assert_not_called()


def test_terminate_shares_grace_budget_across_cohort(monkeypatch):
    from gcsfs.tests.perf.subsystembenchmarks.checkpointing import _dist

    now = [100.0]
    monkeypatch.setattr(_dist.time, "monotonic", lambda: now[0])

    def _make_stubborn():
        p = MagicMock()
        p.is_alive.return_value = True
        p.join.side_effect = lambda timeout=None: now.__setitem__(
            0, now[0] + (timeout or 0.0)
        )
        return p

    ctx = MagicMock()
    ctx.processes = [_make_stubborn() for _ in range(4)]

    _dist._terminate(ctx, grace_s=10.0)

    # 10s shared across SIGTERM joins + 10s shared across SIGKILL joins (not 4 * 20s = 80s).
    assert now[0] - 100.0 == pytest.approx(20.0)
    for proc in ctx.processes:
        proc.terminate.assert_called_once_with()
        proc.kill.assert_called_once_with()


def test_spawned_ranks_close_is_idempotent_and_safe_after_join():
    from gcsfs.tests.perf.subsystembenchmarks.checkpointing._dist import SpawnedRanks

    proc = MagicMock()
    proc.is_alive.return_value = False
    ctx = MagicMock()
    ctx.processes = [proc]
    manager = MagicMock()

    ranks = SpawnedRanks([1.0], ctx, manager)
    ranks.close()
    ranks.close()
    ranks.join()

    proc.terminate.assert_called_once_with()
    manager.shutdown.assert_called_once_with()
    ctx.join.assert_not_called()

    ctx2 = MagicMock()
    ctx2.join.return_value = True
    ctx2.processes = [MagicMock()]
    manager2 = MagicMock()
    joined = SpawnedRanks([1.0], ctx2, manager2)
    joined.join()
    joined.close()

    ctx2.processes[0].terminate.assert_not_called()
    manager2.shutdown.assert_called_once_with()

    ctx3 = MagicMock()
    ctx3.join.side_effect = RuntimeError("worker crash")
    proc3 = MagicMock()
    proc3.is_alive.return_value = False
    ctx3.processes = [proc3]
    manager3 = MagicMock()
    failed = SpawnedRanks([1.0], ctx3, manager3)
    with pytest.raises(RuntimeError, match="worker crash"):
        failed.join()
    failed.close()

    proc3.terminate.assert_called_once_with()
    manager3.shutdown.assert_called_once_with()


def test_setup_distributed_env_reads_torchrun_env_or_explicit_args(monkeypatch):
    import inspect
    import os

    import torch
    import torch.distributed as dist

    from gcsfs.tests.perf.subsystembenchmarks._common import accelerator
    from gcsfs.tests.perf.subsystembenchmarks.checkpointing import _dist
    from gcsfs.tests.perf.subsystembenchmarks.checkpointing._dist import (
        setup_distributed_env,
    )

    assert "/proc/net/route" not in inspect.getsource(_dist)
    assert not hasattr(_dist, "_ensure_open_file_limit")

    events = []

    def fake_bind_rank(local_rank, *, global_rank=None, world_size=None):
        events.append(("bind", local_rank, global_rank, world_size))
        return torch.device("cpu")

    monkeypatch.setattr(accelerator, "bind_rank", fake_bind_rank)
    monkeypatch.setattr(
        accelerator,
        "process_group_backend",
        lambda world_size=None: "gloo",
    )
    monkeypatch.setattr(
        dist,
        "init_process_group",
        lambda backend, rank, world_size: events.append(
            ("init_pg", backend, rank, world_size)
        ),
    )

    def _clear_env():
        for key in (
            "MASTER_ADDR",
            "MASTER_PORT",
            "GLOO_SOCKET_IFNAME",
            "WORLD_SIZE",
            "RANK",
            "LOCAL_RANK",
        ):
            monkeypatch.delenv(key, raising=False)

    # 1. torchrun env path with non-local MASTER_ADDR -> does NOT set GLOO_SOCKET_IFNAME
    _clear_env()
    monkeypatch.setenv("RANK", "3")
    monkeypatch.setenv("LOCAL_RANK", "1")
    monkeypatch.setenv("WORLD_SIZE", "4")
    monkeypatch.setenv("MASTER_ADDR", "leader.headless.svc")
    monkeypatch.setenv("MASTER_PORT", "29400")

    dev = setup_distributed_env()
    assert dev == torch.device("cpu")
    assert events == [
        ("bind", 1, 3, 4),
        ("init_pg", "gloo", 3, 4),
    ]
    assert "GLOO_SOCKET_IFNAME" not in os.environ

    # 2. torchrun env path with loopback MASTER_ADDR -> sets GLOO_SOCKET_IFNAME=lo
    for loopback in ("localhost", "127.0.0.1", "::1"):
        _clear_env()
        events.clear()
        monkeypatch.setenv("RANK", "0")
        monkeypatch.setenv("LOCAL_RANK", "0")
        monkeypatch.setenv("WORLD_SIZE", "2")
        monkeypatch.setenv("MASTER_ADDR", loopback)
        monkeypatch.setenv("MASTER_PORT", "29401")

        setup_distributed_env()
        assert os.environ["GLOO_SOCKET_IFNAME"] == "lo"

    # 3. Explicit args (Ray actor / legacy path) -> defaults MASTER_ADDR=localhost & lo
    _clear_env()
    events.clear()
    dev = setup_distributed_env(rank=1, world_size=4, port=29500)
    assert dev == torch.device("cpu")
    assert events == [
        ("bind", 1, 1, 4),
        ("init_pg", "gloo", 1, 4),
    ]
    assert os.environ["RANK"] == "1"
    assert os.environ["LOCAL_RANK"] == "1"
    assert os.environ["WORLD_SIZE"] == "4"
    assert os.environ["MASTER_ADDR"] == "localhost"
    assert os.environ["MASTER_PORT"] == "29500"
    assert os.environ["GLOO_SOCKET_IFNAME"] == "lo"


def test_normalize_local_results_rebases_monotonic_clocks():
    from gcsfs.tests.perf.subsystembenchmarks.checkpointing._dist import (
        _normalize_local_results,
        reduce_durations,
    )

    node0 = [
        [(1000.2, 1005.5), (1010.0, 1012.0)],
        [(1000.0, 1005.8), (1010.5, 1012.5)],
    ]
    node1 = [
        [(50000.1, 50006.2), (50020.0, 50023.0)],
        [(50000.0, 50005.9), (50020.2, 50022.8)],
    ]

    norm0 = _normalize_local_results(node0, rounds=2)
    norm1 = _normalize_local_results(node1, rounds=2)

    assert norm0[0][0] == pytest.approx((0.2, 5.5))
    assert norm0[1][0] == pytest.approx((0.0, 5.8))
    assert norm1[0][0] == pytest.approx((0.1, 6.2))
    assert norm1[1][0] == pytest.approx((0.0, 5.9))

    durations = reduce_durations(norm0 + norm1, world_size=4, rounds=2)
    assert durations == pytest.approx([6.2, 3.0])

    # Empty / None results handled cleanly
    assert _normalize_local_results([], rounds=2) == []
    assert _normalize_local_results([None, None], rounds=2) == [None, None]
    assert reduce_durations([None, None], world_size=2, rounds=2) == []
