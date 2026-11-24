import socket
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


@pytest.fixture
def port_block(monkeypatch):
    from gcsfs.tests.perf.subsystembenchmarks.checkpointing import _dist

    monkeypatch.setenv("GCSFS_SUBSYSTEM_PORT_BASE", "21000")
    busy = set()
    monkeypatch.setattr(_dist, "_port_is_free", lambda port: port not in busy)
    return busy


def test_find_free_port_returns_the_first_port_of_the_block(port_block):
    assert find_free_port() == 21000


def test_find_free_port_skips_a_port_in_use(port_block):
    port_block.update({21000, 21001})
    assert find_free_port() == 21002


def test_find_free_port_reuses_a_port_once_it_is_released(port_block):
    # setup (a spawned process) and run (the parent) of one case may share a port.
    port_block.add(21000)
    assert find_free_port() == 21001
    port_block.discard(21000)
    assert find_free_port() == 21000


def test_find_free_port_stays_inside_the_block(port_block):
    from gcsfs.tests.perf.subsystembenchmarks.checkpointing._dist import PORT_BLOCK_SIZE

    last = 21000 + PORT_BLOCK_SIZE - 1
    port_block.update(range(21000, last))
    assert find_free_port() == last


def test_find_free_port_fails_when_the_block_is_exhausted(port_block, monkeypatch):
    from gcsfs.tests.perf.subsystembenchmarks.checkpointing import _dist

    monkeypatch.setattr(_dist, "_port_is_free", lambda port: False)
    with pytest.raises(RuntimeError, match="21000"):
        find_free_port()


def test_port_is_free_detects_a_bound_socket():
    from gcsfs.tests.perf.subsystembenchmarks.checkpointing._dist import _port_is_free

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as busy:
        busy.bind(("", 0))
        busy.listen()
        assert not _port_is_free(busy.getsockname()[1])


@pytest.mark.skipif(not socket.has_ipv6, reason="needs IPv6")
def test_port_is_free_detects_a_dual_stack_listener():
    # torch's TCPStore listens on [::]:port with IPV6_V6ONLY off.
    from gcsfs.tests.perf.subsystembenchmarks.checkpointing._dist import _port_is_free

    try:
        busy = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
    except OSError:
        pytest.skip("IPv6 sockets unavailable")
    with busy:
        busy.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
        busy.bind(("::", 0))
        busy.listen()
        assert not _port_is_free(busy.getsockname()[1])


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


def test_setup_distributed_env_cpu_and_cuda(monkeypatch):
    import os

    import torch
    import torch.distributed as dist

    from gcsfs.tests.perf.subsystembenchmarks.checkpointing._dist import (
        setup_distributed_env,
    )

    events = []
    monkeypatch.setattr(
        torch.cuda, "set_device", lambda idx: events.append(("set_device", idx))
    )
    monkeypatch.setattr(
        dist,
        "init_process_group",
        lambda backend, rank, world_size: events.append(
            ("init_pg", backend, rank, world_size)
        ),
    )
    for key in (
        "MASTER_ADDR",
        "MASTER_PORT",
        "GLOO_SOCKET_IFNAME",
        "WORLD_SIZE",
        "RANK",
        "LOCAL_RANK",
    ):
        monkeypatch.delenv(key, raising=False)

    setup_distributed_env(1, 4, 29500)
    assert events == [("init_pg", "gloo", 1, 4)]
    assert os.environ["RANK"] == "1"
    assert os.environ["WORLD_SIZE"] == "4"
    assert os.environ["MASTER_PORT"] == "29500"

    events.clear()
    setup_distributed_env(2, 8, 29501, device="cuda")
    assert events == [
        ("set_device", 2),
        ("init_pg", "cpu:gloo,cuda:nccl", 2, 8),
    ]


def _shared_tensor_rank(rank, world_size, port, prefix, params, q, *, tensors):
    assert len(tensors) == 600
    q.put([(0.0, 1.0)])


def test_spawn_split_raises_soft_nofile_limit_for_shared_tensors(tmp_path):
    import functools
    import resource
    import types

    torch = pytest.importorskip("torch")
    old_soft, old_hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    if old_hard != resource.RLIM_INFINITY and old_hard < 4096:
        pytest.skip("hard RLIMIT_NOFILE too low for shared-tensor test")

    resource.setrlimit(resource.RLIMIT_NOFILE, (1024, old_hard))
    try:
        tensors = [torch.zeros(4).share_memory_() for _ in range(600)]
        real_params = types.SimpleNamespace(world_size=2, rounds=1)
        durations = run_split(
            str(tmp_path),
            real_params,
            functools.partial(_shared_tensor_rank, tensors=tensors),
        )
        assert len(durations) == 1
        new_soft, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
        assert new_soft > 1024
    finally:
        resource.setrlimit(resource.RLIMIT_NOFILE, (old_soft, old_hard))
