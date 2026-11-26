"""Shared local-rank spawning: terminate, spawn_local_ranks, join_local_ranks."""

from unittest.mock import MagicMock

import pytest

from gcsfs.tests.perf.subsystembenchmarks._common import accelerator, spawn


class _NoProcessesContext:
    def join(self, timeout=None):
        return True


def _report_rank(rank, q):
    q.put(rank)


def test_terminate_escalates_to_kill_and_tolerates_missing_processes():
    spawn.terminate(None)
    spawn.terminate(_NoProcessesContext())
    null_procs = MagicMock()
    null_procs.processes = None
    spawn.terminate(null_procs)

    stubborn = MagicMock()
    stubborn.is_alive.return_value = True
    clean = MagicMock()
    clean.is_alive.return_value = False
    ctx = MagicMock()
    ctx.processes = [stubborn, clean]

    spawn.terminate(ctx, grace_s=0.25)

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
    now = [100.0]
    monkeypatch.setattr(spawn.time, "monotonic", lambda: now[0])

    def _make_stubborn():
        p = MagicMock()
        p.is_alive.return_value = True
        p.join.side_effect = lambda timeout=None: now.__setitem__(
            0, now[0] + (timeout or 0.0)
        )
        return p

    ctx = MagicMock()
    ctx.processes = [_make_stubborn() for _ in range(4)]

    spawn.terminate(ctx, grace_s=10.0)

    # 10s shared across SIGTERM joins + 10s shared across SIGKILL joins (not 4 * 20s = 80s).
    assert now[0] - 100.0 == pytest.approx(20.0)
    for proc in ctx.processes:
        proc.terminate.assert_called_once_with()
        proc.kill.assert_called_once_with()


def test_spawn_local_ranks_skips_prepare_launch_when_disabled(monkeypatch):
    prepare_calls = []
    monkeypatch.setattr(accelerator, "prepare_launch", prepare_calls.append)

    results, context, manager = spawn.spawn_local_ranks(
        _report_rank, lambda q: (q,), 2, world_size=2, prepare_launch=False
    )
    spawn.join_local_ranks(context, manager)
    assert sorted(results) == [0, 1]
    assert prepare_calls == []

    results, context, manager = spawn.spawn_local_ranks(
        _report_rank, lambda q: (q,), 2, world_size=2
    )
    spawn.join_local_ranks(context, manager)
    assert sorted(results) == [0, 1]
    assert prepare_calls == [2]


def test_join_local_ranks_terminates_and_shuts_down_on_error():
    proc = MagicMock()
    proc.is_alive.return_value = False
    context = MagicMock()
    context.processes = [proc]
    context.join.side_effect = RuntimeError("rank 1 failed")
    manager = MagicMock()

    with pytest.raises(RuntimeError, match="rank 1 failed"):
        spawn.join_local_ranks(context, manager)

    proc.terminate.assert_called_once_with()
    manager.shutdown.assert_called_once_with()
