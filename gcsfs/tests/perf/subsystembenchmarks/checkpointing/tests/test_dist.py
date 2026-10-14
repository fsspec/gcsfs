from unittest.mock import MagicMock

import pytest

from gcsfs.tests.perf.subsystembenchmarks.checkpointing._dist import (
    MAX_RANKS,
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
