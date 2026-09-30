from unittest.mock import MagicMock

import pytest

from gcsfs.tests.perf.subsystembenchmarks.checkpointing._dist import (
    MAX_RANKS,
    run_split,
)


@pytest.fixture
def fake_spawn(monkeypatch):
    nprocs_seen = []

    def spawn(target_fn, args=(), nprocs=1, join=True):
        nprocs_seen.append(nprocs)
        q = args[4]
        for rank in range(nprocs):
            q.put([(10.0 + rank, 20.0 + rank), (30.0 - rank, 40.0 + 2 * rank)])

    monkeypatch.setattr("torch.multiprocessing.spawn", spawn)
    return nprocs_seen


def _params(world_size):
    params = MagicMock()
    params.world_size = world_size
    params.rounds = 2
    return params


def test_run_split_reduces_max_end_minus_min_begin(fake_spawn):
    durations = run_split("gs://b/ckpt/", _params(4), lambda *a: None)
    assert fake_spawn == [4]
    assert durations == [23.0 - 10.0, 46.0 - 27.0]


def test_run_split_caps_at_max_ranks(fake_spawn):
    run_split("gs://b/ckpt/", _params(MAX_RANKS + 4), lambda *a: None)
    assert fake_spawn == [MAX_RANKS]


def test_run_split_world_size_override(fake_spawn):
    durations = run_split(
        "gs://b/ckpt/", _params(8), lambda *a: None, world_size_override=2
    )
    assert fake_spawn == [2]
    assert durations == [21.0 - 10.0, 42.0 - 29.0]
