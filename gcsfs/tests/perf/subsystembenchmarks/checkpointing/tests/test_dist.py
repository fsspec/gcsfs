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

    monkeypatch.setattr(_dist, "_next_port_offset", 0)
    monkeypatch.setenv("GCSFS_SUBSYSTEM_PORT_BASE", "21000")
    busy = set()
    monkeypatch.setattr(_dist, "_port_is_free", lambda port: port not in busy)
    return busy


def test_find_free_port_hands_out_distinct_ports_from_the_block(port_block):
    assert [find_free_port() for _ in range(3)] == [21000, 21001, 21002]


def test_find_free_port_skips_a_port_in_use(port_block):
    port_block.update({21000, 21001})
    assert find_free_port() == 21002


def test_find_free_port_wraps_around_the_block(port_block):
    from gcsfs.tests.perf.subsystembenchmarks.checkpointing._dist import PORT_BLOCK_SIZE

    ports = [find_free_port() for _ in range(PORT_BLOCK_SIZE + 1)]
    assert ports[-1] == 21000
    assert all(21000 <= p < 21000 + PORT_BLOCK_SIZE for p in ports)


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


def test_find_free_port_without_a_block_asks_the_os(monkeypatch):
    monkeypatch.delenv("GCSFS_SUBSYSTEM_PORT_BASE", raising=False)
    assert 0 < find_free_port() < 65536
