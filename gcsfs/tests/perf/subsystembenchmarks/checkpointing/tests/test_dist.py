"""Tests for shared distributed environment and duration helpers in checkpointing/_dist.py."""

from __future__ import annotations

import os

import pytest
import torch
import torch.distributed as dist

from gcsfs.tests.perf.subsystembenchmarks._common import accelerator
from gcsfs.tests.perf.subsystembenchmarks.checkpointing import _dist
from gcsfs.tests.perf.subsystembenchmarks.checkpointing._dist import (
    _normalize_local_results,
    find_free_port,
    reduce_durations,
    setup_distributed_env,
)


def test_find_free_port_returns_first_free_port_in_the_block(monkeypatch):
    import socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        busy = s.getsockname()[1]
        s.listen()
        monkeypatch.setenv("GCSFS_SUBSYSTEM_PORT_BASE", str(busy))
        port = find_free_port()
    assert busy < port < busy + _dist.PORT_BLOCK_SIZE


def test_find_free_port_without_a_block_asks_the_os(monkeypatch):
    monkeypatch.delenv("GCSFS_SUBSYSTEM_PORT_BASE", raising=False)
    assert 0 < find_free_port() < 65536


def test_setup_distributed_env_reads_torchrun_env_or_explicit_args(monkeypatch):
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

    # 3. Explicit args (Ray actor path) -> defaults MASTER_ADDR=localhost & lo
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
