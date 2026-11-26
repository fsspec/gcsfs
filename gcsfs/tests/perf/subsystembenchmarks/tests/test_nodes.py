import multiprocessing as mp
import os
import socket
import time

import pytest

torch = pytest.importorskip("torch")

from gcsfs.tests.perf.subsystembenchmarks._common import nodes, placement


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def _set_node_env(node_rank: int, num_nodes: int, port: int) -> None:
    os.environ[placement.NODES_ENV] = str(num_nodes)
    os.environ[nodes.NODE_RANK_ENV] = str(node_rank)
    os.environ[nodes.LEADER_ADDR_ENV] = "127.0.0.1"
    os.environ[nodes.STORE_PORT_ENV] = str(port)


def _worker_publish_get_barrier(node_rank: int, port: int, out_q) -> None:
    _set_node_env(node_rank, 2, port)
    ch = nodes.channel()
    assert ch is not None
    if ch.group.is_leader:
        ch.publish("case1/bucket", "gs://my-bucket")
    got = ch.get("case1/bucket")
    ch.barrier("case1/sync")
    out_q.put((node_rank, got))


def _worker_allgather(node_rank: int, port: int, out_q) -> None:
    _set_node_env(node_rank, 2, port)
    ch = nodes.channel()
    assert ch is not None
    gathered = ch.allgather("case2/items", [node_rank + 1])
    out_q.put((node_rank, gathered))


def _worker_follower_failure(node_rank: int, port: int, out_q) -> None:
    _set_node_env(node_rank, 2, port)
    ch = nodes.channel()
    assert ch is not None
    if ch.group.is_leader:
        t0 = time.monotonic()
        try:
            ch.get("c/x")
            out_q.put(("unexpected_success", time.monotonic() - t0))
        except nodes.PeerFailed as exc:
            out_q.put(("peer_failed", str(exc), time.monotonic() - t0))
    else:
        time.sleep(0.2)
        ch.fail("c", "boom")
        out_q.put(("follower_done",))


def _run_two_nodes(target) -> list:
    port = _free_port()
    ctx = mp.get_context("spawn")
    out_q = ctx.Queue()
    procs = [ctx.Process(target=target, args=(rank, port, out_q)) for rank in (0, 1)]
    for p in procs:
        p.start()
    for p in procs:
        p.join(timeout=30)
    try:
        for p in procs:
            assert p.exitcode == 0, f"worker exited with {p.exitcode}"
        return [out_q.get(timeout=5) for _ in range(2)]
    finally:
        for p in procs:
            if p.is_alive():
                p.kill()
                p.join(timeout=5)


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for key in (
        placement.NODES_ENV,
        nodes.NODE_RANK_ENV,
        nodes.LEADER_ADDR_ENV,
        nodes.STORE_PORT_ENV,
        nodes.PHASE_ENV,
    ):
        monkeypatch.delenv(key, raising=False)
    nodes.reset_channel()
    yield
    nodes.reset_channel()


def test_from_env_none_for_single_node(monkeypatch):
    assert nodes.NodeGroup.from_env() is None
    assert nodes.channel() is None

    monkeypatch.setenv(placement.NODES_ENV, "1")
    assert nodes.NodeGroup.from_env() is None
    assert nodes.channel() is None

    monkeypatch.setenv(placement.NODES_ENV, "2")
    monkeypatch.setenv(nodes.NODE_RANK_ENV, "0")
    monkeypatch.setenv(nodes.LEADER_ADDR_ENV, "pod-0.svc")
    group = nodes.NodeGroup.from_env()
    assert group == nodes.NodeGroup(
        node_rank=0, num_nodes=2, leader_addr="pod-0.svc", store_port=29400
    )
    assert group.is_leader is True

    monkeypatch.setenv(nodes.NODE_RANK_ENV, "1")
    monkeypatch.setenv(nodes.STORE_PORT_ENV, "29455")
    follower = nodes.NodeGroup.from_env()
    assert follower == nodes.NodeGroup(
        node_rank=1, num_nodes=2, leader_addr="pod-0.svc", store_port=29455
    )
    assert follower.is_leader is False


def test_publish_get_and_barrier_across_two_nodes():
    results = dict(_run_two_nodes(_worker_publish_get_barrier))
    assert results == {0: "gs://my-bucket", 1: "gs://my-bucket"}


def test_allgather_orders_by_node_rank():
    results = dict(_run_two_nodes(_worker_allgather))
    assert results == {0: [1, 2], 1: [1, 2]}


def test_follower_failure_raises_peer_failed_on_leader():
    results = _run_two_nodes(_worker_follower_failure)
    leader_result = next(r for r in results if r[0] != "follower_done")
    assert leader_result[0] == "peer_failed"
    assert "boom" in leader_result[1]
    assert leader_result[2] < 10.0


def _child_setup_process(out_q) -> None:
    os.environ[nodes.PHASE_ENV] = "setup"
    ch = nodes.channel()
    assert ch is not None
    ch.publish("setup_case/port", "29555")
    out_q.put(ch.get("setup_case/port"))


def test_leader_child_process_connects_to_existing_store(monkeypatch):
    port = _free_port()
    monkeypatch.setenv(placement.NODES_ENV, "2")
    monkeypatch.setenv(nodes.NODE_RANK_ENV, "0")
    monkeypatch.setenv(nodes.LEADER_ADDR_ENV, "127.0.0.1")
    monkeypatch.setenv(nodes.STORE_PORT_ENV, str(port))
    parent_ch = nodes.channel()
    assert parent_ch is not None

    ctx = mp.get_context("spawn")
    out_q = ctx.Queue()
    child = ctx.Process(target=_child_setup_process, args=(out_q,))
    child.start()
    child.join(timeout=20)
    assert child.exitcode == 0
    assert out_q.get(timeout=5) == "29555"
    assert parent_ch.get("setup_case/port") == "29555"


def _worker_leader_exits_after_barrier(node_rank: int, port: int, out_q) -> None:
    _set_node_env(node_rank, 2, port)
    ch = nodes.channel()
    assert ch is not None
    if ch.group.is_leader:
        # Follower enters barrier first and backs off to a longer poll sleep.
        time.sleep(0.4)
        ch.barrier("case3/exit")
        ch.close()
    else:
        ch.barrier("case3/exit")
    out_q.put((node_rank, "ok"))


def test_leader_barrier_waits_for_follower_before_closing_store():
    results = dict(_run_two_nodes(_worker_leader_exits_after_barrier))
    assert results == {0: "ok", 1: "ok"}


def test_next_key_separates_phases(monkeypatch):
    port = _free_port()
    group = nodes.NodeGroup(
        node_rank=0, num_nodes=2, leader_addr="127.0.0.1", store_port=port
    )
    ch = nodes.NodeChannel(group, timeout_s=5.0)
    try:
        assert ch.next_key("caseA") == "caseA/run/0"
        assert ch.next_key("caseA") == "caseA/run/1"
        monkeypatch.setenv(nodes.PHASE_ENV, "setup")
        assert ch.next_key("caseA") == "caseA/setup/0"
        assert ch.next_key("caseA") == "caseA/setup/1"
        monkeypatch.delenv(nodes.PHASE_ENV)
        assert ch.next_key("caseA") == "caseA/run/2"
    finally:
        ch.close()


def _worker_leader_fails_then_exits(node_rank: int, port: int, out_q) -> None:
    _set_node_env(node_rank, 2, port)
    ch = nodes.channel()
    assert ch is not None
    if ch.group.is_leader:
        # Follower is already waiting and has backed off to a longer poll sleep.
        time.sleep(0.4)
        ch.fail("c", "leader boom")
        ch.close()
        out_q.put(("leader_done",))
    else:
        try:
            ch.get("c/x")
            out_q.put(("unexpected_success",))
        except nodes.PeerFailed as exc:
            out_q.put(("peer_failed", str(exc)))
        except Exception as exc:  # e.g. DistNetworkError once the store is gone
            out_q.put(("other", type(exc).__name__))


def test_leader_failure_reaches_follower_before_store_closes():
    results = _run_two_nodes(_worker_leader_fails_then_exits)
    follower = next(r for r in results if r[0] != "leader_done")
    assert follower == ("peer_failed", "leader boom")


def test_leader_fail_returns_when_no_follower_acks(monkeypatch):
    monkeypatch.setattr(nodes, "_FAIL_ACK_TIMEOUT_S", 0.5)
    group = nodes.NodeGroup(
        node_rank=0, num_nodes=2, leader_addr="127.0.0.1", store_port=_free_port()
    )
    ch = nodes.NodeChannel(group, timeout_s=5.0)
    try:
        t0 = time.monotonic()
        ch.fail("c", "boom")
        assert time.monotonic() - t0 < 2.0
    finally:
        ch.close()


def test_follower_connect_timeout_is_bounded(monkeypatch):
    monkeypatch.setattr(nodes, "_CONNECT_TIMEOUT_S", 1.0)
    group = nodes.NodeGroup(
        node_rank=1, num_nodes=2, leader_addr="127.0.0.1", store_port=_free_port()
    )
    t0 = time.monotonic()
    with pytest.raises(Exception):
        nodes.NodeChannel(group, timeout_s=nodes.CHANNEL_TIMEOUT_S)
    assert time.monotonic() - t0 < 10.0
