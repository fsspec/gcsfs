import pytest

from gcsfs.tests.perf.subsystembenchmarks._common import nodes


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for key in (
        nodes.NODES_ENV,
        nodes.NODE_RANK_ENV,
        nodes.LEADER_ADDR_ENV,
        nodes.RDZV_PORT_ENV,
    ):
        monkeypatch.delenv(key, raising=False)


def test_node_group_from_env_none_when_unset_or_one(monkeypatch):
    assert nodes.NodeGroup.from_env() is None

    monkeypatch.setenv(nodes.NODES_ENV, "1")
    assert nodes.NodeGroup.from_env() is None


def test_node_group_from_env_parses_leader_and_follower(monkeypatch):
    monkeypatch.setenv(nodes.NODES_ENV, "2")
    monkeypatch.setenv(nodes.NODE_RANK_ENV, "0")
    monkeypatch.setenv(nodes.LEADER_ADDR_ENV, "pod-0.svc")

    leader = nodes.NodeGroup.from_env()
    assert leader == nodes.NodeGroup(
        node_rank=0,
        num_nodes=2,
        leader_addr="pod-0.svc",
        rdzv_port=nodes.DEFAULT_RDZV_PORT,
    )
    assert leader.is_leader is True

    monkeypatch.setenv(nodes.NODE_RANK_ENV, "1")
    monkeypatch.setenv(nodes.RDZV_PORT_ENV, "29455")
    follower = nodes.NodeGroup.from_env()
    assert follower == nodes.NodeGroup(
        node_rank=1,
        num_nodes=2,
        leader_addr="pod-0.svc",
        rdzv_port=29455,
    )
    assert follower.is_leader is False


def test_node_group_validates_required_vars_and_bounds(monkeypatch):
    monkeypatch.setenv(nodes.NODES_ENV, "2")
    with pytest.raises(ValueError, match=nodes.NODE_RANK_ENV):
        nodes.NodeGroup.from_env()

    monkeypatch.setenv(nodes.NODE_RANK_ENV, "2")
    monkeypatch.setenv(nodes.LEADER_ADDR_ENV, "pod-0.svc")
    with pytest.raises(ValueError, match=nodes.NODE_RANK_ENV):
        nodes.NodeGroup.from_env()

    monkeypatch.setenv(nodes.NODE_RANK_ENV, "-1")
    with pytest.raises(ValueError, match=nodes.NODE_RANK_ENV):
        nodes.NodeGroup.from_env()

    monkeypatch.setenv(nodes.NODE_RANK_ENV, "0")
    monkeypatch.delenv(nodes.LEADER_ADDR_ENV)
    with pytest.raises(ValueError, match=nodes.LEADER_ADDR_ENV):
        nodes.NodeGroup.from_env()

    monkeypatch.setenv(nodes.LEADER_ADDR_ENV, "pod-0.svc")
    monkeypatch.setenv(nodes.RDZV_PORT_ENV, "70000")
    with pytest.raises(ValueError, match=nodes.RDZV_PORT_ENV):
        nodes.NodeGroup.from_env()

    monkeypatch.setenv(nodes.RDZV_PORT_ENV, "not-an-int")
    with pytest.raises(ValueError, match=nodes.RDZV_PORT_ENV):
        nodes.NodeGroup.from_env()

    monkeypatch.delenv(nodes.RDZV_PORT_ENV)
    monkeypatch.setenv(nodes.NODES_ENV, "0")
    with pytest.raises(ValueError, match=nodes.NODES_ENV):
        nodes.NodeGroup.from_env()


def test_no_node_channel_in_nodes_module():
    assert not hasattr(nodes, "NodeChannel")
    assert not hasattr(nodes, "PeerFailed")
    assert not hasattr(nodes, "channel")
    assert not hasattr(nodes, "reset_channel")
    assert not hasattr(nodes, "STORE_PORT_ENV")
