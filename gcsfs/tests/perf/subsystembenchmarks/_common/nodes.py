"""Multi-node group identity parsed from environment variables."""

from __future__ import annotations

import dataclasses
import os

NODES_ENV = "GCSFS_SUBSYSTEM_NODES"
NODE_RANK_ENV = "GCSFS_SUBSYSTEM_NODE_RANK"
LEADER_ADDR_ENV = "GCSFS_SUBSYSTEM_LEADER_ADDR"
RDZV_PORT_ENV = "GCSFS_SUBSYSTEM_RDZV_PORT"
DEFAULT_RDZV_PORT = 29400


def int_env(name: str, *, min_val: int = 1, max_val: int | None = None) -> int | None:
    """Parse an integer env var with optional lower/upper bounds, or None if unset."""
    raw = os.environ.get(name, "").strip()
    if not raw:
        return None
    try:
        val = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer, got {raw!r}") from exc
    if max_val is not None:
        if not min_val <= val <= max_val:
            raise ValueError(f"{name} must be in [{min_val}, {max_val}], got {val}")
    elif val < min_val:
        raise ValueError(f"{name} must be >= {min_val}, got {val}")
    return val


@dataclasses.dataclass(frozen=True)
class NodeGroup:
    """Identity and rendezvous coordinates for a node in a multi-node job."""

    node_rank: int
    num_nodes: int
    leader_addr: str
    rdzv_port: int

    @property
    def is_leader(self) -> bool:
        return self.node_rank == 0

    @classmethod
    def from_env(cls) -> NodeGroup | None:
        num_nodes = int_env(NODES_ENV)
        if num_nodes is None or num_nodes <= 1:
            return None

        node_rank = int_env(NODE_RANK_ENV, min_val=0, max_val=num_nodes - 1)
        if node_rank is None:
            raise ValueError(
                f"{NODE_RANK_ENV} must be set when {NODES_ENV}={num_nodes}"
            )

        leader_addr = os.environ.get(LEADER_ADDR_ENV, "").strip()
        if not leader_addr:
            raise ValueError(
                f"{LEADER_ADDR_ENV} must be set when {NODES_ENV}={num_nodes}"
            )

        rdzv_port = (
            int_env(RDZV_PORT_ENV, min_val=1, max_val=65535) or DEFAULT_RDZV_PORT
        )

        return cls(
            node_rank=node_rank,
            num_nodes=num_nodes,
            leader_addr=leader_addr,
            rdzv_port=rdzv_port,
        )
