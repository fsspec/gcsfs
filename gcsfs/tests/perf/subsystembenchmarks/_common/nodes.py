"""Cross-node coordination channel over PyTorch TCPStore for multi-node jobs."""

from __future__ import annotations

import contextlib
import dataclasses
import datetime
import json
import os
import time
from collections import defaultdict

from gcsfs.tests.perf.subsystembenchmarks._common import placement

NODE_RANK_ENV = "GCSFS_SUBSYSTEM_NODE_RANK"
LEADER_ADDR_ENV = "GCSFS_SUBSYSTEM_LEADER_ADDR"
STORE_PORT_ENV = "GCSFS_SUBSYSTEM_STORE_PORT"
PHASE_ENV = "GCSFS_SUBSYSTEM_PHASE"
CHANNEL_TIMEOUT_S = 7200

_DEFAULT_STORE_PORT = 29400
_POLL_INTERVAL_S = 0.02
_MAX_POLL_INTERVAL_S = 0.5
# TCPStore's own timeout bounds both connect retries and every single RPC, so it is
# kept short; CHANNEL_TIMEOUT_S is enforced as a logical deadline by _wait_keys.
# Peers start within minutes of each other (concurrent installs; only the leader
# runs --preflight).
_CONNECT_TIMEOUT_S = 1800.0
_RPC_TIMEOUT_S = 60.0
# Upper bound on how long a failing leader waits for followers to acknowledge the
# failure before its process (and the TCPStore it hosts) can exit. Followers poll
# for failures at least once per second.
_FAIL_ACK_TIMEOUT_S = 5.0
# Exported by the leader's main process when it binds the TCPStore server so
# spawned child processes (such as driver.setup) connect as clients instead of
# re-binding the port.
_STORE_HOST_PID_ENV = "GCSFS_SUBSYSTEM_STORE_HOST_PID"


class PeerFailed(RuntimeError):
    """Raised when another node in the multi-node group reported a failure."""


@dataclasses.dataclass(frozen=True)
class NodeGroup:
    node_rank: int
    num_nodes: int
    leader_addr: str
    store_port: int

    @property
    def is_leader(self) -> bool:
        return self.node_rank == 0

    @classmethod
    def from_env(cls) -> NodeGroup | None:
        num_nodes = placement.job_nodes()
        if num_nodes is None or num_nodes <= 1:
            return None

        node_rank = placement.int_env(NODE_RANK_ENV, min_val=0, max_val=num_nodes - 1)
        if node_rank is None:
            raise ValueError(
                f"{NODE_RANK_ENV} must be set when {placement.NODES_ENV}={num_nodes}"
            )

        leader_addr = os.environ.get(LEADER_ADDR_ENV, "").strip()
        if not leader_addr:
            raise ValueError(
                f"{LEADER_ADDR_ENV} must be set when {placement.NODES_ENV}={num_nodes}"
            )

        store_port = (
            placement.int_env(STORE_PORT_ENV, min_val=1, max_val=65535)
            or _DEFAULT_STORE_PORT
        )

        return cls(
            node_rank=node_rank,
            num_nodes=num_nodes,
            leader_addr=leader_addr,
            store_port=store_port,
        )


class NodeChannel:
    """Key/value, barrier, allgather, and failure-propagation channel across nodes."""

    def __init__(self, group: NodeGroup, timeout_s: float = CHANNEL_TIMEOUT_S):
        import torch.distributed as dist

        self.group = group
        self.timeout_s = float(timeout_s)
        self._counters: dict[tuple[str, str], int] = defaultdict(int)
        existing_host_pid = os.environ.get(_STORE_HOST_PID_ENV, "").strip()
        is_master = group.is_leader and not existing_host_pid
        self._store = dist.TCPStore(
            host_name=group.leader_addr,
            port=group.store_port,
            world_size=group.num_nodes,
            is_master=is_master,
            timeout=datetime.timedelta(seconds=min(self.timeout_s, _CONNECT_TIMEOUT_S)),
            wait_for_workers=False,
        )
        self._store.set_timeout(
            datetime.timedelta(seconds=min(self.timeout_s, _RPC_TIMEOUT_S))
        )
        self._owns_store = is_master
        if is_master:
            os.environ[_STORE_HOST_PID_ENV] = str(os.getpid())

    @staticmethod
    def _scope(key: str) -> str:
        return key.split("/", 1)[0]

    def _raise_if_failed(self, scope: str) -> None:
        fail_key = f"fail/{scope}"
        if not self._store.check([fail_key]):
            return
        msg = self._store.get(fail_key).decode("utf-8", errors="replace")
        if not self.group.is_leader:
            # Lets a failing leader stop waiting before it tears down the store.
            with contextlib.suppress(Exception):
                self._store.set(f"fail_ack/{scope}/{self.group.node_rank}", "1")
        raise PeerFailed(msg)

    def _wait_keys(self, keys: list[str], scope: str) -> None:
        deadline = time.monotonic() + self.timeout_s
        poll = _POLL_INTERVAL_S
        while True:
            self._raise_if_failed(scope)
            if self._store.check(keys):
                return
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(
                    f"timed out after {self.timeout_s}s waiting for {keys!r}"
                )
            time.sleep(min(poll, remaining))
            poll = min(poll * 2.0, _MAX_POLL_INTERVAL_S)

    def publish(self, key: str, value: str) -> None:
        self._store.set(f"val/{key}", value)

    def get(self, key: str) -> str:
        full_key = f"val/{key}"
        self._wait_keys([full_key], self._scope(key))
        return self._store.get(full_key).decode("utf-8")

    def _ack_leader(self, tag: str, key: str, scope: str) -> None:
        """Hold leader until followers finish polling the leader's TCPStore."""
        if self.group.is_leader:
            ack_keys = [f"{tag}/{key}/{r}" for r in range(1, self.group.num_nodes)]
            if ack_keys:
                self._wait_keys(ack_keys, scope)
        else:
            self._store.set(f"{tag}/{key}/{self.group.node_rank}", "1")

    def barrier(self, key: str) -> None:
        scope = self._scope(key)
        self._store.set(f"barrier/{key}/{self.group.node_rank}", "1")
        keys = [f"barrier/{key}/{r}" for r in range(self.group.num_nodes)]
        self._wait_keys(keys, scope)
        self._ack_leader("barrier_ack", key, scope)

    def allgather(self, key: str, value: list) -> list:
        scope = self._scope(key)
        own_rank = self.group.node_rank
        self._store.set(f"allgather/{key}/{own_rank}", json.dumps(value))
        keys = [f"allgather/{key}/{r}" for r in range(self.group.num_nodes)]
        self._wait_keys(keys, scope)
        peer_keys = [k for r, k in enumerate(keys) if r != own_rank]
        peer_vals = iter(self._store.multi_get(peer_keys)) if peer_keys else iter(())
        out = []
        for r in range(self.group.num_nodes):
            if r == own_rank:
                out.extend(value)
            else:
                out.extend(json.loads(next(peer_vals).decode("utf-8")))
        self._ack_leader("allgather_ack", key, scope)
        return out

    def fail(self, scope: str, message: str) -> None:
        """Record a failure for ``scope``; the leader waits briefly for follower acks.

        Followers learn about failures only by polling the leader-hosted store, so a
        leader that exited right after writing would take the message with it.
        """
        self._store.set(f"fail/{scope}", message)
        if not self.group.is_leader:
            return
        ack_keys = [f"fail_ack/{scope}/{r}" for r in range(1, self.group.num_nodes)]
        deadline = time.monotonic() + _FAIL_ACK_TIMEOUT_S
        poll = _POLL_INTERVAL_S
        while True:
            try:
                if self._store.check(ack_keys):
                    return
            except Exception:
                return
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return
            time.sleep(min(poll, remaining))
            poll = min(poll * 2.0, _MAX_POLL_INTERVAL_S)

    def check_failed(self, scope: str) -> None:
        """Raise PeerFailed immediately if any peer recorded a failure for ``scope``."""
        self._raise_if_failed(scope)

    def next_key(self, case: str) -> str:
        phase = os.environ.get(PHASE_ENV, "run") or "run"
        n = self._counters[(case, phase)]
        self._counters[(case, phase)] = n + 1
        return f"{case}/{phase}/{n}"

    def close(self) -> None:
        if self._owns_store and os.environ.get(_STORE_HOST_PID_ENV) == str(os.getpid()):
            os.environ.pop(_STORE_HOST_PID_ENV, None)
        self._store = None


_CHANNEL: tuple[int, NodeGroup, NodeChannel] | None = None


def channel() -> NodeChannel | None:
    """Return the per-process singleton NodeChannel, or None for single-node runs."""
    global _CHANNEL
    group = NodeGroup.from_env()
    if group is None:
        return None
    pid = os.getpid()
    if _CHANNEL is not None and _CHANNEL[0] == pid and _CHANNEL[1] == group:
        return _CHANNEL[2]
    if _CHANNEL is not None and _CHANNEL[0] == pid:
        _CHANNEL[2].close()
    ch = NodeChannel(group)
    _CHANNEL = (pid, group, ch)
    return ch


def reset_channel() -> None:
    """Close and discard the cached per-process NodeChannel (for tests)."""
    global _CHANNEL
    if _CHANNEL is not None:
        _CHANNEL[2].close()
        _CHANNEL = None
    elif os.environ.get(_STORE_HOST_PID_ENV) == str(os.getpid()):
        os.environ.pop(_STORE_HOST_PID_ENV, None)
