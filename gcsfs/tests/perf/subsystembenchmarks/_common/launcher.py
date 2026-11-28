"""Subprocess and torchrun launcher helpers for subsystem benchmarks."""

from __future__ import annotations

import math
import os
import signal
import socket
import subprocess
import sys
from collections.abc import Mapping, Sequence

from gcsfs.tests.perf.subsystembenchmarks._common import accelerator, nodes
from gcsfs.tests.perf.subsystembenchmarks._common.nodes import NodeGroup

TPU_SLICE_TOPOLOGY_ENV = "GCSFS_SUBSYSTEM_TPU_SLICE_TOPOLOGY"
TPU_SLICE_ADDRESSES_ENV = "GCSFS_SUBSYSTEM_TPU_SLICE_ADDRESSES"
PORT_BASE_ENV = "GCSFS_SUBSYSTEM_PORT_BASE"
PORT_BLOCK_SIZE = 16
RDZV_READ_TIMEOUT_S = 1800
RDZV_JOIN_TIMEOUT_S = 7200

_TORCH_AUTOLOAD_ENV = "TORCH_DEVICE_BACKEND_AUTOLOAD"
_TPU_TOPOLOGY_ENV = "TORCH_TPU_TOPOLOGY"
_TPU_SLICEBUILDER_ADDRESSES_ENV = "TORCH_TPU_SLICEBUILDER_ADDRESSES"
_TPU_TORCHRUN_MODULE = "gcsfs.tests.perf.subsystembenchmarks._common._tpu_torchrun"

_rdzv_step = 0


def find_free_port() -> int:
    """Return an unused localhost TCP port, honoring GCSFS_SUBSYSTEM_PORT_BASE."""
    base = nodes.int_env(PORT_BASE_ENV, min_val=1, max_val=65535)
    if base is not None:
        end = min(base + PORT_BLOCK_SIZE, 65536)
        for candidate in range(base, end):
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                try:
                    sock.bind(("127.0.0.1", candidate))
                except OSError:
                    continue
                return candidate
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def local_nprocs_for_world_size(
    world_size: int,
    node_group: NodeGroup | None = None,
    *,
    devices_per_node: int | None = None,
) -> int:
    """Return how many ranks this node should launch for ``world_size``."""
    if world_size <= 0:
        raise ValueError(f"world_size must be positive, got {world_size}")
    if node_group is None:
        return world_size

    if devices_per_node is None:
        from gcsfs.tests.perf.subsystembenchmarks._common import placement

        per_node = placement.devices_per_node()
    else:
        per_node = devices_per_node
    if per_node <= 0:
        raise ValueError(f"devices_per_node must be positive, got {per_node}")

    if world_size <= per_node:
        return world_size if node_group.is_leader else 0

    active_nodes = math.ceil(world_size / per_node)
    if active_nodes > node_group.num_nodes:
        raise ValueError(
            f"world_size={world_size} requires {active_nodes} nodes at "
            f"{per_node} devices/node, but job has {node_group.num_nodes}"
        )
    if world_size % active_nodes != 0:
        raise ValueError(
            f"world_size={world_size} is not evenly divisible across "
            f"{active_nodes} nodes"
        )
    if node_group.node_rank >= active_nodes:
        return 0
    return world_size // active_nodes


def next_rdzv_port(node_group: NodeGroup) -> int:
    """Allocate the next sequential c10d rendezvous port for multi-node launches."""
    global _rdzv_step
    port = node_group.rdzv_port + _rdzv_step
    _rdzv_step += 1
    return port


def reset_rdzv_step() -> None:
    """Reset the sequential multi-node rendezvous port offset (for tests)."""
    global _rdzv_step
    _rdzv_step = 0


def _build_child_env(
    device_kind: str, env: Mapping[str, str] | None = None
) -> dict[str, str]:
    child_env = os.environ.copy()
    child_env.pop(_TORCH_AUTOLOAD_ENV, None)
    child_env[accelerator.DEVICE_ENV] = device_kind
    if env:
        child_env.update(env)
    return child_env


def _run_managed(cmd: Sequence[str], env: Mapping[str, str]) -> None:
    proc = subprocess.Popen(cmd, env=dict(env), start_new_session=True)
    try:
        rc = proc.wait()
    except BaseException:
        try:
            os.killpg(proc.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        else:
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(proc.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                proc.wait()
        raise
    if rc != 0:
        raise RuntimeError(f"Subprocess exited with status {rc}: {' '.join(cmd)}")


def run_torchrun(
    module: str,
    args: Sequence[str],
    *,
    world_size: int,
    rdzv_id: str,
    node_group: NodeGroup | None = None,
    devices_per_node: int | None = None,
    port: int | None = None,
    env: Mapping[str, str] | None = None,
    prepare_tpu_env: bool = True,
) -> bool:
    """Launch ``module`` via torchrun; return False if skipped on a follower node."""
    local_nprocs = local_nprocs_for_world_size(
        world_size, node_group, devices_per_node=devices_per_node
    )
    if local_nprocs == 0:
        return False

    kind = accelerator.device_kind()
    if kind != "cpu":
        capacity = accelerator.local_device_count()
        if local_nprocs > capacity:
            raise accelerator.AcceleratorSetupError(
                f"local_nprocs={local_nprocs} exceeds local {kind} "
                f"device count {capacity}"
            )

    active_nodes = 1 if node_group is None else world_size // local_nprocs
    child_env = _build_child_env(kind, env)

    if kind == "tpu" and active_nodes == 1:
        child_env.pop(_TPU_TOPOLOGY_ENV, None)
        child_env.pop(_TPU_SLICEBUILDER_ADDRESSES_ENV, None)
    elif kind == "tpu" and active_nodes > 1 and prepare_tpu_env:
        topology = (
            child_env.get(TPU_SLICE_TOPOLOGY_ENV, "").strip()
            or child_env.get(_TPU_TOPOLOGY_ENV, "").strip()
        )
        addresses = (
            child_env.get(TPU_SLICE_ADDRESSES_ENV, "").strip()
            or child_env.get(_TPU_SLICEBUILDER_ADDRESSES_ENV, "").strip()
        )
        if not topology or not addresses:
            raise accelerator.AcceleratorSetupError(
                f"Multi-node TPU launch requires {TPU_SLICE_TOPOLOGY_ENV} "
                f"(or {_TPU_TOPOLOGY_ENV}) and {TPU_SLICE_ADDRESSES_ENV} "
                f"(or {_TPU_SLICEBUILDER_ADDRESSES_ENV})"
            )
        child_env[_TPU_TOPOLOGY_ENV] = topology
        child_env[_TPU_SLICEBUILDER_ADDRESSES_ENV] = addresses

    if active_nodes == 1:
        master_port = port if port is not None else find_free_port()
        torchrun_args = [
            "--standalone",
            f"--nproc_per_node={local_nprocs}",
            f"--master_port={master_port}",
            "-m",
            module,
            *args,
        ]
    else:
        assert node_group is not None
        rdzv_port = port if port is not None else next_rdzv_port(node_group)
        torchrun_args = [
            f"--nnodes={active_nodes}",
            f"--nproc_per_node={local_nprocs}",
            f"--node_rank={node_group.node_rank}",
            "--rdzv_backend=c10d",
            f"--rdzv_endpoint={node_group.leader_addr}:{rdzv_port}",
            f"--rdzv_id={rdzv_id}",
            f"--rdzv_conf=read_timeout={RDZV_READ_TIMEOUT_S},"
            f"join_timeout={RDZV_JOIN_TIMEOUT_S}",
            "-m",
            module,
            *args,
        ]

    if kind == "tpu" and active_nodes == 1 and prepare_tpu_env and local_nprocs > 1:
        cmd = [
            sys.executable,
            "-m",
            _TPU_TORCHRUN_MODULE,
            "--nproc-per-node",
            str(local_nprocs),
            "--",
            *torchrun_args,
        ]
    else:
        cmd = [sys.executable, "-m", "torch.distributed.run", *torchrun_args]

    _run_managed(cmd, child_env)
    return True


def run_subprocess(
    module: str,
    args: Sequence[str],
    *,
    env: Mapping[str, str] | None = None,
) -> None:
    """Launch ``module`` in an isolated Python subprocess."""
    kind = accelerator.device_kind()
    child_env = _build_child_env(kind, env)
    cmd = [sys.executable, "-m", module, *args]
    _run_managed(cmd, child_env)
