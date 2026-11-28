"""Subprocess and torchrun launcher helpers for subsystem benchmarks."""

from __future__ import annotations

import math
import os
import signal
import subprocess
import sys
from collections.abc import Mapping, Sequence

from gcsfs.tests.perf.subsystembenchmarks._common import accelerator
from gcsfs.tests.perf.subsystembenchmarks._common.nodes import NodeGroup

TPU_SLICE_TOPOLOGY_ENV = "GCSFS_SUBSYSTEM_TPU_SLICE_TOPOLOGY"
TPU_SLICE_ADDRESSES_ENV = "GCSFS_SUBSYSTEM_TPU_SLICE_ADDRESSES"
RDZV_READ_TIMEOUT_S = 1800
RDZV_JOIN_TIMEOUT_S = 7200

_TORCH_AUTOLOAD_ENV = "TORCH_DEVICE_BACKEND_AUTOLOAD"
_TPU_TOPOLOGY_ENV = "TORCH_TPU_TOPOLOGY"
_TPU_SLICEBUILDER_ADDRESSES_ENV = "TORCH_TPU_SLICEBUILDER_ADDRESSES"
_TPU_TORCHRUN_MODULE = "gcsfs.tests.perf.subsystembenchmarks._common._tpu_torchrun"

_rdzv_step = 0


def _placement_for_world_size(
    world_size: int,
    node_group: NodeGroup | None = None,
    *,
    devices_per_node: int | None = None,
) -> tuple[int, int]:
    """Return ``(active_nodes, local_nprocs)`` for ``world_size`` on this node."""
    if world_size <= 0:
        raise ValueError(f"world_size must be positive, got {world_size}")
    if node_group is None:
        return 1, world_size

    if devices_per_node is None:
        from gcsfs.tests.perf.subsystembenchmarks._common import placement

        per_node = placement.devices_per_node()
    else:
        per_node = devices_per_node
    if per_node <= 0:
        raise ValueError(f"devices_per_node must be positive, got {per_node}")

    if world_size <= per_node:
        return 1, world_size if node_group.is_leader else 0

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
        return active_nodes, 0
    return active_nodes, world_size // active_nodes


def local_nprocs_for_world_size(
    world_size: int,
    node_group: NodeGroup | None = None,
    *,
    devices_per_node: int | None = None,
) -> int:
    """Return how many ranks this node should launch for ``world_size``."""
    return _placement_for_world_size(
        world_size, node_group, devices_per_node=devices_per_node
    )[1]


def next_rdzv_port(node_group: NodeGroup) -> int:
    """Allocate the next sequential c10d rendezvous port for multi-node launches.

    Every node keeps its own counter, so all nodes must make the same sequence of
    multi-node ``run_torchrun`` calls (nodes outside a phase still advance it).
    Distinct ports per phase keep a late follower from joining the previous
    phase's store; multi-node runs use ``pytest -x``, so a node that fails stops
    instead of drifting out of step.
    """
    global _rdzv_step
    port = node_group.rdzv_port + _rdzv_step
    _rdzv_step += 1
    return port


def reset_rdzv_step() -> None:
    """Reset the sequential multi-node rendezvous port offset (for tests)."""
    global _rdzv_step
    _rdzv_step = 0


def _tpu_slice_env() -> dict[str, str]:
    """Return the TorchTPU slice env a multi-host TPU launch needs."""
    topology = (
        os.environ.get(TPU_SLICE_TOPOLOGY_ENV, "").strip()
        or os.environ.get(_TPU_TOPOLOGY_ENV, "").strip()
    )
    addresses = (
        os.environ.get(TPU_SLICE_ADDRESSES_ENV, "").strip()
        or os.environ.get(_TPU_SLICEBUILDER_ADDRESSES_ENV, "").strip()
    )
    if not topology or not addresses:
        raise accelerator.AcceleratorSetupError(
            f"Multi-node TPU launch requires {TPU_SLICE_TOPOLOGY_ENV} "
            f"(or {_TPU_TOPOLOGY_ENV}) and {TPU_SLICE_ADDRESSES_ENV} "
            f"(or {_TPU_SLICEBUILDER_ADDRESSES_ENV})"
        )
    return {_TPU_TOPOLOGY_ENV: topology, _TPU_SLICEBUILDER_ADDRESSES_ENV: addresses}


def _build_child_env(
    kind: str,
    env: Mapping[str, str] | None = None,
    *,
    multi_host_tpu: bool = False,
    cpu_only: bool = False,
) -> dict[str, str]:
    """Return the environment for a child process running on ``kind``.

    Single-host children start from ``accelerator.single_host_env()`` (no
    inherited ``TORCH_TPU_*``; ``_tpu_torchrun`` or the worker sets what it
    needs). Multi-host TPU launches keep the inherited TorchTPU settings and get
    the slice topology/addresses explicitly.
    """
    if multi_host_tpu:
        child_env = dict(os.environ)
        child_env.pop(_TORCH_AUTOLOAD_ENV, None)
        child_env.update(_tpu_slice_env())
    else:
        child_env = accelerator.single_host_env()
    child_env[accelerator.DEVICE_ENV] = kind
    child_env.update(accelerator.child_probe_env(kind))
    if cpu_only:
        # Prevent torch_tpu autoload in CPU-only children and their DataLoader
        # workers on TPU hosts; caller env overrides still win below.
        child_env[_TORCH_AUTOLOAD_ENV] = "0"
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
    env: Mapping[str, str] | None = None,
    cpu_only: bool = False,
) -> bool:
    """Launch ``module`` via torchrun; return False if this node has no ranks.

    ``cpu_only`` runs the ranks on CPU regardless of the detected accelerator,
    for ranks that never touch a device. Multi-node launches allocate rendezvous
    ports with ``next_rdzv_port`` (see its lockstep contract).
    """
    active_nodes, local_nprocs = _placement_for_world_size(
        world_size, node_group, devices_per_node=devices_per_node
    )
    if local_nprocs == 0:
        if active_nodes > 1:
            next_rdzv_port(node_group)
        return False

    kind = "cpu" if cpu_only else accelerator.device_kind()
    if kind == "tpu":
        # TorchTPU owns one chip per process; CUDA ranks may share a GPU.
        capacity = accelerator.local_device_count()
        if local_nprocs > capacity:
            raise accelerator.AcceleratorSetupError(
                f"local_nprocs={local_nprocs} exceeds local tpu device count "
                f"{capacity}"
            )

    child_env = _build_child_env(
        kind,
        env,
        multi_host_tpu=(kind == "tpu" and active_nodes > 1),
        cpu_only=cpu_only,
    )

    if active_nodes == 1:
        torchrun_args = ["--standalone", f"--nproc_per_node={local_nprocs}"]
    else:
        torchrun_args = [
            f"--nnodes={active_nodes}",
            f"--nproc_per_node={local_nprocs}",
            f"--node_rank={node_group.node_rank}",
            "--rdzv_backend=c10d",
            f"--rdzv_endpoint={node_group.leader_addr}:{next_rdzv_port(node_group)}",
            f"--rdzv_id={rdzv_id}",
            f"--rdzv_conf=read_timeout={RDZV_READ_TIMEOUT_S},"
            f"join_timeout={RDZV_JOIN_TIMEOUT_S}",
        ]
    torchrun_args += ["-m", module, *args]

    if kind == "tpu" and active_nodes == 1 and local_nprocs > 1:
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
    """Launch ``module`` in an isolated single-host Python subprocess."""
    child_env = _build_child_env(accelerator.device_kind(), env)
    _run_managed([sys.executable, "-m", module, *args], child_env)
