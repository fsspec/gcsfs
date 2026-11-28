"""Per-case placement, node-count sizing, and skip selection."""

from __future__ import annotations

import math
import os

from gcsfs.tests.perf.subsystembenchmarks._common import accelerator, limitations
from gcsfs.tests.perf.subsystembenchmarks._common.nodes import NODES_ENV, int_env

DEVICES_PER_NODE_ENV = "GCSFS_SUBSYSTEM_DEVICES_PER_NODE"
COLLECTED_CASES_FILE_ENV = "GCSFS_SUBSYSTEM_COLLECTED_CASES_FILE"
OUTCOME_FILE_ENV = "GCSFS_SUBSYSTEM_OUTCOME_FILE"
GROUP_ENV = "GCSFS_SUBSYSTEM_GROUP"
CASES_ENV = "GCSFS_SUBSYSTEM_CASES"


def devices_per_node() -> int:
    """Return configured devices per node, or probe local capacity if unset."""
    env_val = int_env(DEVICES_PER_NODE_ENV)
    if env_val is not None:
        return env_val
    return accelerator.local_device_count()


def job_nodes() -> int | None:
    """Return configured node count for a multi-node job, or None if unset."""
    return int_env(NODES_ENV)


def nodes_needed(world_size: int, per_node: int) -> int:
    """Return the number of ``per_node``-device nodes required for ``world_size`` ranks."""
    if world_size <= 0:
        raise ValueError(f"world_size must be positive, got {world_size}")
    if per_node <= 0:
        raise ValueError(f"per_node must be positive, got {per_node}")
    return math.ceil(world_size / per_node)


def _target_accelerator() -> str:
    """Return accelerator type for limitation lookup, avoiding hardware probes when listing."""
    if os.environ.get(COLLECTED_CASES_FILE_ENV):
        requested = os.environ.get(accelerator.DEVICE_ENV, "auto")
        if requested == "cuda":
            return "gpu"
        if requested in ("cpu", "tpu"):
            return requested
    return accelerator.accelerator_type()


def devices_needed(params, accel: str) -> int:
    """Return how many accelerator devices one node must provide for ``params``.

    Ranks own one device each, except where ``params`` declares
    ``GPU_RANKS_SHARE_DEVICES`` (dataloading binds ranks to ``rank % gpu_count``
    and gives Ray tasks fractional GPUs), which needs only one GPU per node.
    """
    if accel == "gpu" and getattr(params, "GPU_RANKS_SHARE_DEVICES", False):
        return 1
    return params.max_world_size()


def case_nodes(params, per_node: int, accel: str | None = None) -> int | None:
    """Return the nodes ``params`` needs at ``per_node`` devices, or None if it cannot fit.

    Only cases whose parameters declare ``MULTI_NODE`` may span nodes; any other
    case wider than one node cannot run at this node size.
    """
    if accel is None:
        accel = _target_accelerator()
    need = devices_needed(params, accel)
    if need <= per_node:
        return 1
    if getattr(params, "MULTI_NODE", False):
        return nodes_needed(need, per_node)
    return None


def _width_reason(params, accel: str, per_node) -> str | None:
    """Return the capacity skip reason for a single-node run, or None."""
    if os.environ.get(COLLECTED_CASES_FILE_ENV) or job_nodes() is not None:
        # Listing sizes cases into node counts; multi-node jobs filter by count.
        return None
    width = per_node()
    if case_nodes(params, width, accel) != 1:
        return f"needs {devices_needed(params, accel)} devices; host has {width}"
    return None


def skip_reason(params, group: str) -> str | None:
    """Return the skip reason for ``params`` in ``group``, or None if runnable."""
    accel = _target_accelerator()
    lim = limitations.limitation_reason(group, params, accel)
    if lim is not None:
        return lim
    return _width_reason(params, accel, devices_per_node)


def select(cases, group: str) -> list:
    """Filter cases by ``job_nodes()`` and wrap each as a pytest.param with skip marks.

    Cases skipped by a known limitation also carry ``pytest.mark.limitation`` so the
    run can tell them apart from capacity skips. Devices per node are resolved only
    when a case that is not limitation-skipped needs them.
    """
    import pytest

    nodes = job_nodes()
    accel = _target_accelerator()
    resolved: list[int] = []

    def per_node() -> int:
        if not resolved:
            resolved.append(devices_per_node())
        return resolved[0]

    selected = []
    for case in cases:
        lim = limitations.limitation_reason(group, case, accel)
        if lim is not None:
            marks = (pytest.mark.skip(reason=lim), pytest.mark.limitation)
            selected.append(pytest.param(case, id=case.name, marks=marks))
            continue
        reason = _width_reason(case, accel, per_node)
        if (
            reason is None
            and nodes is not None
            and case_nodes(case, per_node(), accel) != nodes
        ):
            continue
        marks = (pytest.mark.skip(reason=reason),) if reason else ()
        selected.append(pytest.param(case, id=case.name, marks=marks))
    return selected
