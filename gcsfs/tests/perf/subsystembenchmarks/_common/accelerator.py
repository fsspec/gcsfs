"""Thin torch.accelerator probe and worker device helpers for subsystem benchmarks.

Detection runs ``torch.accelerator`` once in a short-lived child process so the
benchmark parent never initializes CUDA or claims libtpu. Launchers forward the
result to their children (``child_probe_env``) so ranks do not probe again.
"""

from __future__ import annotations

import contextlib
import functools
import json
import os
import subprocess
import sys
from collections.abc import Iterator
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import torch

DEVICE_ENV = "GCSFS_SUBSYSTEM_DEVICE"
DEVICES = ("auto", "cpu", "cuda", "tpu")
ACCELERATOR_MODEL_ENV = "GCSFS_SUBSYSTEM_ACCELERATOR_MODEL"
_PROBED_ACCELERATOR_ENV = "_GCSFS_SUBSYSTEM_PROBED_ACCELERATOR"
_TORCH_AUTOLOAD_ENV = "TORCH_DEVICE_BACKEND_AUTOLOAD"
_TPU_ENV_PREFIX = "TORCH_TPU_"
_PROBE_KINDS = ("cpu", "cuda", "tpu")
_PROBE_TIMEOUT_S = 60
CPU_MAX_RANKS = 16
RAY_RESOURCE_SCALE = 10_000


class AcceleratorSetupError(RuntimeError):
    """Raised when explicit accelerator selection cannot be satisfied."""


# Requires torch>=2.6 (torch.accelerator); an older accelerator image fails the
# probe loudly instead of silently benchmarking on CPU.
_PROBE_SNIPPET = """
import json
import torch

acc = torch.accelerator.current_accelerator() if torch.accelerator.is_available() else None
kind = acc.type if acc is not None and acc.type in ("cuda", "tpu") else "cpu"
count = torch.accelerator.device_count() if kind != "cpu" else 0
name = torch.cuda.get_device_name(0) if kind == "cuda" and count else ""
print(json.dumps({"kind": kind, "count": count, "name": name}))
""".strip()


def single_host_env() -> dict[str, str]:
    """Return ``os.environ`` without settings that must not leak into a child.

    ``TORCH_DEVICE_BACKEND_AUTOLOAD=0`` (set around TPU DataLoader workers) would
    hide TorchTPU, and inherited ``TORCH_TPU_*`` launch/slice settings would make
    TorchTPU wait for peers that are not part of a single-host process tree.
    """
    return {
        key: value
        for key, value in os.environ.items()
        if key != _TORCH_AUTOLOAD_ENV and not key.startswith(_TPU_ENV_PREFIX)
    }


def _parse_probe(raw: str) -> tuple[str, int, str]:
    try:
        data = json.loads(raw)
        kind, count, name = str(data["kind"]), int(data["count"]), str(data["name"])
    except (ValueError, KeyError, TypeError) as exc:
        raise AcceleratorSetupError(
            f"invalid accelerator probe result: {raw!r}"
        ) from exc
    if kind not in _PROBE_KINDS or count < 0:
        raise AcceleratorSetupError(f"invalid accelerator probe result: {raw!r}")
    return kind, count, name


def _probe_accelerator() -> tuple[str, int, str]:
    """Return ``(kind, count, name)`` from the forwarded probe or a child process."""
    forwarded = os.environ.get(_PROBED_ACCELERATOR_ENV, "").strip()
    if forwarded:
        return _parse_probe(forwarded)
    env = single_host_env()
    env.pop(_PROBED_ACCELERATOR_ENV, None)
    try:
        proc = subprocess.run(
            [sys.executable, "-c", _PROBE_SNIPPET],
            capture_output=True,
            text=True,
            timeout=_PROBE_TIMEOUT_S,
            check=True,
            env=env,
        )
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
        stderr = exc.stderr or ""
        if isinstance(stderr, bytes):
            stderr = stderr.decode(errors="replace")
        tail = "\n".join(stderr.strip().splitlines()[-20:])
        raise AcceleratorSetupError(
            f"torch.accelerator probe failed ({exc}); stderr tail:\n{tail}"
        ) from exc
    lines = [line for line in proc.stdout.splitlines() if line.strip()]
    if not lines:
        raise AcceleratorSetupError("torch.accelerator probe printed no result")
    return _parse_probe(lines[-1])


@functools.lru_cache(maxsize=1)
def _cached_probe() -> tuple[str, int, str]:
    return _probe_accelerator()


@functools.lru_cache(maxsize=8)
def _resolve_device_kind(requested: str) -> str:
    if requested == "cpu":
        return "cpu"
    kind, count, _ = _cached_probe()
    if requested == "auto":
        return kind if count > 0 else "cpu"
    if kind != requested or count <= 0:
        raise AcceleratorSetupError(
            f"{DEVICE_ENV}={requested} but torch.accelerator reported "
            f"{kind!r} (count={count})"
        )
    return requested


def reset_cache() -> None:
    """Forget the cached probe and device resolution, e.g. after changing env."""
    _cached_probe.cache_clear()
    _resolve_device_kind.cache_clear()


def device_kind() -> str:
    """Return active device kind ('cpu', 'cuda', or 'tpu'), cached per mode."""
    requested = os.environ.get(DEVICE_ENV, "").strip() or "auto"
    if requested not in DEVICES:
        raise ValueError(f"Unsupported device {requested!r}; expected one of {DEVICES}")
    return _resolve_device_kind(requested)


def child_probe_env(kind: str) -> dict[str, str]:
    """Return env vars forwarding this process's probe to children running on ``kind``."""
    if kind == "cpu":
        # Children pinned to CPU never consult the probe.
        return {}
    probed_kind, count, name = _cached_probe()
    payload = {"kind": probed_kind, "count": count, "name": name}
    return {_PROBED_ACCELERATOR_ENV: json.dumps(payload)}


def accelerator_type() -> str:
    """Return reporting accelerator label ('cpu', 'gpu', or 'tpu')."""
    kind = device_kind()
    return "gpu" if kind == "cuda" else kind


def local_device_count() -> int:
    """Return usable rank capacity on this host."""
    kind = device_kind()
    if kind == "cpu":
        return CPU_MAX_RANKS
    _, count, _ = _cached_probe()
    if count <= 0:
        raise AcceleratorSetupError(f"{kind} accelerator reported 0 devices")
    return count


def model_name() -> str:
    """Return the accelerator model label: GPU name, TPU type, or 'none' on CPU."""
    kind = device_kind()
    if kind == "cpu":
        return "none"
    override = os.environ.get(ACCELERATOR_MODEL_ENV, "").strip()
    if override:
        return override
    if kind == "tpu":
        tpu_type = os.environ.get("TPU_ACCELERATOR_TYPE", "").strip()
        if tpu_type:
            return tpu_type
    return _cached_probe()[2] or kind


def backend_name(world_size: int | None = None) -> str:
    """Return the distributed backend label ('gloo', 'nccl', or 'tpu_dist')."""
    kind = device_kind()
    if kind == "cuda":
        return "nccl"
    if kind == "tpu" and world_size != 1:
        return "tpu_dist"
    return "gloo"


def process_group_backend(world_size: int | None = None) -> str:
    """Return the ``init_process_group`` backend string for the active device.

    CUDA keeps gloo for CPU-tensor collectives (e.g. object gathers) next to NCCL.
    """
    name = backend_name(world_size)
    return "cpu:gloo,cuda:nccl" if name == "nccl" else name


def bind_rank(
    local_rank: int,
    *,
    global_rank: int | None = None,
    world_size: int | None = None,
) -> "torch.device":
    """Bind the current worker rank to its device and warm up runtime state.

    CUDA ranks map to ``local_rank % device_count`` so several ranks may share
    a GPU; TorchTPU owns one chip per process.
    """
    import torch

    kind = device_kind()
    if kind == "cpu":
        return torch.device("cpu")
    if kind == "cuda":
        count = torch.cuda.device_count()
        if count <= 0:
            raise AcceleratorSetupError("cuda accelerator reported 0 devices")
        idx = local_rank % count
        torch.cuda.set_device(idx)
        dev = torch.device("cuda", idx)
        torch.zeros(1, device=dev)
        return dev

    rank_value = local_rank if global_rank is None else global_rank
    os.environ.setdefault("RANK", str(rank_value))
    os.environ.setdefault("LOCAL_RANK", str(local_rank))
    if world_size is not None:
        os.environ.setdefault("WORLD_SIZE", str(world_size))
        local_count = local_device_count()
        per_host = local_count if 0 < local_count < world_size else world_size
        os.environ.setdefault("TORCH_TPU_DEVICES_PER_HOST", str(per_host))
    dev = torch.device("tpu")
    torch.zeros(1, device=dev)
    return dev


def synchronize(device: "torch.device | None") -> None:
    """Wait for queued work on ``device``; no-op on CPU or None."""
    if device is None or device.type == "cpu":
        return
    import torch

    torch.accelerator.synchronize(device)


@contextlib.contextmanager
def loader_worker_env() -> Iterator[None]:
    """Temporarily disable Torch device backend autoload for DataLoader workers on TPU."""
    if device_kind() != "tpu":
        yield
        return
    prev = os.environ.get(_TORCH_AUTOLOAD_ENV)
    os.environ[_TORCH_AUTOLOAD_ENV] = "0"
    try:
        yield
    finally:
        if prev is None:
            os.environ.pop(_TORCH_AUTOLOAD_ENV, None)
        else:
            os.environ[_TORCH_AUTOLOAD_ENV] = prev


def ray_resources(world_size: int) -> dict:
    """Return Ray resource kwargs for ``world_size`` tasks/actors on this host."""
    if world_size <= 0:
        raise ValueError(f"world_size must be positive, got {world_size}")
    kind = device_kind()
    if kind == "cpu":
        return {}
    count = local_device_count()
    if kind == "tpu":
        if world_size > count:
            raise ValueError(f"world_size {world_size} exceeds {count} tpu devices")
        return {"resources": {"TPU": 1.0}}
    # Ray tracks fractional GPUs in 1e-4 fixed-point units; integer floor guarantees
    # world_size * share <= count without floating-point oversubscription.
    share = min(1.0, ((count * RAY_RESOURCE_SCALE) // world_size) / RAY_RESOURCE_SCALE)
    return {"num_gpus": share}
