"""Thin torch.accelerator probe and worker device helpers for subsystem benchmarks."""

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
CPU_MAX_RANKS = 16
RAY_RESOURCE_SCALE = 10_000


class AcceleratorSetupError(RuntimeError):
    """Raised when explicit accelerator selection cannot be satisfied."""


_PROBE_SNIPPET = """
import json, os, sys
try:
    import torch
except ImportError:
    print(json.dumps(["cpu", 0, "none"]))
    sys.exit(0)
if not hasattr(torch, "accelerator") or not torch.accelerator.is_available():
    print(json.dumps(["cpu", 0, "none"]))
    sys.exit(0)
acc = torch.accelerator.current_accelerator()
raw = acc.type if acc is not None else "cpu"
kind = raw if raw in ("cuda", "tpu") else "cpu"
count = int(torch.accelerator.device_count()) if kind != "cpu" else 0
if kind == "cuda" and count > 0:
    model = str(torch.cuda.get_device_name(0))
elif kind == "tpu":
    model = (
        os.environ.get("GCSFS_SUBSYSTEM_ACCELERATOR_MODEL", "").strip()
        or os.environ.get("TPU_ACCELERATOR_TYPE", "").strip()
        or "tpu"
    )
else:
    model = "none"
print(json.dumps([kind, count, model]))
""".strip()


@functools.lru_cache(maxsize=1)
def _probe_accelerator() -> tuple[str, int, str]:
    """Probe torch.accelerator in a child process so the parent avoids TPU init."""
    env = dict(os.environ)
    env.pop("TORCH_DEVICE_BACKEND_AUTOLOAD", None)
    proc = subprocess.run(
        [sys.executable, "-c", _PROBE_SNIPPET],
        capture_output=True,
        text=True,
        timeout=60,
        check=True,
        env=env,
    )
    lines = [line.strip() for line in proc.stdout.splitlines() if line.strip()]
    kind, count, model = json.loads(lines[-1])
    return str(kind), int(count), str(model)


@functools.lru_cache(maxsize=8)
def _resolve_device_kind(requested: str) -> str:
    if requested == "cpu":
        return "cpu"
    kind, count, _ = _probe_accelerator()
    if requested == "auto":
        return kind if (kind != "cpu" and count > 0) else "cpu"
    if kind != requested or count <= 0:
        raise AcceleratorSetupError(
            f"{DEVICE_ENV}={requested} but torch.accelerator reported "
            f"{kind!r} (count={count})"
        )
    return requested


def device_kind() -> str:
    """Return active device kind ('cpu', 'cuda', or 'tpu'), cached per mode."""
    requested = os.environ.get(DEVICE_ENV, "auto")
    if requested not in DEVICES:
        raise ValueError(f"Unsupported device {requested!r}; expected one of {DEVICES}")
    return _resolve_device_kind(requested)


def _clear_caches() -> None:
    if hasattr(_probe_accelerator, "cache_clear"):
        _probe_accelerator.cache_clear()
    _resolve_device_kind.cache_clear()


device_kind.cache_clear = _clear_caches  # type: ignore[attr-defined]


def accelerator_type() -> str:
    """Return reporting accelerator label ('cpu', 'gpu', or 'tpu')."""
    kind = device_kind()
    return "gpu" if kind == "cuda" else kind


def local_device_count() -> int:
    """Return usable rank capacity on this host."""
    kind = device_kind()
    if kind == "cpu":
        return CPU_MAX_RANKS
    _, count, _ = _probe_accelerator()
    if count <= 0:
        raise AcceleratorSetupError(f"{kind} accelerator reported 0 devices")
    return count


def model_name() -> str:
    """Return GPU name, TPU model label, or 'none'."""
    kind = device_kind()
    if kind == "cpu":
        return "none"
    override = os.environ.get(ACCELERATOR_MODEL_ENV, "").strip()
    if override:
        return override
    if kind == "tpu":
        return (
            os.environ.get("TPU_ACCELERATOR_TYPE", "").strip()
            or _probe_accelerator()[2]
            or "tpu"
        )
    return _probe_accelerator()[2] or "none"


def process_group_backend(world_size: int | None = None) -> str:
    """Return PyTorch init_process_group backend string for the active device."""
    kind = device_kind()
    if kind == "cuda":
        return "cpu:gloo,cuda:nccl"
    if kind == "tpu":
        return "gloo" if world_size == 1 else "tpu_dist"
    return "gloo"


def backend_name(world_size: int | None = None) -> str:
    """Return concise distributed backend label ('gloo', 'nccl', or 'tpu_dist')."""
    kind = device_kind()
    if kind == "cuda":
        return "nccl"
    if kind == "tpu":
        return "gloo" if world_size == 1 else "tpu_dist"
    return "gloo"


def bind_rank(
    local_rank: int,
    *,
    global_rank: int | None = None,
    world_size: int | None = None,
) -> "torch.device":
    """Bind the current worker rank to its device and warm up runtime state."""
    import torch

    kind = device_kind()
    if kind == "cpu":
        return torch.device("cpu")
    if kind == "cuda":
        idx = local_rank % local_device_count()
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
    key = "TORCH_DEVICE_BACKEND_AUTOLOAD"
    prev = os.environ.get(key)
    os.environ[key] = "0"
    try:
        yield
    finally:
        if prev is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = prev


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
