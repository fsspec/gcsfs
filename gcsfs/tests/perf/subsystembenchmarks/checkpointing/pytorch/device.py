from __future__ import annotations

import os
from typing import TYPE_CHECKING

from gcsfs.tests.perf.subsystembenchmarks.checkpointing._dist import MAX_RANKS

if TYPE_CHECKING:
    import torch

DEVICE_ENV = "GCSFS_SUBSYSTEM_DEVICE"
_DEVICES = ("cpu", "cuda", "tpu")


def _validate_device(device: str) -> str:
    if device not in _DEVICES:
        raise ValueError(f"Unsupported device {device!r}; expected one of {_DEVICES}")
    return device


def run_device() -> str:
    """Return the requested device; ``auto`` (run.py's default) resolves via accelerator."""
    requested = os.environ.get(DEVICE_ENV, "cpu")
    if requested == "auto":
        from gcsfs.tests.perf.subsystembenchmarks._common import accelerator

        requested = accelerator.device_kind()
    return _validate_device(requested)


def device_capacity(device: str) -> int:
    _validate_device(device)
    if device == "cpu":
        return MAX_RANKS
    if device == "tpu":
        from gcsfs.tests.perf.subsystembenchmarks._common import accelerator

        return accelerator.local_device_count()
    import torch

    count = torch.cuda.device_count()
    if count == 0:
        raise RuntimeError(f"{DEVICE_ENV}=cuda but torch.cuda.device_count() == 0")
    return count


def backend_for(device: str) -> str:
    _validate_device(device)
    if device == "cpu":
        return "gloo"
    if device == "tpu":
        return "tpu_dist"
    return "cpu:gloo,cuda:nccl"


def rank_device(device: str, local_rank: int) -> torch.device:
    import torch

    _validate_device(device)
    if device == "cpu":
        return torch.device("cpu")
    if device == "tpu":
        return torch.device("tpu")
    torch.cuda.set_device(local_rank)
    return torch.device("cuda", local_rank)
