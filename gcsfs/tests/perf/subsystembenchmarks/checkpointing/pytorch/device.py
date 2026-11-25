from __future__ import annotations

import os
from typing import TYPE_CHECKING

from gcsfs.tests.perf.subsystembenchmarks.checkpointing._dist import MAX_RANKS

if TYPE_CHECKING:
    import torch

DEVICE_ENV = "GCSFS_SUBSYSTEM_DEVICE"
_DEVICES = ("cpu", "cuda")


def _validate_device(device: str) -> str:
    if device not in _DEVICES:
        raise ValueError(f"Unsupported device {device!r}; expected one of {_DEVICES}")
    return device


def run_device() -> str:
    return _validate_device(os.environ.get(DEVICE_ENV, "cpu"))


def device_capacity(device: str) -> int:
    _validate_device(device)
    if device == "cpu":
        return MAX_RANKS
    import torch

    count = torch.cuda.device_count()
    if count == 0:
        raise RuntimeError(f"{DEVICE_ENV}=cuda but torch.cuda.device_count() == 0")
    return count


def backend_for(device: str) -> str:
    _validate_device(device)
    if device == "cpu":
        return "gloo"
    return "cpu:gloo,cuda:nccl"


def rank_device(device: str, local_rank: int) -> torch.device:
    import torch

    _validate_device(device)
    if device == "cpu":
        return torch.device("cpu")
    torch.cuda.set_device(local_rank)
    return torch.device("cuda", local_rank)
