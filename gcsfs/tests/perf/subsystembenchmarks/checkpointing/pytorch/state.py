import hashlib

import torch
from torch.distributed.checkpoint.state_dict import get_state_dict
from torch.distributed.tensor import DTensor


def get_fqn_state_dict(model, optim) -> dict:
    model_state, optim_state = get_state_dict(model, optim)
    return {"model": model_state, "optim": optim_state}


def _local_tensors(state, prefix=""):
    items = state.items() if isinstance(state, dict) else enumerate(state)
    for key, value in items:
        name = f"{prefix}{key}"
        if isinstance(value, DTensor):
            yield name, value.to_local()
        elif isinstance(value, torch.Tensor):
            yield name, value
        elif isinstance(value, (dict, list, tuple)):
            yield from _local_tensors(value, f"{name}.")


def expected_bytes_requested(state: dict) -> int:
    return sum(t.numel() * t.element_size() for _, t in _local_tensors(state))


def state_checksum(state: dict) -> dict[str, str]:
    return {
        name: hashlib.sha256(
            t.detach().contiguous().reshape(-1).view(torch.uint8).numpy()
        ).hexdigest()
        for name, t in _local_tensors(state)
    }
