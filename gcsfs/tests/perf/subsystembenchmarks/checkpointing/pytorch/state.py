from __future__ import annotations

from collections.abc import Mapping

import google_crc32c
import torch
from torch.distributed.checkpoint.state_dict import StateDictOptions, get_state_dict
from torch.distributed.tensor import DTensor

from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.model import (
    expected_local,
    expected_lr,
    expected_step,
)

# Keys param_groups by FQN. Unflattened, it is one list per rank that differs across
# PP stages, and DCP saves only one rank's copy of it.
STATE_DICT_OPTIONS = StateDictOptions(flatten_optimizer_state_dict=True)


def get_fqn_state_dict(model, optim) -> dict:
    model_state, optim_state = get_state_dict(model, optim, options=STATE_DICT_OPTIONS)
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


def verify_state(
    state: dict,
    seed: int,
    weights: Mapping[str, torch.Tensor],
    device: torch.device | str,
) -> list[str]:
    dev = torch.device(device)
    bad: list[str] = []
    model_state = state.get("model", {})
    optim_state = state.get("optim", {})

    for fqn, t in model_state.items():
        local = t.to_local() if isinstance(t, DTensor) else t
        exp = expected_local(fqn, t, seed, weights, dev)
        if (
            local.device != exp.device
            or local.dtype != exp.dtype
            or not torch.equal(local, exp)
        ):
            bad.append(fqn)

    expected_optim_keys = {
        k
        for fqn in model_state
        for k in (
            f"state.{fqn}.step",
            f"state.{fqn}.exp_avg",
            f"state.{fqn}.exp_avg_sq",
            f"param_groups.{fqn}.lr",
        )
    }
    bad.extend(expected_optim_keys - set(optim_state))

    exp_step = expected_step(seed)
    exp_lr = expected_lr(seed)
    for key, value in optim_state.items():
        if key.startswith("state."):
            rest = key.removeprefix("state.")
            if rest.endswith(".step"):
                local = value.to_local() if isinstance(value, DTensor) else value
                if isinstance(local, torch.Tensor):
                    ok = local.numel() == 1 and torch.equal(
                        local, torch.full_like(local, exp_step)
                    )
                else:
                    ok = local == exp_step
                if not ok:
                    bad.append(key)
            elif rest.endswith((".exp_avg", ".exp_avg_sq")):
                local = value.to_local() if isinstance(value, DTensor) else value
                exp = expected_local(rest, value, seed, weights, dev)
                if (
                    local.device != exp.device
                    or local.dtype != exp.dtype
                    or not torch.equal(local, exp)
                ):
                    bad.append(key)
        elif key.startswith("param_groups.") and key.endswith(".lr"):
            if value != exp_lr:
                bad.append(key)

    return sorted(bad)


def assert_loaded_state(
    rank: int,
    state: dict,
    bytes_requested: int,
    bytes_expected: int,
    seed: int,
    weights: Mapping[str, torch.Tensor],
    device: torch.device | str,
) -> None:
    if bytes_requested != bytes_expected:
        raise RuntimeError(
            f"rank {rank}: DCP requested {bytes_requested} tensor bytes, "
            f"expected {bytes_expected}"
        )
    bad = verify_state(state, seed, weights, device)
    if bad:
        raise RuntimeError(
            f"rank {rank}: value mismatch in {len(bad)} entries, e.g. {bad[0]}"
        )


def state_checksum(state: dict) -> dict[str, str]:
    # Hardware CRC32C runs at several GB/s per thread, so checking a rank's
    # tensors takes about a second where SHA-256 took tens of seconds.
    return {
        name: f"{google_crc32c.value(_raw_bytes(t)):08x}"
        for name, t in _local_tensors(state)
    }


def _raw_bytes(t: torch.Tensor):
    return t.detach().contiguous().reshape(-1).view(torch.uint8).numpy()
