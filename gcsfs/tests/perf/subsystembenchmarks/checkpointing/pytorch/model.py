from __future__ import annotations

import hashlib
import os
import tempfile
from collections.abc import Mapping
from typing import Any

import torch
import torch.nn as nn
from torch.distributed.tensor import DTensor
from torch.distributed.tensor._utils import compute_local_shape_and_global_offset
from transformers import AutoConfig, AutoModelForCausalLM

import gcsfs
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.layout import Layout
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.parallelize import (
    build_mesh,
    parallelize,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.reference import (
    local_slice,
    model_dir,
    random_local,
)

SETUP_SEED = 0
RUN_SEED = 1


def _resolve_model_config(model_id: str) -> AutoConfig:
    if model_id.startswith("gs://"):
        staged_dir = model_dir(model_id)
        if os.path.exists(os.path.join(staged_dir, "config.json")):
            return AutoConfig.from_pretrained(staged_dir)

        fs = gcsfs.GCSFileSystem()
        config_path = f"{model_id.rstrip('/')}/config.json"
        with tempfile.TemporaryDirectory() as tmp_dir:
            local_config_file = os.path.join(tmp_dir, "config.json")
            fs.get_file(config_path, local_config_file)
            return AutoConfig.from_pretrained(tmp_dir)

    return AutoConfig.from_pretrained(model_id)


def build_model(model_id: str | Any) -> nn.Module:
    mid = model_id if isinstance(model_id, str) else model_id.model_id
    cfg = _resolve_model_config(mid)
    with torch.device("meta"):
        model = AutoModelForCausalLM.from_config(cfg, dtype=torch.float32)
    return model


def expected_lr(seed: int) -> float:
    return 1e-3 * (seed + 1)


def expected_step(seed: int) -> float:
    return 1000.0 * (seed + 1)


def expected_local(
    key: str,
    target: torch.Tensor,
    seed: int,
    weights: Mapping[str, torch.Tensor],
    device: torch.device | str,
) -> torch.Tensor:
    if key.endswith(".exp_avg"):
        return random_local(seed, key, target, device).mul_(1e-3)
    if key.endswith(".exp_avg_sq"):
        return random_local(seed, key, target, device).square_().mul_(1e-6)
    if seed == SETUP_SEED:
        full = weights[key]
        if tuple(full.shape) != tuple(target.shape):
            raise ValueError(
                f"Weight {key!r} shape {tuple(full.shape)} "
                f"!= expected {tuple(target.shape)}"
            )
        return local_slice(full, target).to(device=device, dtype=torch.float32).clone()
    return random_local(seed, key, target, device)


def build(
    params: str | Any,
    layout: Layout,
    seed: int,
    weights: Mapping[str, torch.Tensor],
    device: torch.device | str,
) -> tuple[nn.Module, torch.optim.AdamW]:
    dev = torch.device(device)
    mesh = build_mesh(layout, device_type=dev.type)
    model = parallelize(build_model(params), mesh, layout)
    model.to_empty(device=dev)
    with torch.no_grad():
        for p in model.parameters():
            p.requires_grad_(True)
            (p.to_local() if isinstance(p, DTensor) else p).zero_()

    optimizer = torch.optim.AdamW(
        model.parameters(), lr=expected_lr(seed), foreach=False
    )
    # Step one parameter at a time so transient zero grads do not add a 4th shard copy.
    for p in model.parameters():
        p.grad = torch.zeros_like(p)
        optimizer.step()
        p.grad = None

    with torch.no_grad():
        for fqn, p in model.named_parameters():
            p_local = p.to_local() if isinstance(p, DTensor) else p
            p_local.copy_(expected_local(fqn, p, seed, weights, dev))
            st = optimizer.state[p]
            st["step"].fill_(expected_step(seed))
            for moment in ("exp_avg", "exp_avg_sq"):
                m = st[moment]
                m_local = m.to_local() if isinstance(m, DTensor) else m
                m_local.copy_(expected_local(f"{fqn}.{moment}", m, seed, weights, dev))
    return model, optimizer


# Legacy random-init path used by the read driver until it moves to build().
def _shard_generator(seed: int, name: str, t: torch.Tensor) -> torch.Generator:
    """Seeds by shard, not by rank.

    Shards of one tensor get different bytes, so the per-rank checksum catches a
    reader that serves another rank's shard. Replicas (TP norms, HSDP replicate
    groups) get identical bytes, which dcp.save relies on when it keeps a single
    copy of a replicated tensor.
    """
    offset = ()
    if isinstance(t, DTensor):
        _, offset = compute_local_shape_and_global_offset(
            t.shape, t.device_mesh, t.placements
        )
    digest = hashlib.sha256(f"{seed}|{name}|{tuple(offset)}".encode()).digest()
    return torch.Generator().manual_seed(int.from_bytes(digest[:8], "little"))


def materialize(model: nn.Module, seed: int) -> None:
    model.to_empty(device="cpu")
    for fqn, p in model.named_parameters():
        p.requires_grad_(True)
        torch.nn.init.normal_(
            p.to_local() if isinstance(p, DTensor) else p,
            std=0.02,
            generator=_shard_generator(seed, fqn, p),
        )


def build_optimizer(model: nn.Module, seed: int) -> torch.optim.AdamW:
    optimizer = torch.optim.AdamW(model.parameters(), lr=1e-3)
    for fqn, p in model.named_parameters():
        state = optimizer.state[p]
        state["step"] = torch.tensor(seed, dtype=torch.int64)
        state["exp_avg"] = torch.empty_like(p)
        state["exp_avg_sq"] = torch.empty_like(p)
        exp_avg, exp_avg_sq = (
            t.to_local() if isinstance(t, DTensor) else t
            for t in (state["exp_avg"], state["exp_avg_sq"])
        )
        generator = _shard_generator(seed, f"{fqn}.optim", p)
        exp_avg.normal_(generator=generator)
        exp_avg_sq.uniform_(generator=generator)
    return optimizer
