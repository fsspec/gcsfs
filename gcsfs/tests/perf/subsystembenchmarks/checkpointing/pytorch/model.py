import hashlib
import os
import tempfile

import torch
import torch.nn as nn
from torch.distributed.tensor import DTensor
from torch.distributed.tensor._utils import compute_local_shape_and_global_offset
from transformers import AutoConfig, AutoModelForCausalLM

import gcsfs


def _resolve_model_config(model_id: str) -> AutoConfig:
    if model_id.startswith("gs://"):
        dir_name = os.path.basename(model_id.rstrip("/"))
        staged_dir = os.path.join("/tmp", dir_name)
        if os.path.exists(os.path.join(staged_dir, "config.json")):
            return AutoConfig.from_pretrained(staged_dir)

        fs = gcsfs.GCSFileSystem()
        config_path = f"{model_id.rstrip('/')}/config.json"
        with tempfile.TemporaryDirectory() as tmp_dir:
            local_config_file = os.path.join(tmp_dir, "config.json")
            fs.get_file(config_path, local_config_file)
            return AutoConfig.from_pretrained(tmp_dir)

    return AutoConfig.from_pretrained(model_id)


def build_model(params) -> nn.Module:
    cfg = _resolve_model_config(params.model_id)
    with torch.device("meta"):
        model = AutoModelForCausalLM.from_config(cfg, torch_dtype=torch.bfloat16)
    return model


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
