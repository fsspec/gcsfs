import os
import tempfile

import torch
import torch.nn as nn
from torch.distributed.tensor import DTensor
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


def materialize(model: nn.Module, seed: int) -> None:
    model.to_empty(device="cpu")
    torch.manual_seed(seed)
    for p in model.parameters():
        p.requires_grad_(True)
        torch.nn.init.normal_(p.to_local() if isinstance(p, DTensor) else p, std=0.02)


def build_optimizer(model: nn.Module) -> torch.optim.AdamW:
    optimizer = torch.optim.AdamW(model.parameters(), lr=1e-3)
    for p in model.parameters():
        optimizer.state[p]["step"] = torch.tensor(0, dtype=torch.int64)
        optimizer.state[p]["exp_avg"] = torch.zeros_like(p)
        optimizer.state[p]["exp_avg_sq"] = torch.zeros_like(p)
    return optimizer
