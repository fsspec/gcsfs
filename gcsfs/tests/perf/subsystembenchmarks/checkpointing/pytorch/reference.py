from __future__ import annotations

import glob
import hashlib
import json
import os
import shutil
import tempfile

import torch
from torch.distributed.tensor import DTensor
from torch.distributed.tensor._utils import compute_local_shape_and_global_offset
from transformers import AutoModelForCausalLM

import gcsfs


def model_dir(model_id: str) -> str:
    if model_id.startswith("gs://"):
        name = os.path.basename(model_id.removeprefix("gs://").rstrip("/"))
        if not name:
            raise ValueError(f"Invalid GCS model_id {model_id!r}")
        return os.path.join("/tmp", name)
    return model_id


def _has_safetensors(path: str) -> bool:
    if not os.path.isfile(os.path.join(path, "config.json")):
        return False
    index_file = os.path.join(path, "model.safetensors.index.json")
    if os.path.exists(index_file):
        try:
            with open(index_file) as f:
                weight_map = json.load(f)["weight_map"]
        except Exception:
            return False
        shards = set(weight_map.values())
        return bool(shards) and all(
            os.path.isfile(os.path.join(path, s)) for s in shards
        )
    files = [
        os.path.basename(p) for p in glob.glob(os.path.join(path, "*.safetensors"))
    ]
    return bool(files) and not any("-of-" in f for f in files)


def stage_model(model_id: str) -> str:
    target_dir = model_dir(model_id)
    if _has_safetensors(target_dir):
        return target_dir
    if not model_id.startswith("gs://"):
        raise FileNotFoundError(f"no safetensors in {target_dir}")

    fs = gcsfs.GCSFileSystem()
    prefix = model_id.removeprefix("gs://").rstrip("/")
    entries = [
        e
        for e in fs.ls(prefix, detail=True)
        if e.get("type") != "directory" and not e["name"].endswith("/")
    ]
    parent = os.path.dirname(target_dir)
    basename = os.path.basename(target_dir)
    tmp_dir = tempfile.mkdtemp(prefix=f".{basename}.", dir=parent)
    try:
        if entries:
            rpaths = [e["name"] for e in entries]
            lpaths = [os.path.join(tmp_dir, os.path.basename(p)) for p in rpaths]
            fs.get(rpaths, lpaths)
        if not _has_safetensors(tmp_dir):
            raise FileNotFoundError(f"no safetensors in {target_dir}")
        shutil.rmtree(target_dir, ignore_errors=True)
        os.replace(tmp_dir, target_dir)
    except BaseException:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        raise
    return target_dir


def load_weights(model_id: str) -> dict[str, torch.Tensor]:
    model = AutoModelForCausalLM.from_pretrained(
        model_dir(model_id), dtype=torch.bfloat16
    )
    state = model.state_dict()
    for tensor in state.values():
        tensor.share_memory_()
    return state


def random_full(
    seed: int,
    key: str,
    shape: torch.Size | tuple[int, ...],
    device: torch.device | str,
) -> torch.Tensor:
    digest = hashlib.sha256(f"{seed}|{key}".encode()).digest()
    generator = torch.Generator(device=device).manual_seed(
        int.from_bytes(digest[:8], "little")
    )
    return torch.randn(shape, generator=generator, dtype=torch.float32, device=device)


def local_slice(full: torch.Tensor, t: torch.Tensor) -> torch.Tensor:
    if not isinstance(t, DTensor):
        return full
    shape, offset = compute_local_shape_and_global_offset(
        t.shape, t.device_mesh, t.placements
    )
    slices = tuple(slice(o, o + s) for s, o in zip(shape, offset))
    return full[slices]
