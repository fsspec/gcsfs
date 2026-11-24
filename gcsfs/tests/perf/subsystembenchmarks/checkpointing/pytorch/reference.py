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
from gcsfs.tests.perf.subsystembenchmarks.checkpointing._dist import (
    _ensure_open_file_limit,
)


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
    _ensure_open_file_limit()
    model = AutoModelForCausalLM.from_pretrained(
        model_dir(model_id), dtype=torch.bfloat16
    )
    state = model.state_dict()
    del model
    for tensor in state.values():
        tensor.share_memory_()
    return state


def _random_slice(
    seed: int,
    key: str,
    global_shape: tuple[int, ...],
    offsets: tuple[int, ...],
    local_shape: tuple[int, ...],
    device: torch.device | str,
) -> torch.Tensor:
    dev = torch.device(device)
    digest = hashlib.sha256(f"{seed}|{key}".encode()).digest()
    gen = torch.Generator(device="cpu").manual_seed(
        int.from_bytes(digest[:8], "little")
    )
    ndim = len(global_shape)
    if ndim == 0:
        return torch.randn((), generator=gen, dtype=torch.float32, device="cpu").to(dev)
    scale = ndim**-0.5
    out: torch.Tensor | None = None
    for d, (full_n, off, local_n) in enumerate(zip(global_shape, offsets, local_shape)):
        axis = (
            torch.randn((full_n,), generator=gen, dtype=torch.float32, device="cpu")
            .mul_(scale)[off : off + local_n]
            .to(dev)
        )
        view_shape = [1] * ndim
        view_shape[d] = local_n
        term = axis.reshape(view_shape)
        out = term.expand(local_shape).clone() if out is None else out.add_(term)
    assert out is not None
    return out


def random_local(
    seed: int,
    key: str,
    target: torch.Tensor,
    device: torch.device | str,
) -> torch.Tensor:
    if isinstance(target, DTensor):
        local_shape, offsets = compute_local_shape_and_global_offset(
            target.shape, target.device_mesh, target.placements
        )
    else:
        local_shape = tuple(target.shape)
        offsets = (0,) * target.ndim
    return _random_slice(
        seed, key, tuple(target.shape), tuple(offsets), tuple(local_shape), device
    )


def random_full(
    seed: int,
    key: str,
    shape: torch.Size | tuple[int, ...],
    device: torch.device | str,
) -> torch.Tensor:
    shape_t = tuple(shape)
    return _random_slice(seed, key, shape_t, (0,) * len(shape_t), shape_t, device)


def local_slice(full: torch.Tensor, t: torch.Tensor) -> torch.Tensor:
    if not isinstance(t, DTensor):
        return full
    shape, offset = compute_local_shape_and_global_offset(
        t.shape, t.device_mesh, t.placements
    )
    slices = tuple(slice(o, o + s) for s, o in zip(shape, offset))
    return full[slices]
