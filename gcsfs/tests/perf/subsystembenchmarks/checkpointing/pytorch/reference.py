from __future__ import annotations

import glob
import hashlib
import json
import os
import tempfile
from collections.abc import Iterator, Mapping, Sequence

import torch
from safetensors.torch import safe_open, save_file
from torch.distributed.tensor import DTensor
from torch.distributed.tensor._utils import compute_local_shape_and_global_offset
from transformers import AutoModelForCausalLM

from gcsfs.tests.perf.subsystembenchmarks.checkpointing.model_staging import (
    model_dir,
    stage_model,
)

__all__ = [
    "CANONICAL_FILENAME",
    "MmapWeightStore",
    "ensure_canonical_weights",
    "load_weights",
    "local_slice",
    "model_dir",
    "random_full",
    "random_local",
    "stage_model",
]

CANONICAL_FILENAME = ".canonical.safetensors"


def _source_safetensors(staged_dir: str) -> list[str]:
    index_file = os.path.join(staged_dir, "model.safetensors.index.json")
    if os.path.isfile(index_file):
        try:
            with open(index_file) as f:
                weight_map = json.load(f)["weight_map"]
            shards = sorted(set(weight_map.values()))
            paths = [os.path.join(staged_dir, s) for s in shards]
            if paths and all(os.path.isfile(p) for p in paths):
                return paths
        except Exception:
            pass
    paths = sorted(
        p
        for p in glob.glob(os.path.join(staged_dir, "*.safetensors"))
        if not os.path.basename(p).startswith(".")
    )
    if not paths:
        raise FileNotFoundError(f"no safetensors in {staged_dir}")
    return paths


def _safetensors_match_params(
    paths: Sequence[str], param_shapes: Mapping[str, tuple[int, ...]]
) -> bool:
    seen: set[str] = set()
    try:
        for path in paths:
            with safe_open(path, framework="pt", device="cpu") as sf:
                for key in sf.keys():
                    if key not in param_shapes or key in seen:
                        return False
                    sl = sf.get_slice(key)
                    if (
                        tuple(sl.get_shape()) != param_shapes[key]
                        or sl.get_dtype() != "BF16"
                    ):
                        return False
                    seen.add(key)
    except Exception:
        return False
    return len(seen) == len(param_shapes)


class MmapWeightStore(Mapping[str, torch.Tensor]):
    """Read-only mmap-backed safetensors weight store shared via the OS page cache."""

    def __init__(self, paths: Sequence[str]) -> None:
        self._paths: tuple[str, ...] = tuple(paths)
        self._handles: list | None = None
        self._closed: bool = False
        self._key_to_idx: dict[str, int] = {}
        self._open_handles()
        assert self._handles is not None
        for idx, handle in enumerate(self._handles):
            for key in handle.keys():
                self._key_to_idx[key] = idx

    def _open_handles(self) -> None:
        handles = []
        try:
            for path in self._paths:
                handle = safe_open(path, framework="pt", device="cpu")
                handle.__enter__()
                handles.append(handle)
        except BaseException:
            for handle in handles:
                try:
                    handle.__exit__(None, None, None)
                except Exception:
                    pass
            raise
        self._handles = handles

    def _handle_for(self, key: str):
        if self._closed:
            raise ValueError("MmapWeightStore is closed")
        idx = self._key_to_idx[key]
        if self._handles is None:
            self._open_handles()
        assert self._handles is not None
        return self._handles[idx]

    def get_shape(self, key: str) -> tuple[int, ...]:
        return tuple(self._handle_for(key).get_slice(key).get_shape())

    def get_slice(self, key: str, slices: tuple[slice, ...]) -> torch.Tensor:
        tensor = self._handle_for(key).get_slice(key)[slices]
        return (
            tensor
            if tensor.dtype == torch.bfloat16
            else tensor.to(dtype=torch.bfloat16)
        )

    def __getitem__(self, key: str) -> torch.Tensor:
        tensor = self._handle_for(key).get_tensor(key)
        return (
            tensor
            if tensor.dtype == torch.bfloat16
            else tensor.to(dtype=torch.bfloat16)
        )

    def __iter__(self) -> Iterator[str]:
        return iter(self._key_to_idx)

    def __len__(self) -> int:
        return len(self._key_to_idx)

    def __contains__(self, key: object) -> bool:
        return key in self._key_to_idx

    def close(self) -> None:
        handles = getattr(self, "_handles", None)
        if handles is not None:
            for handle in handles:
                try:
                    handle.__exit__(None, None, None)
                except Exception:
                    pass
            self._handles = None
        key_to_idx = getattr(self, "_key_to_idx", None)
        if key_to_idx is not None:
            key_to_idx.clear()
        self._closed = True

    clear = close

    def __enter__(self) -> MmapWeightStore:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass

    def __getstate__(self) -> dict:
        return {
            "paths": self._paths,
            "key_to_idx": self._key_to_idx,
            "closed": self._closed,
        }

    def __setstate__(self, state: dict) -> None:
        self._paths = state["paths"]
        self._key_to_idx = state["key_to_idx"]
        self._closed = state["closed"]
        self._handles = None


def ensure_canonical_weights(model_id: str) -> str:
    """Ensure staged weights match ``build_model`` FQNs/shapes/bf16, writing ``.canonical.safetensors`` if needed."""
    from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.model import (
        build_model,
    )

    staged_dir = model_dir(model_id)
    meta_model = build_model(model_id)
    param_shapes = {name: tuple(p.shape) for name, p in meta_model.named_parameters()}
    del meta_model

    source_shards = _source_safetensors(staged_dir)
    canonical_path = os.path.join(staged_dir, CANONICAL_FILENAME)

    if _safetensors_match_params(source_shards, param_shapes):
        if os.path.exists(canonical_path):
            try:
                os.unlink(canonical_path)
            except FileNotFoundError:
                pass
        return staged_dir

    if os.path.isfile(canonical_path):
        max_source_mtime = max(os.path.getmtime(p) for p in source_shards)
        if os.path.getmtime(
            canonical_path
        ) >= max_source_mtime and _safetensors_match_params(
            [canonical_path], param_shapes
        ):
            return staged_dir

    model = AutoModelForCausalLM.from_pretrained(
        staged_dir, dtype=torch.bfloat16, local_files_only=True
    )
    state = model.state_dict()
    del model

    seen_ptrs: set[int] = set()
    tensors: dict[str, torch.Tensor] = {}
    for key, tensor in state.items():
        t = tensor.detach().to(device="cpu", dtype=torch.bfloat16)
        if not t.is_contiguous() or t.data_ptr() in seen_ptrs:
            t = t.clone().contiguous()
        seen_ptrs.add(t.data_ptr())
        tensors[key] = t
    del state

    tmp_path: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            dir=staged_dir,
            prefix=".canonical.",
            suffix=".tmp",
            delete=False,
        ) as tmp:
            tmp_path = tmp.name
        save_file(tensors, tmp_path)
        os.replace(tmp_path, canonical_path)
        tmp_path = None
    finally:
        if tmp_path is not None and os.path.exists(tmp_path):
            try:
                os.unlink(tmp_path)
            except OSError:
                pass

    return staged_dir


def load_weights(model_id: str) -> MmapWeightStore:
    staged_dir = ensure_canonical_weights(model_id)
    canonical_path = os.path.join(staged_dir, CANONICAL_FILENAME)
    if os.path.isfile(canonical_path):
        return MmapWeightStore([canonical_path])
    return MmapWeightStore(_source_safetensors(staged_dir))


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
