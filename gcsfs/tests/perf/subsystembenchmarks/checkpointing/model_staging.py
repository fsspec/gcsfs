"""Lazy local staging of GCS-hosted Hugging Face model directories."""

from __future__ import annotations

import glob
import json
import os
import shutil
import tempfile

import gcsfs


def model_dir(model_id: str) -> str:
    """Resolve ``model_id`` to a validated local path under ``/tmp`` for ``gs://`` URIs."""
    if model_id.startswith("gs://"):
        name = os.path.basename(model_id.removeprefix("gs://").rstrip("/"))
        if not name or name in {".", ".."}:
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
    """Download ``gs://`` model weights to ``/tmp/<model>`` atomically if not already staged."""
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
            real_tmp = os.path.realpath(tmp_dir)
            rpaths = []
            lpaths = []
            for e in entries:
                fname = os.path.basename(e["name"])
                if not fname or fname in {".", ".."}:
                    raise ValueError(f"Invalid remote object key {e['name']!r}")
                lpath = os.path.join(tmp_dir, fname)
                if os.path.commonpath([real_tmp, os.path.realpath(lpath)]) != real_tmp:
                    raise ValueError(
                        f"Refusing to write outside {tmp_dir}: {e['name']!r}"
                    )
                rpaths.append(e["name"])
                lpaths.append(lpath)
            fs.get(rpaths, lpaths)
        if not _has_safetensors(tmp_dir):
            raise FileNotFoundError(f"no safetensors in {target_dir}")
        shutil.rmtree(target_dir, ignore_errors=True)
        os.replace(tmp_dir, target_dir)
    except BaseException:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        raise
    return target_dir


def stage_gcs_model(model_id: str) -> str:
    """Stage ``model_id`` if it is a ``gs://`` URI; return non-GCS IDs unchanged."""
    if model_id and model_id.startswith("gs://"):
        return stage_model(model_id)
    return model_id
