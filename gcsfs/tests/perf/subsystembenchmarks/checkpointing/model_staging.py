"""Lazy local staging of GCS-hosted Hugging Face model directories."""

from __future__ import annotations

import contextlib
import fcntl
import glob
import hashlib
import json
import os
import shutil
import tempfile
from collections.abc import Iterator

import gcsfs

MODEL_CACHE_DIR_ENV = "GCSFS_SUBSYSTEM_MODEL_CACHE_DIR"
_INDEX_FILE = "model.safetensors.index.json"


def cache_root() -> str:
    """Return the directory holding staged ``gs://`` models."""
    return os.environ.get(MODEL_CACHE_DIR_ENV, "").strip() or tempfile.gettempdir()


def model_dir(model_id: str) -> str:
    """Return the local directory for ``model_id``.

    A ``gs://bucket/path/<name>`` URI maps to ``<cache_root>/<name>-<hash>``, where the
    hash covers the bucket and object path so URIs sharing a basename do not collide.
    Local paths and hub IDs are returned unchanged.
    """
    if not model_id.startswith("gs://"):
        return model_id
    path = model_id.removeprefix("gs://").rstrip("/")
    parts = path.split("/")
    if any(part in ("", ".", "..") for part in parts):
        raise ValueError(f"Invalid GCS model_id {model_id!r}")
    digest = hashlib.sha256(path.encode()).hexdigest()[:16]
    return os.path.join(cache_root(), f"{parts[-1]}-{digest}")


def safetensors_files(path: str) -> list[str]:
    """Return the complete set of safetensors weight files in ``path``, or ``[]``.

    With ``model.safetensors.index.json`` these are the shards it lists, all of which
    must exist. Without an index they are the unsharded ``*.safetensors`` files; hidden
    files (such as the canonical copy) are ignored, and a lone ``-of-`` shard counts as
    incomplete.
    """
    index_file = os.path.join(path, _INDEX_FILE)
    if os.path.exists(index_file):
        try:
            with open(index_file, encoding="utf-8") as f:
                index = json.load(f)
        except (OSError, ValueError):
            return []
        weight_map = index.get("weight_map") if isinstance(index, dict) else None
        if not isinstance(weight_map, dict) or not weight_map:
            return []
        shards = sorted(set(weight_map.values()))
        if not all(_is_plain_filename(s) for s in shards):
            return []
        paths = [os.path.join(path, s) for s in shards]
        return paths if all(os.path.isfile(p) for p in paths) else []
    paths = sorted(
        p
        for p in glob.glob(os.path.join(path, "*.safetensors"))
        if not os.path.basename(p).startswith(".")
    )
    if any("-of-" in os.path.basename(p) for p in paths):
        return []
    return paths


def _is_plain_filename(name: object) -> bool:
    return (
        isinstance(name, str)
        and name not in ("", ".", "..")
        and os.path.basename(name) == name
    )


def _is_staged(path: str) -> bool:
    return os.path.isfile(os.path.join(path, "config.json")) and bool(
        safetensors_files(path)
    )


@contextlib.contextmanager
def _exclusive_lock(lock_path: str) -> Iterator[None]:
    """Hold an exclusive ``flock`` on ``lock_path`` (created ``0o600``)."""
    fd = os.open(lock_path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        yield
    finally:
        os.close(fd)


def _download(fs: gcsfs.GCSFileSystem, prefix: str, dest_dir: str) -> None:
    """Copy the objects directly under ``prefix`` into ``dest_dir``."""
    real_dest = os.path.realpath(dest_dir)
    rpaths, lpaths = [], []
    for entry in fs.ls(prefix, detail=True):
        if entry.get("type") == "directory" or entry["name"].endswith("/"):
            continue
        fname = os.path.basename(entry["name"])
        if not _is_plain_filename(fname):
            raise ValueError(f"Invalid remote object key {entry['name']!r}")
        lpath = os.path.join(dest_dir, fname)
        if os.path.commonpath([real_dest, os.path.realpath(lpath)]) != real_dest:
            raise ValueError(f"Refusing to write outside {dest_dir}: {entry['name']!r}")
        rpaths.append(entry["name"])
        lpaths.append(lpath)
    if rpaths:
        fs.get(rpaths, lpaths)


def stage_model(model_id: str) -> str:
    """Return a local directory with ``model_id``'s config and safetensors weights.

    A ``gs://`` model is downloaded into its cache entry unless already staged. A file
    lock serializes stagers of the same entry (concurrent benchmark processes on one
    host), and the download goes to a temporary directory that is renamed into place,
    so a complete entry is never modified.
    """
    target_dir = model_dir(model_id)
    if not model_id.startswith("gs://"):
        if not _is_staged(target_dir):
            raise FileNotFoundError(f"no safetensors in {target_dir}")
        return target_dir

    parent = os.path.dirname(target_dir)
    os.makedirs(parent, exist_ok=True)
    with _exclusive_lock(f"{target_dir}.lock"):
        if _is_staged(target_dir):
            return target_dir
        tmp_dir = tempfile.mkdtemp(
            prefix=f".{os.path.basename(target_dir)}.", dir=parent
        )
        try:
            _download(
                gcsfs.GCSFileSystem(),
                model_id.removeprefix("gs://").rstrip("/"),
                tmp_dir,
            )
            if not _is_staged(tmp_dir):
                raise FileNotFoundError(f"no safetensors in {model_id}")
            # Only an incomplete earlier attempt can be here; holding the lock means
            # no other stager is writing it.
            shutil.rmtree(target_dir, ignore_errors=True)
            os.replace(tmp_dir, target_dir)
        except BaseException:
            shutil.rmtree(tmp_dir, ignore_errors=True)
            raise
    return target_dir


def stage_gcs_model(model_id: str) -> str:
    """Stage ``model_id`` if it is a ``gs://`` URI; return other IDs unchanged."""
    if model_id.startswith("gs://"):
        return stage_model(model_id)
    return model_id
