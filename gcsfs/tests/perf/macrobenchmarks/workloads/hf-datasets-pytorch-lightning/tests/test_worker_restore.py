"""Regression tests for HF datasets >= 5.1 + StatefulDataLoader worker restore."""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

datasets = pytest.importorskip("datasets")
pytest.importorskip("datasets.distributed")
pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")
torch = pytest.importorskip("torch")
StatefulDataLoader = pytest.importorskip(
    "torchdata.stateful_dataloader"
).StatefulDataLoader

WORKLOAD_DIR = Path(__file__).resolve().parents[1]
TRAIN_SCRIPT = WORKLOAD_DIR / "helm_chart" / "train_pytorch_lightning.py"


def _load_patch_fn():
    """Extract ``_patch_datasets_worker_restore`` from train_pytorch_lightning.py
    without executing its top-level env/tokenizer initialization.
    """
    source = TRAIN_SCRIPT.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(TRAIN_SCRIPT))
    fn_nodes = [
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef)
        and node.name == "_patch_datasets_worker_restore"
    ]
    assert len(fn_nodes) == 1, "_patch_datasets_worker_restore not found"

    # Also verify it is invoked at module top-level so forkserver workers apply it.
    top_level_calls = [
        node
        for node in tree.body
        if isinstance(node, ast.Expr)
        and isinstance(node.value, ast.Call)
        and isinstance(node.value.func, ast.Name)
        and node.value.func.id == "_patch_datasets_worker_restore"
    ]
    assert len(top_level_calls) == 1, (
        "_patch_datasets_worker_restore must be called at module top-level "
        "so forkserver workers apply it on import"
    )

    mod_ast = ast.Module(body=fn_nodes, type_ignores=[])
    ns = {"datasets": datasets, "torch": torch}
    exec(compile(mod_ast, str(TRAIN_SCRIPT), "exec"), ns)
    return ns["_patch_datasets_worker_restore"]


def _make_parquet_shards(root: Path, num_shards: int = 20, rows_per_shard: int = 50):
    for i in range(num_shards):
        table = pa.table(
            {"text": [f"shard-{i:02d}-row-{j:03d}" for j in range(rows_per_shard)]}
        )
        pq.write_table(table, root / f"part-{i:05d}.parquet")


def _build_ds(root: Path, *, rank: int = 0, world_size: int = 8):
    ds = datasets.load_dataset(
        "parquet",
        data_files=f"{root}/*.parquet",
        split="train",
        streaming=True,
        columns=["text"],
    )
    ds = ds.shuffle(seed=42, buffer_size=100, max_buffer_input_shards=10)
    return datasets.distributed.split_dataset_by_node(
        ds, rank=rank, world_size=world_size
    )


def _collate_text(examples):
    return [ex["text"] for ex in examples]


def test_multiworker_stateful_dataloader_restore_succeeds_with_patch(
    tmp_path, monkeypatch
):
    """Restoring a multi-worker StatefulDataLoader on shuffled streaming Parquet
    must not raise IndexError in datasets._inner_load_state_dict.
    """
    _make_parquet_shards(tmp_path, num_shards=20, rows_per_shard=50)

    # Save a checkpoint state_dict after pulling batches from all 4 workers.
    loader1 = StatefulDataLoader(
        _build_ds(tmp_path),
        batch_size=4,
        collate_fn=_collate_text,
        num_workers=4,
        persistent_workers=True,
        prefetch_factor=2,
        multiprocessing_context="fork",
    )
    it1 = iter(loader1)
    for _ in range(5):
        next(it1)
    saved_state = loader1.state_dict()
    del it1, loader1

    # Apply patch (and verify idempotency) via monkeypatch so test state is clean.
    orig_prepare = datasets.IterableDataset._prepare_ex_iterable_for_iteration
    monkeypatch.setattr(
        datasets.IterableDataset, "_prepare_ex_iterable_for_iteration", orig_prepare
    )
    patch_fn = _load_patch_fn()
    patch_fn()
    patched_once = datasets.IterableDataset._prepare_ex_iterable_for_iteration
    patch_fn()
    assert (
        datasets.IterableDataset._prepare_ex_iterable_for_iteration is patched_once
    ), "patch must be idempotent"

    # Restore and pull batches across all 4 workers.
    loader2 = StatefulDataLoader(
        _build_ds(tmp_path),
        batch_size=4,
        collate_fn=_collate_text,
        num_workers=4,
        persistent_workers=True,
        prefetch_factor=2,
        multiprocessing_context="fork",
    )
    loader2.load_state_dict(saved_state)
    it2 = iter(loader2)
    resumed_batches = [next(it2) for _ in range(5)]
    assert len(resumed_batches) == 5
    assert all(len(b) == 4 for b in resumed_batches)
    del it2, loader2


def test_single_process_restore_still_loads_state_with_patch(tmp_path, monkeypatch):
    """With num_workers=0 (get_worker_info() is None), restore still resumes state."""
    _make_parquet_shards(tmp_path, num_shards=8, rows_per_shard=40)

    orig_prepare = datasets.IterableDataset._prepare_ex_iterable_for_iteration
    monkeypatch.setattr(
        datasets.IterableDataset, "_prepare_ex_iterable_for_iteration", orig_prepare
    )
    _load_patch_fn()()

    loader1 = StatefulDataLoader(
        _build_ds(tmp_path, world_size=1),
        batch_size=4,
        collate_fn=_collate_text,
        num_workers=0,
    )
    it1 = iter(loader1)
    for _ in range(3):
        next(it1)
    saved_state = loader1.state_dict()
    del it1, loader1

    loader2 = StatefulDataLoader(
        _build_ds(tmp_path, world_size=1),
        batch_size=4,
        collate_fn=_collate_text,
        num_workers=0,
    )
    loader2.load_state_dict(saved_state)
    it2 = iter(loader2)
    resumed = [next(it2) for _ in range(3)]
    assert len(resumed) == 3
    assert all(len(b) == 4 for b in resumed)


def test_patch_noops_when_private_hook_missing(monkeypatch):
    """If a future datasets release removes _prepare_ex_iterable_for_iteration,
    patching must no-op instead of raising AttributeError at import time.
    """
    monkeypatch.delattr(
        datasets.IterableDataset, "_prepare_ex_iterable_for_iteration", raising=False
    )
    _load_patch_fn()()
    assert not hasattr(datasets.IterableDataset, "_prepare_ex_iterable_for_iteration")
