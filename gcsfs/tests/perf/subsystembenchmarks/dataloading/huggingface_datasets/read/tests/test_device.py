"""Hugging Face read driver wiring for CPU/GPU delivery.

Helper behavior is covered in dataloading/tests/test_device.py; these tests
check the driver binds the right rank, pins on GPU, and feeds batches.
"""

import dataclasses

import pytest

torch = pytest.importorskip("torch")
pytest.importorskip("datasets")
pytest.importorskip("pyarrow")

from gcsfs.tests.perf.subsystembenchmarks.dataloading import datagen  # noqa: E402
from gcsfs.tests.perf.subsystembenchmarks.dataloading.huggingface_datasets.parameters import (  # noqa: E402
    HFReadParameters,
)
from gcsfs.tests.perf.subsystembenchmarks.dataloading.huggingface_datasets.read import (  # noqa: E402
    driver as _hf_read,
)

FAKE_GPU = torch.device("meta")


def _ingest(tmp_path, files=4, rows=20):
    prefix = f"file://{tmp_path}/data/"
    manifest = datagen.ingest_dataset(
        prefix,
        fmt="pretok_parquet",
        seq_len=8,
        file_count=files,
        rows_per_file=rows,
        row_group_size=10,
    )
    return prefix, manifest


def _params(manifest, **overrides):
    params = HFReadParameters(
        name="device",
        bucket_name="",
        bucket_type="regional",
        rounds=2,
        scenario="read",
        framework="huggingface_datasets",
        fmt="pretok_parquet",
        seq_len=8,
        file_count=manifest["file_count"],
        rows_per_file=manifest["rows_per_file"],
        row_group_size=10,
        access="sequential",
        num_workers=0,
        batch_size=8,
    )
    return dataclasses.replace(params, **overrides)


@pytest.fixture
def sync_calls(monkeypatch):
    calls = []
    monkeypatch.setattr(torch.cuda, "synchronize", calls.append)
    return calls


@pytest.fixture
def ranks_on_fake_gpu(monkeypatch):
    ranks = []
    monkeypatch.setattr(
        _hf_read.device_lib,
        "rank_device",
        lambda rank: ranks.append(rank) or FAKE_GPU,
    )
    return ranks


@pytest.fixture
def loader_kwargs(monkeypatch):
    """Record _build_loader kwargs; build unpinned (no accelerator to pin for)."""
    recorded = []
    real_build_loader = _hf_read._build_loader

    def build_loader(ds, **kwargs):
        recorded.append(kwargs)
        return real_build_loader(ds, **{**kwargs, "pin_memory": False})

    monkeypatch.setattr(_hf_read, "_build_loader", build_loader)
    return recorded


def test_build_loader_pins_memory_only_when_requested():
    kwargs = dict(batch_size=2, num_workers=0, prefetch_factor=2)
    assert _hf_read._build_loader([1, 2], **kwargs).pin_memory is False
    assert _hf_read._build_loader([1, 2], pin_memory=True, **kwargs).pin_memory


def test_run_epochs_feeds_rank0_device_with_pinned_loader(
    tmp_path, sync_calls, ranks_on_fake_gpu, loader_kwargs
):
    prefix, manifest = _ingest(tmp_path)

    _, rows, _, _ = _hf_read.run_epochs(
        prefix=prefix,
        fmt="pretok_parquet",
        access="sequential",
        num_workers=0,
        batch_size=8,
        rounds=2,
    )

    assert rows == [manifest["sample_count"]] * 2
    assert ranks_on_fake_gpu == [0]
    assert loader_kwargs[0]["pin_memory"] is True
    assert sync_calls == [FAKE_GPU, FAKE_GPU]


def test_run_epochs_on_cpu_neither_pins_nor_synchronizes(
    tmp_path, monkeypatch, sync_calls, loader_kwargs
):
    prefix, manifest = _ingest(tmp_path)
    monkeypatch.setattr(_hf_read.device_lib.env, "detect_accelerator", lambda: "cpu")

    _, rows, _, _ = _hf_read.run_epochs(
        prefix=prefix,
        fmt="pretok_parquet",
        access="sequential",
        num_workers=0,
        batch_size=8,
        rounds=1,
    )

    assert rows == [manifest["sample_count"]]
    assert loader_kwargs[0]["pin_memory"] is False
    assert sync_calls == []


def test_run_rank_epochs_feeds_its_own_rank_device(
    tmp_path, sync_calls, ranks_on_fake_gpu, loader_kwargs
):
    prefix, manifest = _ingest(tmp_path)

    per_epoch, _, _ = _hf_read.run_rank_epochs(
        1, 2, prefix, _params(manifest, split_by_node=True, world_size=2)
    )

    assert [rows for _, _, rows in per_epoch] == [manifest["sample_count"] // 2] * 2
    assert ranks_on_fake_gpu == [1]
    assert loader_kwargs[0]["pin_memory"] is True
    assert sync_calls == [FAKE_GPU, FAKE_GPU]


@pytest.mark.skipif(not torch.cuda.is_available(), reason="requires a CUDA GPU")
def test_run_epochs_delivers_batches_on_real_gpu(tmp_path):
    prefix, manifest = _ingest(tmp_path)
    device = _hf_read.device_lib.rank_device(0)
    loader = _hf_read._build_loader(
        _hf_read._build_dataset(prefix, "pretok_parquet", "sequential", 0).with_format(
            "torch"
        ),
        batch_size=8,
        num_workers=0,
        prefetch_factor=2,
        pin_memory=True,
    )

    batches = list(_hf_read.device_lib.feed(loader, device))

    assert sum(len(b["label"]) for b in batches) == manifest["sample_count"]
    assert all(b["label"].device == device for b in batches)
