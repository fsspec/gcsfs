"""Hugging Face read driver wiring for CPU/GPU delivery.

Helper behavior is covered in dataloading/tests/test_device.py; these tests
check the driver binds the right rank, pins on GPU, and feeds batches.
"""

import dataclasses

import pytest

torch = pytest.importorskip("torch")
pytest.importorskip("datasets")
pytest.importorskip("pyarrow")

from gcsfs.tests.perf.subsystembenchmarks._common import accelerator  # noqa: E402
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
    monkeypatch.setattr(accelerator, "synchronize", calls.append)
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


def test_build_loader_spawns_workers_and_keeps_in_process_loading_unchanged():
    kwargs = dict(batch_size=2, prefetch_factor=2)
    in_process = _hf_read._build_loader([1, 2], num_workers=0, **kwargs)
    assert in_process.multiprocessing_context is None
    with_workers = _hf_read._build_loader([1, 2], num_workers=2, **kwargs)
    assert with_workers.multiprocessing_context.get_start_method() == "spawn"


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
    monkeypatch.setattr(accelerator, "device_kind", lambda: "cpu")

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


def test_hf_driver_split_by_node_uses_distributed_runner(tmp_path, monkeypatch):
    prefix, manifest = _ingest(tmp_path)
    calls = []

    def fake_distributed(kind, pfx, params, **kwargs):
        calls.append((kind, pfx, params, kwargs))
        return ([0.1, 0.2], [manifest["sample_count"]] * 2, 0.05, 0.01)

    monkeypatch.setattr(_hf_read, "run_rank_epochs_distributed", fake_distributed)
    params = _params(manifest, split_by_node=True, world_size=2)
    result = _hf_read.HFReadDriver().run_read(prefix, params, manifest)

    assert len(calls) == 1
    assert calls[0][0] is _hf_read.worker_entry
    assert calls[0][1] == prefix
    assert calls[0][2] == params
    assert result.rows_per_epoch == [manifest["sample_count"]] * 2


def test_tpu_single_rank_isolates_in_subprocess(tmp_path, monkeypatch):
    """split_by_node=False on TPU must not bind TPU in pytest parent."""
    prefix, manifest = _ingest(tmp_path)
    monkeypatch.setattr(accelerator, "device_kind", lambda: "tpu")
    parent_binds = []
    monkeypatch.setattr(
        _hf_read.device_lib, "rank_device", lambda r: parent_binds.append(r)
    )
    isolated = []

    def fake_isolated(kind, pfx, params, *, extra=None):
        isolated.append((kind, pfx, params.world_size, params.split_by_node, extra))
        return ([0.1, 0.2], [manifest["sample_count"]] * 2, 0.05, 0.01)

    monkeypatch.setattr(_hf_read, "run_single_rank_isolated", fake_isolated)
    params = _params(manifest, split_by_node=False, world_size=4)
    result = _hf_read.HFReadDriver().run_read(prefix, params, manifest)

    assert parent_binds == []
    assert isolated == [(_hf_read.worker_entry, prefix, 1, False, {"split": False})]
    assert result.rows_per_epoch == [manifest["sample_count"]] * 2


def test_run_rank_epochs_split_false_skips_node_split(
    tmp_path, monkeypatch, sync_calls, ranks_on_fake_gpu, loader_kwargs
):
    prefix, manifest = _ingest(tmp_path)

    def no_split(*args):
        raise AssertionError("split=False must not split by node")

    monkeypatch.setattr(_hf_read, "_split", no_split)
    per_epoch, _, _ = _hf_read.run_rank_epochs(
        0, 2, prefix, _params(manifest, world_size=2), split=False
    )

    assert [rows for _, _, rows in per_epoch] == [manifest["sample_count"]] * 2


def test_hf_driver_consults_device_pin_memory(
    tmp_path, monkeypatch, sync_calls, ranks_on_fake_gpu, loader_kwargs
):
    prefix, manifest = _ingest(tmp_path)
    pin_calls = []
    monkeypatch.setattr(
        _hf_read.device_lib,
        "pin_memory",
        lambda dev: pin_calls.append(dev) or False,
    )

    _hf_read.run_epochs(
        prefix=prefix,
        fmt="pretok_parquet",
        access="sequential",
        num_workers=0,
        batch_size=8,
        rounds=1,
    )
    _hf_read.run_rank_epochs(
        0, 1, prefix, _params(manifest, rounds=1, world_size=1), split=False
    )

    assert pin_calls == [FAKE_GPU, FAKE_GPU]
    assert [kw["pin_memory"] for kw in loader_kwargs] == [False, False]


def test_worker_entry_forwards_split_extra(
    tmp_path, monkeypatch, sync_calls, ranks_on_fake_gpu, loader_kwargs
):
    prefix, manifest = _ingest(tmp_path)

    def no_split(*args):
        raise AssertionError("split=False must not split by node")

    monkeypatch.setattr(_hf_read, "_split", no_split)
    per_epoch, _, _ = _hf_read.worker_entry(
        0,
        1,
        prefix,
        _params(manifest, rounds=1, world_size=1),
        None,
        {"split": False},
    )

    assert [rows for _, _, rows in per_epoch] == [manifest["sample_count"]]
