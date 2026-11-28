"""WebDataset read driver wiring for CPU/GPU delivery."""

import pytest

torch = pytest.importorskip("torch")
pytest.importorskip("webdataset")
pytest.importorskip("PIL")

from gcsfs.tests.perf.subsystembenchmarks._common import accelerator  # noqa: E402
from gcsfs.tests.perf.subsystembenchmarks.dataloading.webdataset import (  # noqa: E402
    configs,
    imagegen,
)
from gcsfs.tests.perf.subsystembenchmarks.dataloading.webdataset.read import (  # noqa: E402
    driver,
)

FAKE_GPU = torch.device("meta")
_TINY_PIXEL_BUDGET = imagegen.FACTOR * imagegen.FACTOR * 4
_FILES, _ROWS = 2, 3


def _params(**overrides):
    cases = configs.WebDatasetReadConfigurator(configs.__file__).generate_cases()
    params = next(c for c in cases if c.sweep_axis == "baseline")
    params.file_count = _FILES
    params.rows_per_file = _ROWS
    params.pixel_budget = _TINY_PIXEL_BUDGET
    params.access = "sequential"
    params.num_workers = 0
    params.batch_size = 2
    params.split_by_node = False
    params.world_size = 1
    params.rounds = 2
    for key, value in overrides.items():
        setattr(params, key, value)
    return params


@pytest.fixture
def prefix(tmp_path):
    root = str(tmp_path) + "/data/"
    imagegen.ingest_tar_shards(
        root,
        fmt="image_tar",
        file_count=_FILES,
        rows_per_file=_ROWS,
        pixel_budget=_TINY_PIXEL_BUDGET,
        image_encoding="jpeg",
        jpeg_quality=75,
        sample_shape="pairs",
    )
    return root


@pytest.fixture
def sync_calls(monkeypatch):
    calls = []
    monkeypatch.setattr(accelerator, "synchronize", calls.append)
    return calls


@pytest.fixture
def ranks_on_fake_gpu(monkeypatch):
    ranks = []
    monkeypatch.setattr(
        driver.device_lib, "rank_device", lambda rank: ranks.append(rank) or FAKE_GPU
    )
    return ranks


@pytest.fixture
def loader_kwargs(monkeypatch):
    """Record build_loader kwargs; build unpinned (no accelerator to pin for)."""
    recorded = []
    real_build_loader = driver.build_loader

    def build_loader(dataset, params, **kwargs):
        recorded.append(kwargs)
        return real_build_loader(dataset, params, **{**kwargs, "pin_memory": False})

    monkeypatch.setattr(driver, "build_loader", build_loader)
    return recorded


def test_build_loader_pins_memory_only_when_requested(prefix):
    params = _params()
    dataset = driver.build_dataset(prefix, params, split_by_node=False)
    assert driver.build_loader(dataset, params).pin_memory is False
    assert driver.build_loader(dataset, params, pin_memory=True).pin_memory is True


def test_run_rank_epochs_feeds_its_rank_device_with_pinned_loader(
    prefix, sync_calls, ranks_on_fake_gpu, loader_kwargs
):
    per_epoch, _, _ = driver.run_rank_epochs(
        0, 1, prefix, _params(decode=True), sample_count=_FILES * _ROWS
    )

    assert [rows for _, _, rows in per_epoch] == [_FILES * _ROWS] * 2
    assert ranks_on_fake_gpu == [0]
    assert loader_kwargs[0]["pin_memory"] is True
    assert sync_calls == [FAKE_GPU, FAKE_GPU]


def test_run_rank_epochs_without_decode_skips_the_gpu(
    prefix, sync_calls, ranks_on_fake_gpu, loader_kwargs
):
    per_epoch, _, _ = driver.run_rank_epochs(
        0, 1, prefix, _params(decode=False), sample_count=_FILES * _ROWS
    )

    assert [rows for _, _, rows in per_epoch] == [_FILES * _ROWS] * 2
    assert ranks_on_fake_gpu == []
    assert loader_kwargs[0]["pin_memory"] is False
    assert sync_calls == []


@pytest.mark.parametrize("decode", [False, True])
def test_feed_moves_decoded_images_and_leaves_raw_bytes_on_host(
    prefix, sync_calls, decode
):
    params = _params(decode=decode)
    with driver.case_read_env(params):
        dataset = driver.build_dataset(prefix, params, split_by_node=False)
        loader = driver.build_loader(dataset, params)
        batches = list(driver.device_lib.feed(loader, FAKE_GPU))

    samples = [sample for batch in batches for sample in batch]
    images = [image for sample in samples for image in sample["images"]]
    assert len(samples) == _FILES * _ROWS
    assert images
    if decode:
        assert all(image.device == FAKE_GPU for image in images)
    else:
        assert all(isinstance(image, bytes) for image in images)
    assert sync_calls == [FAKE_GPU]


def test_resampled_rounds_still_synchronize_after_stopping_at_target(
    prefix, sync_calls, ranks_on_fake_gpu, loader_kwargs
):
    per_epoch, _, _ = driver.run_rank_epochs(
        0,
        1,
        prefix,
        _params(resampled=True, decode=True),
        sample_count=_FILES * _ROWS,
    )

    assert [rows for _, _, rows in per_epoch] == [_FILES * _ROWS] * 2
    assert sync_calls == [FAKE_GPU, FAKE_GPU]


def test_run_rank_epochs_on_cpu_neither_pins_nor_synchronizes(
    prefix, monkeypatch, sync_calls, loader_kwargs
):
    monkeypatch.setattr(accelerator, "device_kind", lambda: "cpu")

    per_epoch, _, _ = driver.run_rank_epochs(
        0, 1, prefix, _params(rounds=1), sample_count=_FILES * _ROWS
    )

    assert per_epoch[0][2] == _FILES * _ROWS
    assert loader_kwargs[0]["pin_memory"] is False
    assert sync_calls == []


@pytest.mark.parametrize("decode", [False, True])
def test_split_run_passes_prepare_tpu_env_matching_decode(prefix, monkeypatch, decode):
    seen = []

    def fake_distributed(kind, pfx, params, *, extra=None, prepare_tpu_env=True):
        seen.append((kind, pfx, params, extra, prepare_tpu_env))
        return ([0.1], [_FILES * _ROWS], 0.05, 0.01)

    monkeypatch.setattr(driver, "run_rank_epochs_distributed", fake_distributed)
    params = _params(split_by_node=True, world_size=2, decode=decode, rounds=1)
    result = driver.WebDatasetReadDriver().run_read(
        prefix, params, {"sample_count": _FILES * _ROWS}
    )

    assert len(seen) == 1
    kind, pfx, got_params, extra, prepare_tpu_env = seen[0]
    assert kind == "wds"
    assert pfx == prefix
    assert got_params == params
    assert extra == {"cache_root": None, "sample_count": _FILES * _ROWS}
    assert prepare_tpu_env is decode
    assert result.rows_per_epoch == [_FILES * _ROWS]


def test_tpu_single_rank_decoded_isolates_in_subprocess(prefix, monkeypatch):
    """split_by_node=False decoded on TPU must not bind in parent."""
    monkeypatch.setattr(accelerator, "device_kind", lambda: "tpu")
    parent_binds = []
    monkeypatch.setattr(
        driver.device_lib, "rank_device", lambda r: parent_binds.append(r)
    )
    isolated = []

    def fake_isolated(kind, pfx, params, *, extra=None):
        isolated.append((kind, pfx, params.world_size, params.split_by_node, extra))
        return ([0.1], [_FILES * _ROWS], 0.05, 0.01)

    monkeypatch.setattr(driver, "run_single_rank_isolated", fake_isolated)
    params = _params(split_by_node=False, world_size=4, decode=True, rounds=1)
    res = driver.WebDatasetReadDriver().run_read(
        prefix, params, {"sample_count": _FILES * _ROWS}
    )

    assert parent_binds == []
    assert isolated == [
        ("wds", prefix, 1, False, {"cache_root": None, "sample_count": _FILES * _ROWS})
    ]
    assert res.rows_per_epoch == [_FILES * _ROWS]


def test_tpu_single_rank_undecoded_stays_host_only(prefix, monkeypatch):
    monkeypatch.setattr(accelerator, "device_kind", lambda: "tpu")
    distributed, isolated, binds = [], [], []
    monkeypatch.setattr(
        driver, "run_rank_epochs_distributed", lambda *a, **kw: distributed.append(a)
    )
    monkeypatch.setattr(
        driver, "run_single_rank_isolated", lambda *a, **kw: isolated.append(a)
    )
    monkeypatch.setattr(driver.device_lib, "rank_device", lambda r: binds.append(r))

    res = driver.WebDatasetReadDriver().run_read(
        prefix, _params(decode=False, rounds=1), {"sample_count": _FILES * _ROWS}
    )

    assert distributed == []
    assert isolated == []
    assert binds == []
    assert res.rows_per_epoch == [_FILES * _ROWS]


def test_wds_driver_consults_device_pin_memory(
    prefix, monkeypatch, sync_calls, ranks_on_fake_gpu, loader_kwargs
):
    pin_calls = []
    monkeypatch.setattr(
        driver.device_lib,
        "pin_memory",
        lambda dev: pin_calls.append(dev) or False,
    )

    driver.run_rank_epochs(
        0, 1, prefix, _params(decode=True, rounds=1), sample_count=_FILES * _ROWS
    )

    assert pin_calls == [FAKE_GPU]
    assert loader_kwargs[0]["pin_memory"] is False


def test_wds_worker_registered_and_handles_deserialized_namespace(
    prefix, monkeypatch, sync_calls, ranks_on_fake_gpu, loader_kwargs
):
    import dataclasses
    from types import SimpleNamespace

    from gcsfs.tests.perf.subsystembenchmarks.dataloading import _worker

    worker_fn = _worker._resolve_worker("wds")
    # Simulate _worker.main deserializing dataclass params into SimpleNamespace.
    ns_params = SimpleNamespace(
        **dataclasses.asdict(_params(access="shuffled", decode=True, rounds=1))
    )
    per_epoch, _, _ = worker_fn(
        0,
        1,
        prefix,
        ns_params,
        None,
        {"cache_root": None, "sample_count": _FILES * _ROWS},
    )

    assert [rows for _, _, rows in per_epoch] == [_FILES * _ROWS]
