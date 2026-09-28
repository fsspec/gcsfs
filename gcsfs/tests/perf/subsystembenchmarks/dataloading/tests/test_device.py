"""Shared CPU/GPU device helpers for data-loading drivers.

The ``meta`` device stands in for a GPU so copy paths run on CPU-only hosts;
``torch.cuda`` calls that need a real device are replaced with recorders.
"""

import pytest

torch = pytest.importorskip("torch")

from gcsfs.tests.perf.subsystembenchmarks.dataloading import (  # noqa: E402
    device as device_lib,
)

FAKE_GPU = torch.device("meta")


@pytest.fixture
def sync_calls(monkeypatch):
    calls = []
    monkeypatch.setattr(torch.cuda, "synchronize", calls.append)
    return calls


@pytest.fixture
def on_gpu(monkeypatch):
    def configure(device_count):
        monkeypatch.setattr(device_lib.env, "detect_accelerator", lambda: "gpu")
        monkeypatch.setattr(torch.cuda, "device_count", lambda: device_count)

    return configure


@pytest.fixture
def warmups(monkeypatch):
    """Record the context warm-up allocation instead of touching a real GPU."""
    devices = []
    monkeypatch.setattr(torch.cuda, "set_device", lambda index: None)
    monkeypatch.setattr(
        torch, "zeros", lambda *shape, device=None: devices.append(device)
    )
    return devices


def test_rank_device_is_none_on_cpu(monkeypatch):
    monkeypatch.setattr(device_lib.env, "detect_accelerator", lambda: "cpu")
    assert device_lib.rank_device(3) is None


def test_rank_device_binds_rank_modulo_device_count(on_gpu, warmups, monkeypatch):
    bound = []
    on_gpu(4)
    monkeypatch.setattr(torch.cuda, "set_device", bound.append)

    assert device_lib.rank_device(6) == torch.device("cuda", 2)
    assert bound == [2]


def test_rank_device_creates_the_cuda_context_before_timed_rounds(on_gpu, warmups):
    on_gpu(4)

    device = device_lib.rank_device(6)

    assert warmups == [device]


def test_to_device_moves_nested_tensors_and_passes_other_values_through():
    batch = [
        {"images": [torch.ones(3, 2, 2), torch.ones(3, 4, 4)], "text": "cap"},
        {"images": [b"raw-jpeg"], "text": None, "ids": (torch.ones(2), 7)},
    ]

    out = device_lib.to_device(batch, FAKE_GPU)

    assert [t.device for t in out[0]["images"]] == [FAKE_GPU, FAKE_GPU]
    assert out[0]["images"][1].shape == (3, 4, 4)
    assert out[0]["text"] == "cap"
    assert out[1]["images"] == [b"raw-jpeg"]
    assert out[1]["text"] is None
    assert isinstance(out[1]["ids"], tuple)
    assert out[1]["ids"][0].device == FAKE_GPU
    assert out[1]["ids"][1] == 7


def test_synchronize_is_a_noop_on_cpu(sync_calls):
    device_lib.synchronize(None)
    assert sync_calls == []


def test_feed_returns_loader_unchanged_on_cpu():
    loader = [{"label": torch.tensor([1])}]
    assert device_lib.feed(loader, None) is loader


def test_feed_moves_batches_and_synchronizes_once_per_pass(sync_calls):
    feed = device_lib.feed([{"label": torch.tensor([1])}] * 3, FAKE_GPU)

    first = list(feed)
    list(feed)

    assert [b["label"].device for b in first] == [FAKE_GPU] * 3
    assert sync_calls == [FAKE_GPU, FAKE_GPU]


def test_feed_synchronizes_when_a_pass_yields_no_batches(sync_calls):
    assert list(device_lib.feed([], FAKE_GPU)) == []
    assert sync_calls == [FAKE_GPU]


def test_feed_synchronizes_when_iterator_is_closed_early(sync_calls):
    iterator = iter(device_lib.feed([{"label": torch.tensor([1])}] * 3, FAKE_GPU))

    next(iterator)
    assert sync_calls == []
    iterator.close()

    assert sync_calls == [FAKE_GPU]


def test_ray_task_num_gpus_is_zero_on_cpu(monkeypatch):
    monkeypatch.setattr(device_lib.env, "detect_accelerator", lambda: "cpu")
    assert device_lib.ray_task_num_gpus(8) == 0


@pytest.mark.parametrize(
    "gpus, world_size, expected",
    [(8, 8, 1.0), (8, 4, 1.0), (4, 8, 0.5), (3, 8, 0.375), (2, 3, 0.6666)],
)
def test_ray_task_num_gpus_shares_gpus_in_ray_precision(
    on_gpu, gpus, world_size, expected
):
    on_gpu(gpus)

    share = device_lib.ray_task_num_gpus(world_size)

    assert share == expected
    assert share * world_size <= max(gpus, world_size)


@pytest.mark.skipif(not torch.cuda.is_available(), reason="requires a CUDA GPU")
def test_feed_delivers_tensors_to_a_real_gpu():
    device = device_lib.rank_device(0)
    batches = list(device_lib.feed([{"label": torch.arange(4)}], device))
    assert batches[0]["label"].device == device
