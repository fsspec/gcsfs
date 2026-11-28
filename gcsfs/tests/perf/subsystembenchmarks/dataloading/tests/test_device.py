"""Shared CPU/GPU/TPU device helpers for data-loading drivers.

The ``meta`` device stands in for an accelerator so copy paths run on CPU-only hosts;
``accelerator`` calls that need a real device are replaced with recorders.
"""

import itertools
import os

import pytest

torch = pytest.importorskip("torch")

from gcsfs.tests.perf.subsystembenchmarks._common import accelerator
from gcsfs.tests.perf.subsystembenchmarks.dataloading import device as device_lib
from gcsfs.tests.perf.subsystembenchmarks.dataloading import driver as dl_driver

FAKE_GPU = torch.device("meta")
FAKE_TPU = torch.device("meta")


@pytest.fixture(autouse=True)
def _clean_accelerator(monkeypatch):
    accelerator.reset_cache()
    monkeypatch.delenv(accelerator.DEVICE_ENV, raising=False)
    monkeypatch.delenv("TORCH_DEVICE_BACKEND_AUTOLOAD", raising=False)
    yield
    accelerator.reset_cache()


@pytest.fixture
def sync_calls(monkeypatch):
    calls = []
    monkeypatch.setattr(accelerator, "synchronize", calls.append)
    return calls


@pytest.fixture
def on_cpu(monkeypatch):
    monkeypatch.setattr(accelerator, "device_kind", lambda: "cpu")


@pytest.fixture
def on_gpu(monkeypatch):
    def configure(device_count):
        monkeypatch.setattr(accelerator, "device_kind", lambda: "cuda")
        monkeypatch.setattr(accelerator, "local_device_count", lambda: device_count)

    return configure


@pytest.fixture
def on_tpu(monkeypatch):
    def configure(device_count):
        monkeypatch.setattr(accelerator, "device_kind", lambda: "tpu")
        monkeypatch.setattr(accelerator, "local_device_count", lambda: device_count)

    return configure


def test_rank_device_is_none_on_cpu(on_cpu):
    assert device_lib.rank_device(3) is None


@pytest.mark.parametrize("kind", ["cuda", "tpu"])
def test_rank_device_delegates_to_accelerator_bind_rank(monkeypatch, kind):
    bound = []
    monkeypatch.setattr(accelerator, "device_kind", lambda: kind)
    monkeypatch.setattr(
        accelerator,
        "bind_rank",
        lambda rank, **kw: bound.append((rank, kw)) or FAKE_GPU,
    )

    assert device_lib.rank_device(2) == FAKE_GPU
    assert device_lib.rank_device(3, world_size=4) == FAKE_GPU
    assert bound == [
        (2, {"global_rank": 2, "world_size": None}),
        (3, {"global_rank": 3, "world_size": 4}),
    ]


def test_pin_memory_matches_device_presence():
    assert device_lib.pin_memory(None) is False
    assert device_lib.pin_memory(FAKE_GPU) is True


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


def test_to_device_keeps_tensors_already_on_the_device():
    tensor = torch.ones(2, device=FAKE_GPU)
    assert device_lib.to_device({"label": tensor}, FAKE_GPU)["label"] is tensor


def test_feed_returns_loader_unchanged_on_cpu(on_cpu):
    loader = [{"label": torch.tensor([1])}]
    assert device_lib.feed(loader, None) is loader


def test_feed_moves_batches_and_synchronizes_once_per_pass(on_gpu, sync_calls):
    on_gpu(8)
    feed = device_lib.feed([{"label": torch.tensor([1])}] * 3, FAKE_GPU)

    first = list(feed)
    list(feed)

    assert [b["label"].device for b in first] == [FAKE_GPU] * 3
    assert sync_calls == [FAKE_GPU, FAKE_GPU]


def test_feed_wraps_with_loader_worker_env_and_syncs_on_tpu(on_tpu, sync_calls):
    on_tpu(8)
    env_inside = []

    class _Loader:
        def __iter__(self):
            env_inside.append(os.environ.get("TORCH_DEVICE_BACKEND_AUTOLOAD"))
            yield {"label": torch.tensor([1])}
            yield {"label": torch.tensor([2])}

    feed = device_lib.feed(_Loader(), FAKE_TPU)
    out = list(feed)
    assert [b["label"].device for b in out] == [FAKE_TPU, FAKE_TPU]
    assert env_inside == ["0"]
    assert "TORCH_DEVICE_BACKEND_AUTOLOAD" not in os.environ
    assert sync_calls == [FAKE_TPU]


def test_feed_sets_worker_env_on_tpu_even_without_device(on_tpu, sync_calls):
    on_tpu(8)
    env_inside = []

    class _Loader:
        def __iter__(self):
            env_inside.append(os.environ.get("TORCH_DEVICE_BACKEND_AUTOLOAD"))
            yield {"raw": b"jpeg-bytes"}

    feed = device_lib.feed(_Loader(), None)
    assert list(feed) == [{"raw": b"jpeg-bytes"}]
    assert env_inside == ["0"]
    assert "TORCH_DEVICE_BACKEND_AUTOLOAD" not in os.environ
    assert sync_calls == []


def test_feed_synchronizes_when_a_pass_yields_no_batches(on_gpu, sync_calls):
    on_gpu(8)
    assert list(device_lib.feed([], FAKE_GPU)) == []
    assert sync_calls == [FAKE_GPU]


def test_feed_synchronizes_when_iterator_is_closed_early(on_gpu, sync_calls):
    on_gpu(8)
    iterator = iter(device_lib.feed([{"label": torch.tensor([1])}] * 3, FAKE_GPU))

    next(iterator)
    assert sync_calls == []
    iterator.close()

    assert sync_calls == [FAKE_GPU]


def test_rounds_stopped_at_a_target_synchronize_before_their_end_timestamp(
    on_gpu, monkeypatch
):
    """measure_epochs breaks at the target; the feed must sync before the end
    timestamp (CPython closes the abandoned generator at the break)."""
    on_gpu(8)
    clock = itertools.count()
    synced_at = []
    monkeypatch.setattr(dl_driver, "timestamp", lambda: next(clock))
    monkeypatch.setattr(
        accelerator, "synchronize", lambda device: synced_at.append(next(clock))
    )
    feed = device_lib.feed([{"label": torch.ones(2)}] * 10, FAKE_GPU)

    per_epoch, _ = dl_driver.measure_epochs(feed, 2, lambda batch: 2, target=4)

    assert [rows for _, _, rows in per_epoch] == [4, 4]
    assert len(synced_at) == 2
    for (begin, end, _), sync in zip(per_epoch, synced_at):
        assert begin < sync < end


@pytest.mark.skipif(not torch.cuda.is_available(), reason="requires a CUDA GPU")
def test_feed_delivers_tensors_to_a_real_gpu():
    device = device_lib.rank_device(0)
    batches = list(device_lib.feed([{"label": torch.arange(4)}], device))
    assert batches[0]["label"].device == device
