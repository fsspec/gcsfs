import pytest
import torch

from gcsfs.tests.perf.subsystembenchmarks._common import accelerator
from gcsfs.tests.perf.subsystembenchmarks.checkpointing._dist import MAX_RANKS
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.device import (
    DEVICE_ENV,
    backend_for,
    device_capacity,
    rank_device,
    run_device,
)


def test_run_device_default_cpu(monkeypatch):
    monkeypatch.delenv(DEVICE_ENV, raising=False)
    assert run_device() == "cpu"


@pytest.mark.parametrize("device", ["cpu", "cuda", "tpu"])
def test_run_device_accepts_known_devices(monkeypatch, device):
    monkeypatch.setenv(DEVICE_ENV, device)
    assert run_device() == device


@pytest.mark.parametrize("kind", ["cpu", "cuda", "tpu"])
def test_run_device_auto_resolves_via_accelerator(monkeypatch, kind):
    monkeypatch.setenv(DEVICE_ENV, "auto")
    monkeypatch.setattr(accelerator, "device_kind", lambda: kind)
    assert run_device() == kind


def test_run_device_rejects_unknown(monkeypatch):
    monkeypatch.setenv(DEVICE_ENV, "xpu")
    with pytest.raises(ValueError, match="xpu"):
        run_device()


def test_backend_for():
    assert backend_for("cpu") == "gloo"
    assert backend_for("cuda") == "cpu:gloo,cuda:nccl"
    assert backend_for("tpu") == "tpu_dist"
    with pytest.raises(ValueError, match="xpu"):
        backend_for("xpu")


def test_device_capacity_cpu_is_max_ranks():
    assert device_capacity("cpu") == MAX_RANKS


def test_device_capacity_cuda_uses_device_count(monkeypatch):
    monkeypatch.setattr(torch.cuda, "device_count", lambda: 8)
    assert device_capacity("cuda") == 8
    monkeypatch.setattr(accelerator, "local_device_count", lambda: 4)
    assert device_capacity("tpu") == 4
    with pytest.raises(ValueError, match="xpu"):
        device_capacity("xpu")


def test_device_capacity_cuda_requires_a_gpu(monkeypatch):
    monkeypatch.setattr(torch.cuda, "device_count", lambda: 0)
    with pytest.raises(RuntimeError, match=DEVICE_ENV):
        device_capacity("cuda")


def test_rank_device(monkeypatch):
    seen = []
    monkeypatch.setattr(torch.cuda, "set_device", seen.append)

    assert rank_device("cpu", 3) == torch.device("cpu")
    assert seen == []

    assert rank_device("cuda", 3) == torch.device("cuda", 3)
    assert seen == [3]

    with pytest.raises(ValueError, match="xpu"):
        rank_device("xpu", 0)
