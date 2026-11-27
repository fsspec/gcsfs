import pytest
import torch

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


@pytest.mark.parametrize("device", ["cpu", "cuda"])
def test_run_device_accepts_known_devices(monkeypatch, device):
    monkeypatch.setenv(DEVICE_ENV, device)
    assert run_device() == device


def test_run_device_rejects_unknown(monkeypatch):
    monkeypatch.setenv(DEVICE_ENV, "tpu")
    with pytest.raises(ValueError, match="tpu"):
        run_device()


def test_backend_for():
    assert backend_for("cpu") == "gloo"
    assert backend_for("cuda") == "cpu:gloo,cuda:nccl"
    with pytest.raises(ValueError, match="tpu"):
        backend_for("tpu")


def test_device_capacity_cpu_is_max_ranks():
    assert device_capacity("cpu") == MAX_RANKS


def test_device_capacity_cuda_uses_device_count(monkeypatch):
    monkeypatch.setattr(torch.cuda, "device_count", lambda: 8)
    assert device_capacity("cuda") == 8
    with pytest.raises(ValueError, match="tpu"):
        device_capacity("tpu")


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

    with pytest.raises(ValueError, match="tpu"):
        rank_device("tpu", 0)
