import os
import sys

import pytest
import torch

from gcsfs.tests.perf.subsystembenchmarks._common import accelerator


@pytest.fixture(autouse=True)
def _reset_accelerator(monkeypatch):
    accelerator.device_kind.cache_clear()
    for key in (
        accelerator.DEVICE_ENV,
        "TORCH_TPU_TOPOLOGY",
        "TORCH_TPU_SLICEBUILDER_ADDRESSES",
        "TORCH_TPU_XPROF_SESSION_ID",
        "RAY_EXPERIMENTAL_NOSET_TPU_VISIBLE_CHIPS",
        "TORCH_DEVICE_BACKEND_AUTOLOAD",
        "MASTER_ADDR",
        "MASTER_PORT",
        "RANK",
        "LOCAL_RANK",
        "WORLD_SIZE",
        "TORCH_TPU_DEVICES_PER_HOST",
        "GCSFS_SUBSYSTEM_TPU_SLICE_TOPOLOGY",
        "GCSFS_SUBSYSTEM_TPU_SLICE_ADDRESSES",
        "CUDA_VISIBLE_DEVICES",
        "_GCSFS_SUBSYSTEM_MANAGED_TPU_ENV",
    ):
        monkeypatch.delenv(key, raising=False)
    yield
    accelerator.device_kind.cache_clear()


@pytest.fixture
def on_cpu(monkeypatch):
    monkeypatch.setattr(accelerator, "_cuda_available", lambda: False)
    monkeypatch.setattr(accelerator, "_cuda_hardware_present", lambda: False)
    monkeypatch.setattr(accelerator, "_tpu_hardware_present", lambda: False)
    monkeypatch.setattr(accelerator, "_torch_tpu_installed", lambda: False)
    monkeypatch.setattr(accelerator, "_tpu_device_count", lambda: 0)
    monkeypatch.setattr(accelerator, "_tpu_type", lambda: "none")


@pytest.fixture
def on_cuda(monkeypatch):
    monkeypatch.setattr(accelerator, "_cuda_available", lambda: True)
    monkeypatch.setattr(accelerator, "_cuda_hardware_present", lambda: True)
    monkeypatch.setattr(accelerator, "_tpu_hardware_present", lambda: False)
    monkeypatch.setattr(torch.cuda, "device_count", lambda: 8)
    monkeypatch.setattr(torch.cuda, "get_device_name", lambda idx: "NVIDIA H200")


@pytest.fixture
def on_cuda_and_tpu(monkeypatch):
    monkeypatch.setattr(accelerator, "_cuda_available", lambda: True)
    monkeypatch.setattr(accelerator, "_cuda_hardware_present", lambda: True)
    monkeypatch.setattr(accelerator, "_tpu_hardware_present", lambda: True)
    monkeypatch.setattr(accelerator, "_torch_tpu_installed", lambda: True)
    monkeypatch.setattr(torch.cuda, "device_count", lambda: 8)


@pytest.fixture
def on_tpu(monkeypatch):
    monkeypatch.delitem(sys.modules, "torch_tpu", raising=False)
    monkeypatch.setattr(accelerator, "_cuda_available", lambda: False)
    monkeypatch.setattr(accelerator, "_cuda_hardware_present", lambda: False)
    monkeypatch.setattr(accelerator, "_tpu_hardware_present", lambda: True)
    monkeypatch.setattr(accelerator, "_torch_tpu_installed", lambda: True)
    monkeypatch.setattr(accelerator, "_tpu_device_count", lambda: 8)
    monkeypatch.setattr(accelerator, "_tpu_type", lambda: "tpu7x")


def test_auto_prefers_cuda(on_cuda_and_tpu):
    assert accelerator.device_kind() == "cuda"


def test_auto_tpu(on_tpu):
    assert (
        accelerator.device_kind(),
        accelerator.accelerator_type(),
        accelerator.backend_name(),
        accelerator.process_group_backend(),
    ) == ("tpu", "tpu", "tpu_dist", "tpu_dist")


def test_auto_cpu(on_cpu):
    assert accelerator.device_kind() == "cpu"
    assert accelerator.accelerator_type() == "cpu"
    assert accelerator.process_group_backend() == "gloo"
    assert accelerator.backend_name() == "gloo"
    assert accelerator.local_device_count() == accelerator.CPU_MAX_RANKS == 16
    assert accelerator.model_name() == "none"


def test_cuda_backend(on_cuda):
    assert accelerator.device_kind() == "cuda"
    assert accelerator.accelerator_type() == "gpu"
    assert accelerator.process_group_backend() == "cpu:gloo,cuda:nccl"
    assert accelerator.backend_name() == "nccl"
    assert accelerator.local_device_count() == 8
    assert accelerator.model_name() == "NVIDIA H200"


def test_explicit_cpu_on_gpu_host(on_cuda, monkeypatch):
    monkeypatch.setenv(accelerator.DEVICE_ENV, "cpu")
    assert accelerator.device_kind() == "cpu"
    assert accelerator.accelerator_type() == "cpu"
    assert accelerator.process_group_backend() == "gloo"


def test_explicit_cuda_without_cuda_raises(on_cpu, monkeypatch):
    monkeypatch.setenv(accelerator.DEVICE_ENV, "cuda")
    with pytest.raises(accelerator.AcceleratorSetupError, match="cuda"):
        accelerator.device_kind()


def test_explicit_tpu_without_hardware_raises(on_cpu, monkeypatch):
    monkeypatch.setenv(accelerator.DEVICE_ENV, "tpu")
    with pytest.raises(accelerator.AcceleratorSetupError, match="tpu"):
        accelerator.device_kind()


def test_tpu_hardware_without_torch_tpu_raises(monkeypatch):
    monkeypatch.setattr(accelerator, "_cuda_available", lambda: False)
    monkeypatch.setattr(accelerator, "_tpu_hardware_present", lambda: True)
    monkeypatch.setattr(accelerator, "_torch_tpu_installed", lambda: False)
    with pytest.raises(
        accelerator.AcceleratorSetupError, match="pip install torch_tpu"
    ):
        accelerator.device_kind()


def test_unknown_device_raises(on_cpu, monkeypatch):
    monkeypatch.setenv(accelerator.DEVICE_ENV, "xpu")
    with pytest.raises(ValueError, match="xpu"):
        accelerator.device_kind()


def test_tpu_detection_does_not_import_torch_tpu(on_tpu):
    assert accelerator.device_kind() == "tpu"
    assert accelerator.local_device_count() == 8
    assert accelerator.model_name() == "tpu7x"
    assert "torch_tpu" not in sys.modules


def test_local_device_count_cuda_zero_raises(on_cuda, monkeypatch):
    monkeypatch.setattr(torch.cuda, "device_count", lambda: 0)
    with pytest.raises(accelerator.AcceleratorSetupError, match=accelerator.DEVICE_ENV):
        accelerator.local_device_count()


def test_bind_rank_cpu(on_cpu, monkeypatch):
    seen = []
    monkeypatch.setattr(torch.cuda, "set_device", seen.append)
    assert accelerator.bind_rank(3) == torch.device("cpu")
    assert seen == []


def test_bind_rank_cuda_modulo(on_cuda, monkeypatch):
    seen = []
    warmed = []
    monkeypatch.setattr(torch.cuda, "device_count", lambda: 4)
    monkeypatch.setattr(torch.cuda, "set_device", seen.append)
    monkeypatch.setattr(
        torch, "zeros", lambda *a, **kw: warmed.append(kw.get("device"))
    )

    dev = accelerator.bind_rank(6)
    assert dev == torch.device("cuda", 2)
    assert seen == [2]
    assert warmed == [torch.device("cuda", 2)]


def test_bind_rank_tpu_sets_rank_and_global_rank(on_tpu, monkeypatch):
    warmed = []
    monkeypatch.setenv("TORCH_TPU_SLICEBUILDER_ADDRESSES", "a:1,a:2,a:3,a:4")
    monkeypatch.setattr(torch, "device", lambda kind, *a: f"fake-{kind}")
    monkeypatch.setattr(
        torch, "zeros", lambda *a, **kw: warmed.append(kw.get("device"))
    )

    dev = accelerator.bind_rank(1, global_rank=9, world_size=16)
    assert dev == "fake-tpu"
    assert os.environ["RANK"] == "9"
    assert os.environ["LOCAL_RANK"] == "1"
    assert os.environ["WORLD_SIZE"] == "16"
    assert os.environ["TORCH_TPU_DEVICES_PER_HOST"] == "8"
    assert warmed == ["fake-tpu"]


def test_ray_resources(monkeypatch):
    monkeypatch.setattr(accelerator, "_cuda_available", lambda: False)
    monkeypatch.setattr(accelerator, "_tpu_hardware_present", lambda: False)
    assert accelerator.ray_resources(8) == {}

    accelerator.device_kind.cache_clear()
    monkeypatch.setattr(accelerator, "_cuda_available", lambda: True)
    monkeypatch.setattr(torch.cuda, "device_count", lambda: 8)
    assert accelerator.ray_resources(16) == {"num_gpus": 0.5}
    assert accelerator.ray_resources(8) == {"num_gpus": 1.0}

    accelerator.device_kind.cache_clear()
    monkeypatch.setattr(accelerator, "_cuda_available", lambda: False)
    monkeypatch.setattr(accelerator, "_tpu_hardware_present", lambda: True)
    monkeypatch.setattr(accelerator, "_torch_tpu_installed", lambda: True)
    monkeypatch.setattr(accelerator, "_tpu_device_count", lambda: 8)
    assert accelerator.ray_resources(8) == {"resources": {"TPU": 1.0}}

    with pytest.raises(ValueError):
        accelerator.ray_resources(0)


@pytest.mark.parametrize(
    "gpus, world_size, expected",
    [(8, 8, 1.0), (8, 4, 1.0), (4, 8, 0.5), (3, 8, 0.375), (2, 3, 0.6666)],
)
def test_ray_resources_gpu_share_floors_to_ray_precision(
    on_cuda, monkeypatch, gpus, world_size, expected
):
    monkeypatch.setattr(torch.cuda, "device_count", lambda: gpus)

    share = accelerator.ray_resources(world_size)["num_gpus"]

    assert share == expected
    assert share * world_size <= max(gpus, world_size)


def test_ray_resources_tpu_requests_whole_chips(on_tpu):
    assert accelerator.ray_resources(8) == {"resources": {"TPU": 1.0}}
    assert accelerator.ray_resources(4) == {"resources": {"TPU": 1.0}}
    with pytest.raises(ValueError, match="exceeds 8 tpu devices"):
        accelerator.ray_resources(16)


def test_loader_worker_env_restores(on_tpu, monkeypatch):
    monkeypatch.setenv("TORCH_DEVICE_BACKEND_AUTOLOAD", "1")
    with accelerator.loader_worker_env():
        assert os.environ["TORCH_DEVICE_BACKEND_AUTOLOAD"] == "0"
    assert os.environ["TORCH_DEVICE_BACKEND_AUTOLOAD"] == "1"

    monkeypatch.delenv("TORCH_DEVICE_BACKEND_AUTOLOAD")
    with accelerator.loader_worker_env():
        assert os.environ["TORCH_DEVICE_BACKEND_AUTOLOAD"] == "0"
    assert "TORCH_DEVICE_BACKEND_AUTOLOAD" not in os.environ


def test_prepare_launch_tpu_subset(on_tpu, monkeypatch):
    monkeypatch.setattr(accelerator, "_cleanup_stale_tpu_pg_stores", lambda: None)
    accelerator.prepare_launch(2)
    assert os.environ["TORCH_TPU_TOPOLOGY"] == "1,1,1,2"
    addrs = os.environ["TORCH_TPU_SLICEBUILDER_ADDRESSES"].split(",")
    assert len(addrs) == 2
    assert os.environ["MASTER_ADDR"] == "127.0.0.1"
    assert int(os.environ["MASTER_PORT"]) > 0

    # World size 1 clears managed vars on 2-core-per-chip tpu7x.
    accelerator.prepare_launch(1)
    assert "TORCH_TPU_TOPOLOGY" not in os.environ
    assert "TORCH_TPU_SLICEBUILDER_ADDRESSES" not in os.environ

    # User-preset TORCH_TPU_TOPOLOGY is never overwritten.
    monkeypatch.setenv("TORCH_TPU_TOPOLOGY", "2,2,2,2")
    accelerator.prepare_launch(2)
    assert os.environ["TORCH_TPU_TOPOLOGY"] == "2,2,2,2"


def test_synchronize_skips_cpu_and_none(monkeypatch):
    calls = []
    monkeypatch.setattr(
        torch.accelerator, "synchronize", lambda dev=None: calls.append(dev)
    )
    accelerator.synchronize(None)
    accelerator.synchronize(torch.device("cpu"))
    assert calls == []
    accelerator.synchronize(torch.device("cuda", 2))
    assert calls == [torch.device("cuda", 2)]


def test_prepare_launch_tpu_multihost_uses_slice_env(on_tpu, monkeypatch):
    monkeypatch.setattr(accelerator, "_cleanup_stale_tpu_pg_stores", lambda: None)
    slice_addrs = ",".join(f"10.0.0.{i // 8}:{15000 + (i % 8)}" for i in range(16))
    monkeypatch.setenv("GCSFS_SUBSYSTEM_TPU_SLICE_TOPOLOGY", "2,2,2,2")
    monkeypatch.setenv("GCSFS_SUBSYSTEM_TPU_SLICE_ADDRESSES", slice_addrs)

    accelerator.prepare_launch(16)
    assert os.environ["TORCH_TPU_TOPOLOGY"] == "2,2,2,2"
    assert os.environ["TORCH_TPU_SLICEBUILDER_ADDRESSES"] == slice_addrs
    assert "MASTER_ADDR" not in os.environ

    # Single-host world_size=8 uses the single-host recipe, not the 16-rank slice env.
    accelerator.prepare_launch(8)
    assert os.environ["TORCH_TPU_TOPOLOGY"] == "2,2,1,2"
    assert len(os.environ["TORCH_TPU_SLICEBUILDER_ADDRESSES"].split(",")) == 8
    assert os.environ["MASTER_ADDR"] == "127.0.0.1"


def test_prepare_launch_tpu_refreshes_managed_env_across_spawned_child(
    on_tpu, monkeypatch
):
    monkeypatch.setattr(accelerator, "_cleanup_stale_tpu_pg_stores", lambda: None)
    accelerator.prepare_launch(4)
    assert os.environ["TORCH_TPU_TOPOLOGY"] == "1,2,1,2"
    assert len(os.environ["TORCH_TPU_SLICEBUILDER_ADDRESSES"].split(",")) == 4

    # Subsequent calls refresh env vars tracked solely by the marker env var.
    accelerator.prepare_launch(8)
    assert os.environ["TORCH_TPU_TOPOLOGY"] == "2,2,1,2"
    assert len(os.environ["TORCH_TPU_SLICEBUILDER_ADDRESSES"].split(",")) == 8

    # Externally preset user value still wins.
    monkeypatch.setenv("TORCH_TPU_TOPOLOGY", "2,2,2,2")
    accelerator.prepare_launch(4)
    assert os.environ["TORCH_TPU_TOPOLOGY"] == "2,2,2,2"


def test_tpu_single_rank_backend_uses_gloo(on_tpu):
    assert accelerator.process_group_backend(1) == "gloo"
    assert accelerator.backend_name(1) == "gloo"
    assert accelerator.process_group_backend(4) == "tpu_dist"
    assert accelerator.backend_name(4) == "tpu_dist"


def test_capture_and_restore_launch_env(on_tpu, monkeypatch):
    monkeypatch.setattr(accelerator, "_cleanup_stale_tpu_pg_stores", lambda: None)
    monkeypatch.setenv("TORCH_TPU_TOPOLOGY", "preset")
    snap = accelerator.capture_launch_env(("RAY_EXPERIMENTAL_NOSET_TPU_VISIBLE_CHIPS",))
    os.environ["RAY_EXPERIMENTAL_NOSET_TPU_VISIBLE_CHIPS"] = "1"
    accelerator.prepare_launch(2)
    assert "TORCH_TPU_SLICEBUILDER_ADDRESSES" in os.environ
    assert accelerator._MANAGED_ENV_MARKER in os.environ

    accelerator.restore_launch_env(snap)
    assert os.environ["TORCH_TPU_TOPOLOGY"] == "preset"
    assert "TORCH_TPU_SLICEBUILDER_ADDRESSES" not in os.environ
    assert "RAY_EXPERIMENTAL_NOSET_TPU_VISIBLE_CHIPS" not in os.environ
    assert accelerator._MANAGED_ENV_MARKER not in os.environ


def test_auto_gpu_hardware_without_cuda_raises(monkeypatch):
    monkeypatch.setattr(accelerator, "_cuda_available", lambda: False)
    monkeypatch.setattr(accelerator, "_cuda_hardware_present", lambda: True)
    monkeypatch.setattr(accelerator, "_tpu_hardware_present", lambda: False)
    with pytest.raises(accelerator.AcceleratorSetupError, match="CUDA"):
        accelerator.device_kind()


def test_auto_gpu_hardware_masked_by_cuda_visible_devices_falls_back_to_cpu(
    monkeypatch, tmp_path
):
    fake_ctl = tmp_path / "nvidiactl"
    fake_ctl.write_text("")
    monkeypatch.setattr(accelerator, "_NVIDIA_CTL_PATH", fake_ctl)
    monkeypatch.setattr(accelerator, "_cuda_available", lambda: False)
    monkeypatch.setattr(accelerator, "_tpu_hardware_present", lambda: False)
    monkeypatch.setenv("CUDA_VISIBLE_DEVICES", "")
    assert accelerator._cuda_hardware_present() is False
    assert accelerator.device_kind() == "cpu"
