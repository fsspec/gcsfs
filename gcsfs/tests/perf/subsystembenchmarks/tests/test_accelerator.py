import inspect
import json
import os
import subprocess
import sys
import types

import pytest

from gcsfs.tests.perf.subsystembenchmarks._common import accelerator


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    accelerator.device_kind.cache_clear()
    for key in (
        accelerator.DEVICE_ENV,
        getattr(
            accelerator, "ACCELERATOR_MODEL_ENV", "GCSFS_SUBSYSTEM_ACCELERATOR_MODEL"
        ),
        "TPU_ACCELERATOR_TYPE",
        "TORCH_DEVICE_BACKEND_AUTOLOAD",
        "RANK",
        "LOCAL_RANK",
        "WORLD_SIZE",
        "TORCH_TPU_DEVICES_PER_HOST",
    ):
        monkeypatch.delenv(key, raising=False)
    yield
    accelerator.device_kind.cache_clear()


def test_cpu_device_skips_subprocess_probe(monkeypatch):
    monkeypatch.setenv(accelerator.DEVICE_ENV, "cpu")
    monkeypatch.setattr(
        subprocess,
        "run",
        lambda *a, **kw: pytest.fail("CPU mode must not spawn probe subprocess"),
    )

    assert accelerator.device_kind() == "cpu"
    assert accelerator.accelerator_type() == "cpu"
    assert accelerator.local_device_count() == 16
    assert accelerator.model_name() == "none"
    assert accelerator.process_group_backend() == "gloo"
    assert accelerator.backend_name() == "gloo"


def test_auto_trusts_torch_accelerator_strictly(monkeypatch):
    cases = [
        ("cpu", 0, "none", "cpu", 16, "none", "gloo", "gloo"),
        ("cuda", 8, "NVIDIA H200", "gpu", 8, "NVIDIA H200", "nccl", "nccl"),
        ("tpu", 8, "tpu7x", "tpu", 8, "tpu7x", "gloo", "tpu_dist"),
    ]
    for kind, count, model, accel_type, exp_count, exp_model, b1, b8 in cases:
        accelerator.device_kind.cache_clear()
        monkeypatch.setattr(
            accelerator,
            "_probe_accelerator",
            lambda k=kind, c=count, m=model: (k, c, m),
        )
        assert accelerator.device_kind() == kind
        assert accelerator.accelerator_type() == accel_type
        assert accelerator.local_device_count() == exp_count
        assert accelerator.model_name() == exp_model
        assert accelerator.backend_name(world_size=1) == b1
        assert accelerator.backend_name(world_size=8) == b8


@pytest.mark.parametrize(
    "requested, probe_result",
    [
        ("cuda", ("cpu", 0, "none")),
        ("cuda", ("cuda", 0, "none")),
        ("cuda", ("tpu", 8, "tpu7x")),
        ("tpu", ("cpu", 0, "none")),
        ("tpu", ("tpu", 0, "tpu")),
        ("tpu", ("cuda", 8, "NVIDIA H200")),
    ],
)
def test_explicit_cuda_or_tpu_fails_fast_when_unavailable_or_zero(
    monkeypatch, requested, probe_result
):
    monkeypatch.setenv(accelerator.DEVICE_ENV, requested)
    monkeypatch.setattr(accelerator, "_probe_accelerator", lambda: probe_result)

    with pytest.raises(accelerator.AcceleratorSetupError):
        accelerator.device_kind()


def test_probe_subprocess_strips_torch_device_backend_autoload(monkeypatch):
    monkeypatch.setenv("TORCH_DEVICE_BACKEND_AUTOLOAD", "0")
    captured = {}

    def fake_run(cmd, **kwargs):
        captured["cmd"] = cmd
        captured["env"] = kwargs.get("env")
        return subprocess.CompletedProcess(
            cmd, 0, stdout=json.dumps(["tpu", 8, "tpu7x"]) + "\n", stderr=""
        )

    monkeypatch.setattr(subprocess, "run", fake_run)

    result = accelerator._probe_accelerator()
    assert result == ("tpu", 8, "tpu7x")
    assert "TORCH_DEVICE_BACKEND_AUTOLOAD" not in captured["env"]
    assert os.environ["TORCH_DEVICE_BACKEND_AUTOLOAD"] == "0"


def test_tpu_model_name_reads_env_vars_in_order(monkeypatch):
    monkeypatch.setattr(accelerator, "_probe_accelerator", lambda: ("tpu", 8, "tpu"))

    assert accelerator.model_name() == "tpu"

    monkeypatch.setenv("TPU_ACCELERATOR_TYPE", "v6e-8")
    assert accelerator.model_name() == "v6e-8"

    monkeypatch.setenv(accelerator.ACCELERATOR_MODEL_ENV, "tpu7x")
    assert accelerator.model_name() == "tpu7x"


def test_ray_resources_floors_gpu_fraction_with_integer_scale(monkeypatch):
    monkeypatch.setattr(accelerator, "_probe_accelerator", lambda: ("cuda", 2, "H200"))
    res = accelerator.ray_resources(3)
    assert res == {"num_gpus": 0.6666}
    assert 3 * res["num_gpus"] <= 2

    accelerator.device_kind.cache_clear()
    monkeypatch.setattr(accelerator, "_probe_accelerator", lambda: ("cuda", 4, "H200"))
    assert accelerator.ray_resources(2) == {"num_gpus": 1.0}

    accelerator.device_kind.cache_clear()
    monkeypatch.setattr(accelerator, "_probe_accelerator", lambda: ("tpu", 4, "v6e"))
    assert accelerator.ray_resources(4) == {"resources": {"TPU": 1.0}}
    with pytest.raises(ValueError, match="exceeds"):
        accelerator.ray_resources(5)


def test_bind_rank_tpu_preserves_torchrun_env_and_fills_ray_actor_env(monkeypatch):
    monkeypatch.setattr(accelerator, "_probe_accelerator", lambda: ("tpu", 4, "v6e"))

    fake_torch = types.SimpleNamespace(
        device=lambda kind, idx=None: types.SimpleNamespace(type=kind, index=idx),
        zeros=lambda *a, **kw: None,
    )
    monkeypatch.setitem(sys.modules, "torch", fake_torch)

    # Case 1: torchrun already set RANK/LOCAL_RANK/WORLD_SIZE -> preserve them.
    monkeypatch.setenv("RANK", "7")
    monkeypatch.setenv("LOCAL_RANK", "3")
    monkeypatch.setenv("WORLD_SIZE", "8")
    dev = accelerator.bind_rank(1, global_rank=1, world_size=4)
    assert dev.type == "tpu"
    assert os.environ["RANK"] == "7"
    assert os.environ["LOCAL_RANK"] == "3"
    assert os.environ["WORLD_SIZE"] == "8"

    # Case 2: Ray actor where env vars are unset -> fill them.
    for key in ("RANK", "LOCAL_RANK", "WORLD_SIZE", "TORCH_TPU_DEVICES_PER_HOST"):
        monkeypatch.delenv(key, raising=False)
    dev = accelerator.bind_rank(2, global_rank=2, world_size=4)
    assert dev.type == "tpu"
    assert os.environ["RANK"] == "2"
    assert os.environ["LOCAL_RANK"] == "2"
    assert os.environ["WORLD_SIZE"] == "4"
    assert os.environ["TORCH_TPU_DEVICES_PER_HOST"] == "4"


def test_loader_worker_env_toggles_autoload_on_tpu_only(monkeypatch):
    # On TPU with var unset -> sets "0", then unsets on exit.
    monkeypatch.setattr(accelerator, "_probe_accelerator", lambda: ("tpu", 4, "v6e"))
    with accelerator.loader_worker_env():
        assert os.environ["TORCH_DEVICE_BACKEND_AUTOLOAD"] == "0"
    assert "TORCH_DEVICE_BACKEND_AUTOLOAD" not in os.environ

    # On TPU with previous value -> sets "0", then restores previous value.
    monkeypatch.setenv("TORCH_DEVICE_BACKEND_AUTOLOAD", "1")
    with accelerator.loader_worker_env():
        assert os.environ["TORCH_DEVICE_BACKEND_AUTOLOAD"] == "0"
    assert os.environ["TORCH_DEVICE_BACKEND_AUTOLOAD"] == "1"

    # On CPU and CUDA -> no-op.
    for probe_res in (("cpu", 0, "none"), ("cuda", 4, "H200")):
        accelerator.device_kind.cache_clear()
        monkeypatch.setattr(accelerator, "_probe_accelerator", lambda r=probe_res: r)
        monkeypatch.delenv("TORCH_DEVICE_BACKEND_AUTOLOAD", raising=False)
        with accelerator.loader_worker_env():
            assert "TORCH_DEVICE_BACKEND_AUTOLOAD" not in os.environ


def test_no_pci_or_dev_shm_hacks_in_accelerator():
    src = inspect.getsource(accelerator)
    for forbidden in (
        "0x1ae0",
        "torch_tpu_pg_store_",
        "/dev/vfio",
        "/dev/nvidiactl",
        "AF_INET6",
    ):
        assert forbidden not in src, f"Forbidden token {forbidden!r} found"
