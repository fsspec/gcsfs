"""Tests for GPU/TPU device placement, subprocess worker, and Ray actor options in ray_pytorch."""

from __future__ import annotations

import contextlib
import json
import os
import sys
import types
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

torch = pytest.importorskip("torch")
pytest.importorskip("ray")

from gcsfs.tests.perf.subsystembenchmarks._common import (  # noqa: E402
    accelerator,
    launcher,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.ray_pytorch import (  # noqa: E402
    common,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.ray_pytorch.parameters import (  # noqa: E402
    RayCheckpointParameters,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.ray_pytorch.read import (  # noqa: E402
    driver as read_driver,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.ray_pytorch.write import (  # noqa: E402
    driver as write_driver,
)


def _make_params(
    strategy="fsdp_sharded",
    world_size=4,
    tp=1,
    dp=4,
    rounds=1,
    model_id="gs://bucket/Llama-3.1-8B",
    setup_world_size=None,
):
    return RayCheckpointParameters(
        name="ray-test",
        bucket_name="",
        bucket_type="regional",
        rounds=rounds,
        scenario="checkpoint_read",
        framework="ray_pytorch",
        model_id=model_id,
        strategy=strategy,
        world_size=world_size,
        tensor_parallel_size=tp,
        data_parallel_size=dp,
        setup_world_size=setup_world_size,
    )


def _dummy_model():
    payload = SimpleNamespace(
        model=SimpleNamespace(layers=[torch.nn.Linear(4, 4, bias=False)])
    )
    probe = torch.nn.Linear(4, 4, bias=False)
    return SimpleNamespace(payload=payload, probe=probe)


def test_parallelize_model_uses_device_type(monkeypatch):
    import torch.distributed.device_mesh as device_mesh_mod
    import torch.distributed.fsdp as fsdp_mod

    mesh_calls = []

    class _FakeMesh:
        def __getitem__(self, key):
            return f"submesh:{key}"

    def fake_init_device_mesh(device_type, mesh_shape, *, mesh_dim_names=None):
        mesh_calls.append((device_type, mesh_shape, mesh_dim_names))
        return _FakeMesh()

    monkeypatch.setattr(device_mesh_mod, "init_device_mesh", fake_init_device_mesh)
    monkeypatch.setattr(fsdp_mod, "fully_shard", lambda *a, **k: None)
    monkeypatch.setattr(common, "apply_llama_tp", lambda *a, **k: None)

    fsdp_params = _make_params(strategy="fsdp_sharded", world_size=4, dp=4, tp=1)
    common.parallelize_model(_dummy_model(), fsdp_params, "cuda")
    common.parallelize_model(_dummy_model(), fsdp_params)

    mp_params = _make_params(
        strategy="model_parallel_sharded", world_size=8, dp=2, tp=4
    )
    common.parallelize_model(_dummy_model(), mp_params, "tpu")

    assert mesh_calls == [
        ("cuda", (4,), None),
        ("cpu", (4,), None),
        ("tpu", (2, 4), ("dp", "tp")),
    ]


def test_drivers_run_inside_launcher_subprocess(monkeypatch):
    staged = []
    calls = []
    monkeypatch.setattr(write_driver, "stage_gcs_model", lambda m: staged.append(m))
    monkeypatch.setattr(read_driver, "stage_gcs_model", lambda m: staged.append(m))

    def fake_run_subprocess(module, args, *, env=None):
        calls.append((module, list(args)))
        if "--result-file" in args:
            rf = args[args.index("--result-file") + 1]
            with open(rf, "w", encoding="utf-8") as f:
                json.dump({"durations": [3.0, 4.0]}, f)

    monkeypatch.setattr(launcher, "run_subprocess", fake_run_subprocess)

    params = _make_params(world_size=2, rounds=2)
    write_driver.RayCheckpointWriteDriver().setup("gs://b/ckpt", params)
    write_res = write_driver.RayCheckpointWriteDriver().run("gs://b/ckpt", params)
    assert write_res.durations == [3.0, 4.0]
    assert write_res.window is None
    assert write_res.resource_metrics is None

    read_driver.RayCheckpointReadDriver().setup("gs://b/ckpt", params)
    read_res = read_driver.RayCheckpointReadDriver().run("gs://b/ckpt", params)
    assert read_res.durations == [3.0, 4.0]
    assert read_res.window is None
    assert read_res.resource_metrics is None

    assert staged == ["gs://bucket/Llama-3.1-8B"] * 4
    expected_mod = (
        "gcsfs.tests.perf.subsystembenchmarks.checkpointing.ray_pytorch._worker"
    )
    assert [m for m, _ in calls] == [expected_mod] * 3
    ops = [args[args.index("--op") + 1] for _, args in calls]
    assert ops == ["save", "setup_load", "load"]


@pytest.mark.parametrize("ram_root", ["/dev/shm", None])
def test_run_ray_workers_sets_actor_tmpdir_and_kills_actors_in_finally(
    monkeypatch, ram_root
):
    monkeypatch.setattr(common, "_ram_staging_root", lambda: ram_root)
    monkeypatch.setattr(common, "ensure_ray_initialized", lambda ws: None)
    monkeypatch.setattr(common, "find_free_port", lambda: 29500)
    monkeypatch.setattr(accelerator, "ray_resources", lambda ws: {"num_gpus": 1.0})

    killed = []
    monkeypatch.setattr(common, "kill_actors", lambda workers: killed.extend(workers))
    monkeypatch.setattr(common.ray, "get", lambda refs: refs)

    recorded = []
    created = []

    class _Handle:
        def __getattr__(self, name):
            return SimpleNamespace(remote=lambda: name)

    class _FakeWorkerCls:
        def options(self, **kwargs):
            recorded.append(kwargs)

            def _remote(*args):
                h = _Handle()
                created.append(h)
                return h

            return SimpleNamespace(remote=_remote)

    results = common.run_ray_workers(
        _FakeWorkerCls(), 2, "gs://b/ckpt", object(), run_method="go"
    )
    assert results == ["go", "go"]
    assert killed == created
    if ram_root is None:
        assert recorded == [{"num_gpus": 1.0}] * 2
    else:
        assert (
            recorded
            == [{"num_gpus": 1.0, "runtime_env": {"env_vars": {"TMPDIR": ram_root}}}]
            * 2
        )

    # Actors are still killed if ray.get raises.
    killed.clear()
    created.clear()
    monkeypatch.setattr(
        common.ray,
        "get",
        lambda refs: (_ for _ in ()).throw(RuntimeError("actor boom")),
    )
    with pytest.raises(RuntimeError, match="actor boom"):
        common.run_ray_workers(
            _FakeWorkerCls(), 2, "gs://b/ckpt", object(), run_method="go"
        )
    assert killed == created


def test_tpu_bootstrap_clears_torch_tpu_env_and_calls_set_tpu_launch_env_before_ray_init(
    monkeypatch,
):
    events = []
    monkeypatch.setenv("TORCH_TPU_TOPOLOGY", "stale-topo")
    monkeypatch.setenv("TORCH_TPU_SLICEBUILDER_ADDRESSES", "stale-addr")
    monkeypatch.setenv("TORCH_TPU_CUSTOM_STALE", "1")
    monkeypatch.delenv("RAY_EXPERIMENTAL_NOSET_TPU_VISIBLE_CHIPS", raising=False)

    monkeypatch.setattr(accelerator, "device_kind", lambda: "tpu")
    monkeypatch.setattr(accelerator, "local_device_count", lambda: 8)
    monkeypatch.setattr(common.ray, "is_initialized", lambda: False)

    fake_tpu_dist = types.ModuleType("torch.tpu.distributed")

    def fake_set_tpu_launch_env(*, nproc_per_node):
        events.append(
            (
                "set_tpu_launch_env",
                nproc_per_node,
                os.environ.get("RAY_EXPERIMENTAL_NOSET_TPU_VISIBLE_CHIPS"),
                {k: v for k, v in os.environ.items() if k.startswith("TORCH_TPU_")},
            )
        )
        os.environ["TORCH_TPU_TOPOLOGY"] = f"1x{nproc_per_node}"

    fake_tpu_dist.set_tpu_launch_env = fake_set_tpu_launch_env
    fake_tpu = types.ModuleType("torch.tpu")
    fake_tpu.distributed = fake_tpu_dist
    monkeypatch.setitem(sys.modules, "torch.tpu", fake_tpu)
    monkeypatch.setitem(sys.modules, "torch.tpu.distributed", fake_tpu_dist)

    def fake_ray_init(**kwargs):
        events.append(
            (
                "ray.init",
                kwargs.get("address"),
                os.environ.get("RAY_EXPERIMENTAL_NOSET_TPU_VISIBLE_CHIPS"),
                os.environ.get("TORCH_TPU_TOPOLOGY"),
            )
        )

    monkeypatch.setattr(common.ray, "init", fake_ray_init)

    common.ensure_ray_initialized(4)

    assert events == [
        ("set_tpu_launch_env", 4, "1", {}),
        ("ray.init", "local", "1", "1x4"),
    ]


def test_tpu_single_rank_clears_torch_tpu_env_without_set_tpu_launch_env(monkeypatch):
    events = []
    monkeypatch.setenv("TORCH_TPU_TOPOLOGY", "stale-topo")
    monkeypatch.setenv("RAY_EXPERIMENTAL_NOSET_TPU_VISIBLE_CHIPS", "1")

    monkeypatch.setattr(accelerator, "device_kind", lambda: "tpu")
    monkeypatch.setattr(accelerator, "local_device_count", lambda: 8)
    monkeypatch.setattr(common.ray, "is_initialized", lambda: False)
    monkeypatch.setattr(
        common.ray,
        "init",
        lambda **kwargs: events.append(
            (
                "ray.init",
                kwargs.get("address"),
                os.environ.get("TORCH_TPU_TOPOLOGY"),
                os.environ.get("RAY_EXPERIMENTAL_NOSET_TPU_VISIBLE_CHIPS"),
            )
        ),
    )

    common.ensure_ray_initialized(1)
    assert events == [("ray.init", "local", None, None)]
    assert "RAY_EXPERIMENTAL_NOSET_TPU_VISIBLE_CHIPS" not in os.environ


def test_kill_actors_invokes_ray_kill_no_restart(monkeypatch):
    killed = []
    monkeypatch.setattr(common.ray, "is_initialized", lambda: True)
    monkeypatch.setattr(common.ray, "kill", lambda w, **kw: killed.append((w, kw)))
    common.kill_actors(["actor-0", "actor-1"])
    assert killed == [
        ("actor-0", {"no_restart": True}),
        ("actor-1", {"no_restart": True}),
    ]


def test_worker_main_runs_op_and_shuts_down_ray(monkeypatch, tmp_path):
    from gcsfs.tests.perf.subsystembenchmarks.checkpointing.ray_pytorch import (
        _worker,
    )

    events = []
    monkeypatch.setattr(
        _worker,
        "run_ray_save",
        lambda prefix, params: events.append(("save", prefix)) or [1.25],
    )
    monkeypatch.setattr(_worker, "shutdown_ray", lambda: events.append(("shutdown",)))

    spec_file = tmp_path / "spec.json"
    result_file = tmp_path / "result.json"
    params = _make_params(world_size=2, rounds=1)
    spec_file.write_text(
        json.dumps({"prefix": "gs://b/ckpt", "params": params.__dict__}),
        encoding="utf-8",
    )

    _worker.main(
        [
            "--op",
            "save",
            "--spec-file",
            str(spec_file),
            "--result-file",
            str(result_file),
        ]
    )

    assert events == [("save", "gs://b/ckpt"), ("shutdown",)]
    assert json.loads(result_file.read_text(encoding="utf-8")) == {"durations": [1.25]}


@pytest.mark.parametrize("world_size,expected_broadcast", [(1, False), (4, True)])
def test_load_full_state_dict_uses_weights_only_and_broadcast_matches_world_size(
    monkeypatch, world_size, expected_broadcast
):
    seen_options = []
    fake_device = torch.device("meta")

    monkeypatch.setattr(
        read_driver, "setup_distributed_env", lambda rank, ws, port: fake_device
    )
    monkeypatch.setattr(
        read_driver,
        "setup_model_and_optimizer",
        lambda params, dev=None: (object(), object()),
    )
    monkeypatch.setattr(
        read_driver,
        "resolve_storage",
        lambda prefix: (MagicMock(), MagicMock(), "b/ckpt"),
    )
    monkeypatch.setattr(
        read_driver.dist, "broadcast_object_list", lambda lst, src: None
    )
    monkeypatch.setattr(read_driver.dist, "barrier", lambda: None)
    monkeypatch.setattr(read_driver.dist, "destroy_process_group", lambda: None)
    load_kwargs = []
    monkeypatch.setattr(
        read_driver.torch,
        "load",
        lambda *a, **k: load_kwargs.append(k) or {"model": {}, "optimizer": {}},
    )
    monkeypatch.setattr(
        read_driver,
        "set_model_state_dict",
        lambda m, sd, *, options: seen_options.append(
            ("model", options.broadcast_from_rank0)
        ),
    )
    monkeypatch.setattr(
        read_driver,
        "set_optimizer_state_dict",
        lambda m, o, sd, *, options: seen_options.append(
            ("opt", options.broadcast_from_rank0)
        ),
    )
    monkeypatch.setattr(accelerator, "synchronize", lambda dev: None)

    class _FakeCkpt:
        def __init__(self, **kwargs):
            pass

        def as_directory(self):
            return contextlib.nullcontext("/tmp/fake")

    monkeypatch.setattr(read_driver.ray.train, "Checkpoint", _FakeCkpt)

    params = _make_params(
        strategy="single" if world_size == 1 else "ddp",
        world_size=world_size,
        rounds=1,
    )
    worker_cls = read_driver.RayCheckpointLoadWorker.__ray_metadata__.modified_class
    worker = worker_cls(0, world_size, 29502, "gs://b/ckpt", params)
    worker.setup()
    worker.load_rounds()

    assert seen_options == [
        ("model", expected_broadcast),
        ("opt", expected_broadcast),
    ]
    assert [k["weights_only"] for k in load_kwargs] == [True]


def test_load_and_save_rounds_synchronize_before_end(monkeypatch):
    events = []
    fake_device = torch.device("cuda", 0)

    monkeypatch.setattr(
        read_driver,
        "setup_distributed_env",
        lambda rank, ws, port: events.append(("setup_env", rank, ws, port))
        or fake_device,
    )
    monkeypatch.setattr(
        read_driver,
        "setup_model_and_optimizer",
        lambda params, dev=None: events.append(("setup_model", dev))
        or (object(), object()),
    )
    monkeypatch.setattr(
        read_driver,
        "resolve_storage",
        lambda prefix: (MagicMock(), MagicMock(), "b/ckpt"),
    )
    monkeypatch.setattr(
        read_driver.dist, "broadcast_object_list", lambda lst, src: None
    )
    monkeypatch.setattr(read_driver.dist, "barrier", lambda: events.append("barrier"))
    monkeypatch.setattr(read_driver.dist, "destroy_process_group", lambda: None)
    monkeypatch.setattr(read_driver, "get_state_dict", lambda *a, **k: ({}, {}))
    monkeypatch.setattr(
        read_driver.dcp, "load", lambda *a, **k: events.append("dcp.load")
    )
    monkeypatch.setattr(
        read_driver, "set_state_dict", lambda *a, **k: events.append("set_state_dict")
    )
    monkeypatch.setattr(
        accelerator,
        "synchronize",
        lambda dev: events.append(("synchronize", dev)),
    )

    ticks = iter([10.0, 25.0])
    monkeypatch.setattr(
        read_driver.time,
        "perf_counter",
        lambda: events.append("perf_counter") or next(ticks),
    )

    class _FakeCkpt:
        def __init__(self, **kwargs):
            pass

        def as_directory(self):
            return contextlib.nullcontext("/tmp/fake")

    monkeypatch.setattr(read_driver.ray.train, "Checkpoint", _FakeCkpt)

    params = _make_params(strategy="fsdp_sharded", world_size=2, dp=2, rounds=1)
    worker_cls = read_driver.RayCheckpointLoadWorker.__ray_metadata__.modified_class
    worker = worker_cls(0, 2, 29500, "gs://b/ckpt", params)
    worker.setup()
    assert worker.device == fake_device

    durations = worker.load_rounds()
    assert durations == [(10.0, 25.0)]
    assert events == [
        ("setup_env", 0, 2, 29500),
        ("setup_model", fake_device),
        "barrier",
        "perf_counter",
        "dcp.load",
        "set_state_dict",
        ("synchronize", fake_device),
        "barrier",
        "perf_counter",
    ]

    # Save worker
    events.clear()
    monkeypatch.setattr(
        write_driver,
        "setup_distributed_env",
        lambda rank, ws, port: events.append(("setup_env", rank, ws, port))
        or fake_device,
    )
    monkeypatch.setattr(
        write_driver,
        "setup_model_and_optimizer",
        lambda params, dev=None: events.append(("setup_model", dev))
        or (object(), object()),
    )
    monkeypatch.setattr(
        write_driver,
        "resolve_storage",
        lambda prefix: (MagicMock(), MagicMock(), "b/ckpt"),
    )
    monkeypatch.setattr(
        write_driver,
        "stage_checkpoint_locally",
        lambda *a, **k: events.append("stage") and None,
    )
    monkeypatch.setattr(write_driver.dist, "barrier", lambda: events.append("barrier"))
    monkeypatch.setattr(
        write_driver,
        "upload_checkpoint",
        lambda *a, **k: events.append("upload"),
    )
    monkeypatch.setattr(write_driver.dist, "destroy_process_group", lambda: None)

    ticks = iter([10.0, 25.0])
    monkeypatch.setattr(
        write_driver.time,
        "perf_counter",
        lambda: events.append("perf_counter") or next(ticks),
    )

    save_cls = write_driver.RayCheckpointSaveWorker.__ray_metadata__.modified_class
    save_worker = save_cls(0, 2, 29501, "gs://b/ckpt", params)
    save_worker.setup()
    assert save_worker.device == fake_device
    assert save_worker.save_rounds() == [(10.0, 25.0)]
    assert events == [
        ("setup_env", 0, 2, 29501),
        ("setup_model", fake_device),
        "stage",
        "barrier",
        "perf_counter",
        "upload",
        ("synchronize", fake_device),
        "perf_counter",
    ]
