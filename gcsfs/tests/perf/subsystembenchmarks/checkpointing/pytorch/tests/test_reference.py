import functools
import glob
import os
import re
import shutil
import uuid

import fsspec
import pytest
import torch
import torch.distributed as dist
from torch.distributed.device_mesh import init_device_mesh
from torch.distributed.tensor import Shard, distribute_tensor

pytest.importorskip("transformers")
from transformers import AutoModelForCausalLM  # noqa: E402

import gcsfs  # noqa: E402
from gcsfs.tests.perf.subsystembenchmarks.checkpointing._dist import (  # noqa: E402
    run_split,
    setup_distributed_env,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.model import (  # noqa: E402
    build_model,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.reference import (  # noqa: E402
    load_weights,
    local_slice,
    model_dir,
    random_full,
    stage_model,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.tests.tiny import (  # noqa: E402
    make_params,
    tiny_model_dir,
)


def test_model_dir():
    assert model_dir("gs://my-bucket/models/Llama-3.1-8B") == "/tmp/Llama-3.1-8B"
    assert model_dir("gs://my-bucket/models/Llama-3.1-8B/") == "/tmp/Llama-3.1-8B"
    assert model_dir("/var/tmp/local-model") == "/var/tmp/local-model"
    assert model_dir("meta-llama/Llama-3.1-8B") == "meta-llama/Llama-3.1-8B"


@pytest.mark.parametrize("kind", ["llama", "olmoe"])
def test_load_weights_matches_model_fqns(tmp_path, kind):
    model_path = tiny_model_dir(tmp_path, kind=kind)
    weights = load_weights(model_path)
    model = build_model(model_path)

    param_shapes = {name: p.shape for name, p in model.named_parameters()}
    assert set(weights.keys()) == set(param_shapes.keys())
    for fqn, tensor in weights.items():
        assert tensor.shape == param_shapes[fqn], fqn
        assert tensor.dtype == torch.bfloat16, fqn
        assert tensor.device == torch.device("cpu"), fqn
        assert tensor.is_shared(), fqn


def test_random_full_deterministic_and_seed_dependent():
    shape = torch.Size([16, 32])
    device = torch.device("cpu")
    a1 = random_full(42, "optim.state.w.exp_avg", shape, device)
    a2 = random_full(42, "optim.state.w.exp_avg", shape, device)
    a_tuple = random_full(42, "optim.state.w.exp_avg", (16, 32), device)
    diff_seed = random_full(43, "optim.state.w.exp_avg", shape, device)
    diff_key = random_full(42, "optim.state.w.exp_avg_sq", shape, device)

    assert a1.dtype == torch.float32
    assert a1.shape == shape
    assert a1.device == device
    assert torch.equal(a1, a2)
    assert torch.equal(a1, a_tuple)
    assert not torch.equal(a1, diff_seed)
    assert not torch.equal(a1, diff_key)


def _local_slice_rank(rank, world_size, port, prefix, params, q):
    setup_distributed_env(rank, world_size, port)
    try:
        full = torch.arange(8 * 12, dtype=torch.float32).reshape(8, 12)
        assert local_slice(full, full) is full

        mesh_1d = init_device_mesh("cpu", (4,))
        for dim, placements in ((0, [Shard(0)]), (1, [Shard(1)])):
            dt = distribute_tensor(full, mesh_1d, placements)
            sliced = local_slice(full, dt)
            assert torch.equal(sliced, dt.to_local())
            parts = [torch.empty_like(sliced) for _ in range(world_size)]
            dist.all_gather(parts, sliced.contiguous())
            assert torch.equal(torch.cat(parts, dim=dim), full)

        mesh_2d = init_device_mesh("cpu", (2, 2), mesh_dim_names=("dp", "tp"))
        dt_2d = distribute_tensor(full, mesh_2d, [Shard(0), Shard(1)])
        sliced_2d = local_slice(full, dt_2d)
        assert torch.equal(sliced_2d, dt_2d.to_local())
        parts_2d = [torch.empty_like(sliced_2d) for _ in range(world_size)]
        dist.all_gather(parts_2d, sliced_2d.contiguous())
        reassembled = torch.cat(
            [
                torch.cat([parts_2d[0], parts_2d[1]], dim=1),
                torch.cat([parts_2d[2], parts_2d[3]], dim=1),
            ],
            dim=0,
        )
        assert torch.equal(reassembled, full)

        q.put(None)
    finally:
        dist.destroy_process_group()


def test_local_slice_reassembles_full():
    assert run_split(None, make_params(world_size=4), _local_slice_rank, 4) == []


def test_stage_model_downloads_missing_weights(tmp_path, monkeypatch):
    src_dir = tiny_model_dir(tmp_path / "src", num_layers=2)
    name = f"test-stage-model-{uuid.uuid4().hex}"
    gcs_uri = f"gs://bucket/{name}"
    target_dir = f"/tmp/{name}"

    mem_fs = fsspec.filesystem("memory")
    remote_prefix = f"bucket/{name}"
    try:
        with open(os.path.join(src_dir, "config.json"), "rb") as f:
            config_bytes = f.read()
        with open(os.path.join(src_dir, "model.safetensors"), "rb") as f:
            weight_bytes = f.read()

        mem_fs.pipe(f"{remote_prefix}/config.json", config_bytes)
        mem_fs.pipe(f"{remote_prefix}/model.safetensors", weight_bytes)
        mem_fs.pipe(f"{remote_prefix}/original/consolidated.00.pth", b"X" * 64)
        monkeypatch.setattr(gcsfs, "GCSFileSystem", lambda: mem_fs)

        os.makedirs(target_dir, exist_ok=True)
        with open(os.path.join(target_dir, "config.json"), "wb") as f:
            f.write(config_bytes)
        assert not os.path.exists(os.path.join(target_dir, "model.safetensors"))

        staged = stage_model(gcs_uri)
        assert staged == target_dir
        assert os.path.isfile(os.path.join(target_dir, "config.json"))
        assert os.path.isfile(os.path.join(target_dir, "model.safetensors"))
        assert not os.path.exists(os.path.join(target_dir, "original"))
        assert not os.path.exists(os.path.join(target_dir, name))

        # Already staged: does not touch GCS again.
        monkeypatch.setattr(
            gcsfs,
            "GCSFileSystem",
            lambda: (_ for _ in ()).throw(AssertionError("unexpected GCS call")),
        )
        assert stage_model(gcs_uri) == target_dir
    finally:
        shutil.rmtree(target_dir, ignore_errors=True)
        if mem_fs.exists(remote_prefix):
            mem_fs.rm(remote_prefix, recursive=True)


def test_stage_model_restages_incomplete_sharded_dir(tmp_path, monkeypatch):
    src_dir = tiny_model_dir(tmp_path / "src", num_layers=2)
    sharded = tmp_path / "sharded"
    AutoModelForCausalLM.from_pretrained(src_dir).save_pretrained(
        sharded, max_shard_size="20KB"
    )
    shards = sorted(f for f in os.listdir(sharded) if f.endswith(".safetensors"))
    assert len(shards) >= 2

    name = f"test-stage-sharded-{uuid.uuid4().hex}"
    gcs_uri = f"gs://bucket/{name}"
    target_dir = f"/tmp/{name}"
    mem_fs = fsspec.filesystem("memory")
    remote_prefix = f"bucket/{name}"
    try:
        for fname in os.listdir(sharded):
            with open(sharded / fname, "rb") as f:
                mem_fs.pipe(f"{remote_prefix}/{fname}", f.read())
        monkeypatch.setattr(gcsfs, "GCSFileSystem", lambda: mem_fs)

        os.makedirs(target_dir, exist_ok=True)
        for fname in ("config.json", "model.safetensors.index.json", shards[0]):
            shutil.copyfile(sharded / fname, os.path.join(target_dir, fname))

        assert stage_model(gcs_uri) == target_dir
        assert sorted(os.listdir(target_dir)) == sorted(os.listdir(sharded))
        assert len(load_weights(gcs_uri)) > 0
    finally:
        shutil.rmtree(target_dir, ignore_errors=True)
        if mem_fs.exists(remote_prefix):
            mem_fs.rm(remote_prefix, recursive=True)


def test_stage_model_failed_copy_leaves_target_untouched(tmp_path, monkeypatch):
    src_dir = tiny_model_dir(tmp_path / "src", num_layers=2)
    name = f"test-stage-fail-{uuid.uuid4().hex}"
    gcs_uri = f"gs://bucket/{name}"
    target_dir = f"/tmp/{name}"
    mem_fs = fsspec.filesystem("memory")
    remote_prefix = f"bucket/{name}"
    try:
        for fname in ("config.json", "model.safetensors"):
            with open(os.path.join(src_dir, fname), "rb") as f:
                mem_fs.pipe(f"{remote_prefix}/{fname}", f.read())

        def fail_get(rpaths, lpaths, *args, **kwargs):
            dest = (
                lpaths[0]
                if isinstance(lpaths, list)
                else os.path.join(lpaths, "partial.safetensors")
            )
            os.makedirs(os.path.dirname(dest), exist_ok=True)
            with open(dest, "wb") as f:
                f.write(b"partial")
            raise OSError("boom")

        monkeypatch.setattr(mem_fs, "get", fail_get)
        monkeypatch.setattr(gcsfs, "GCSFileSystem", lambda: mem_fs)

        os.makedirs(target_dir, exist_ok=True)
        shutil.copyfile(
            os.path.join(src_dir, "config.json"),
            os.path.join(target_dir, "config.json"),
        )

        with pytest.raises(OSError, match="boom"):
            stage_model(gcs_uri)
        assert os.listdir(target_dir) == ["config.json"]
        assert glob.glob(f"/tmp/.{name}.*") == []
    finally:
        shutil.rmtree(target_dir, ignore_errors=True)
        for p in glob.glob(f"/tmp/.{name}.*"):
            shutil.rmtree(p, ignore_errors=True)
        if mem_fs.exists(remote_prefix):
            mem_fs.rm(remote_prefix, recursive=True)


def test_stage_model_local_without_weights_raises(tmp_path):
    empty_dir = tmp_path / "no_weights"
    empty_dir.mkdir()
    (empty_dir / "config.json").write_text("{}")

    with pytest.raises(FileNotFoundError, match=re.escape(str(empty_dir))):
        stage_model(str(empty_dir))

    valid_dir = tiny_model_dir(tmp_path / "with_weights", num_layers=2)
    assert stage_model(valid_dir) == valid_dir


def _shared_weights_rank(rank, world_size, port, prefix, params, q, *, weights):
    setup_distributed_env(rank, world_size, port)
    try:
        assert len(weights) > 0
        for fqn, tensor in weights.items():
            assert tensor.is_shared(), fqn
            assert tensor.dtype == torch.bfloat16, fqn

        fresh = AutoModelForCausalLM.from_pretrained(
            params.model_id, dtype=torch.bfloat16
        ).state_dict()
        key = "model.layers.0.self_attn.q_proj.weight"
        assert torch.equal(weights[key], fresh[key])
        q.put(None)
    finally:
        dist.destroy_process_group()


def test_weights_shared_with_spawned_ranks(tmp_path):
    model_path = tiny_model_dir(tmp_path, num_layers=2)
    weights = load_weights(model_path)
    target = functools.partial(_shared_weights_rank, weights=weights)
    assert run_split(None, make_params(model_path, world_size=2), target, 2) == []
