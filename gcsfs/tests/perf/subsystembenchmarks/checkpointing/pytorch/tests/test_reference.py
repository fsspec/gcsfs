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
from torch.distributed.tensor import DTensor, Replicate, Shard, distribute_tensor
from torch.distributed.tensor.placement_types import _StridedShard
from torch.testing._internal.distributed.fake_pg import FakeStore

pytest.importorskip("transformers")
from transformers import AutoModelForCausalLM  # noqa: E402

import gcsfs  # noqa: E402
from gcsfs.tests.perf.subsystembenchmarks.checkpointing._dist import (  # noqa: E402
    setup_distributed_env,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.model import (  # noqa: E402
    SETUP_SEED,
    build_model,
    expected_local,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.reference import (  # noqa: E402
    MmapWeightStore,
    ensure_canonical_weights,
    load_weights,
    local_slice,
    model_dir,
    random_full,
    random_local,
    stage_model,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.tests.tiny import (  # noqa: E402
    make_params,
    run_test_ranks,
    tiny_model_dir,
)


def test_model_dir():
    assert model_dir("gs://my-bucket/models/Llama-3.1-8B") == "/tmp/Llama-3.1-8B"
    assert model_dir("gs://my-bucket/models/Llama-3.1-8B/") == "/tmp/Llama-3.1-8B"
    assert model_dir("/var/tmp/local-model") == "/var/tmp/local-model"
    assert model_dir("meta-llama/Llama-3.1-8B") == "meta-llama/Llama-3.1-8B"


def test_stage_model_rejects_dot_and_traversal_segments(monkeypatch):
    for bad_uri in (
        "gs://",
        "gs:///",
        "gs://.",
        "gs://..",
        "gs://bucket/.",
        "gs://bucket/..",
    ):
        with pytest.raises(ValueError, match="Invalid GCS model_id"):
            model_dir(bad_uri)

    name = f"test-stage-dot-{uuid.uuid4().hex}"
    gcs_uri = f"gs://bucket/{name}"
    target_dir = f"/tmp/{name}"

    class _FakeFS:
        def ls(self, prefix, detail=True):
            return [
                {"name": f"bucket/{name}/config.json", "type": "file"},
                {"name": f"bucket/{name}/..", "type": "file"},
            ]

        def get(self, rpaths, lpaths):
            raise AssertionError("fs.get must not run on traversal object keys")

    monkeypatch.setattr(gcsfs, "GCSFileSystem", _FakeFS)
    try:
        with pytest.raises(ValueError, match="Invalid remote object key"):
            stage_model(gcs_uri)
        assert not os.path.exists(target_dir)
        assert glob.glob(f"/tmp/.{name}.*") == []
    finally:
        shutil.rmtree(target_dir, ignore_errors=True)


@pytest.mark.parametrize("kind", ["llama", "olmoe"])
def test_load_weights_matches_model_fqns(tmp_path, kind):
    model_path = tiny_model_dir(tmp_path, kind=kind)
    weights = load_weights(model_path)
    assert isinstance(weights, MmapWeightStore)
    model = build_model(model_path)

    param_shapes = {name: p.shape for name, p in model.named_parameters()}
    assert set(weights.keys()) == set(param_shapes.keys())
    first_fqn = next(iter(param_shapes))
    for fqn, tensor in weights.items():
        assert tensor.shape == param_shapes[fqn], fqn
        assert tensor.dtype == torch.bfloat16, fqn
        assert tensor.device == torch.device("cpu"), fqn

    weights.close()
    # Second close is a no-op; reads after close raise ValueError.
    weights.close()
    with pytest.raises(ValueError, match="closed"):
        _ = weights[first_fqn]


def test_mmap_weight_store_slices_without_full_copy(tmp_path, monkeypatch):
    from torch.distributed.tensor._utils import compute_local_shape_and_global_offset

    model_path = tiny_model_dir(tmp_path, kind="llama", num_layers=1)
    fqn = "model.layers.0.self_attn.q_proj.weight"

    with load_weights(model_path) as weights:
        assert isinstance(weights, MmapWeightStore)
        full = weights[fqn]
        monkeypatch.setattr(
            MmapWeightStore,
            "__getitem__",
            lambda self, k: (_ for _ in ()).throw(
                AssertionError("expected_local must not materialize full tensor")
            ),
        )
        for rank in range(4):
            dist.init_process_group("fake", store=FakeStore(), rank=rank, world_size=4)
            try:
                mesh_1d = init_device_mesh("cpu", (4,))
                mesh_2d = init_device_mesh("cpu", (2, 2), mesh_dim_names=("dp", "tp"))
                for mesh, placements in (
                    (mesh_1d, [Shard(0)]),
                    (mesh_1d, [Shard(1)]),
                    (mesh_2d, [Shard(0), Shard(1)]),
                ):
                    local_shape, offsets = compute_local_shape_and_global_offset(
                        full.shape, mesh, placements
                    )
                    dt = DTensor.from_local(
                        torch.empty(local_shape, dtype=torch.float32),
                        mesh,
                        placements,
                        run_check=False,
                    )
                    slices = tuple(
                        slice(o, o + s) for s, o in zip(local_shape, offsets)
                    )
                    got = weights.get_slice(fqn, slices)
                    ref = local_slice(full, dt)
                    assert got.dtype == torch.bfloat16
                    assert torch.equal(got, ref)
                    assert torch.equal(
                        expected_local(
                            fqn, dt, SETUP_SEED, weights, torch.device("cpu")
                        ),
                        ref.to(dtype=torch.float32),
                    )
            finally:
                dist.destroy_process_group()


@pytest.mark.parametrize("kind", ["llama", "olmoe"])
def test_canonical_safetensors_recovers_from_stale_or_corrupt_file(
    tmp_path, monkeypatch, kind
):
    from safetensors import safe_open

    from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch import (
        reference as ref_mod,
    )

    model_path = tiny_model_dir(tmp_path, kind=kind, num_layers=1)
    canonical = os.path.join(model_path, ".canonical.safetensors")
    param_shapes = {
        name: tuple(p.shape) for name, p in build_model(model_path).named_parameters()
    }

    # 1. Corrupt/truncated .canonical.safetensors is atomically regenerated.
    with open(canonical, "wb") as f:
        f.write(b"not-a-valid-safetensors-header")

    assert ensure_canonical_weights(model_path) == model_path
    assert os.path.isfile(canonical)
    assert glob.glob(os.path.join(model_path, ".canonical.*.tmp")) == []
    with safe_open(canonical, framework="pt", device="cpu") as sf:
        assert set(sf.keys()) == set(param_shapes.keys())
        for fqn, expected_shape in param_shapes.items():
            sl = sf.get_slice(fqn)
            assert tuple(sl.get_shape()) == expected_shape
            assert sl.get_dtype() == "BF16"

    # 2. Fresh valid .canonical.safetensors is reused without re-loading HF model.
    monkeypatch.setattr(
        ref_mod.AutoModelForCausalLM,
        "from_pretrained",
        lambda *a, **kw: (_ for _ in ()).throw(AssertionError("unexpected reload")),
    )
    assert ensure_canonical_weights(model_path) == model_path

    # 3. Stale mtime (older than source shard) triggers regeneration.
    monkeypatch.undo()
    src_shard = os.path.join(model_path, "model.safetensors")
    src_mtime = os.path.getmtime(src_shard)
    os.utime(canonical, (src_mtime - 60, src_mtime - 60))

    reloads = []
    orig_from_pretrained = ref_mod.AutoModelForCausalLM.from_pretrained

    def spy_from_pretrained(*args, **kwargs):
        reloads.append((args, kwargs))
        return orig_from_pretrained(*args, **kwargs)

    monkeypatch.setattr(
        ref_mod.AutoModelForCausalLM, "from_pretrained", spy_from_pretrained
    )
    assert ensure_canonical_weights(model_path) == model_path
    assert len(reloads) == 1
    assert os.path.getmtime(canonical) >= src_mtime
    assert glob.glob(os.path.join(model_path, ".canonical.*.tmp")) == []
    with safe_open(canonical, framework="pt", device="cpu") as sf:
        assert set(sf.keys()) == set(param_shapes.keys())
        for fqn, expected_shape in param_shapes.items():
            sl = sf.get_slice(fqn)
            assert tuple(sl.get_shape()) == expected_shape
            assert sl.get_dtype() == "BF16"


def test_ensure_canonical_weights_skips_conversion_for_matching_bf16_shards(
    tmp_path, monkeypatch
):
    from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch import (
        reference as ref_mod,
    )

    src_dir = tiny_model_dir(tmp_path / "src", kind="llama", num_layers=1)
    bf16_dir = tmp_path / "bf16"
    AutoModelForCausalLM.from_pretrained(
        src_dir, dtype=torch.bfloat16, local_files_only=True
    ).save_pretrained(bf16_dir)

    monkeypatch.setattr(
        ref_mod.AutoModelForCausalLM,
        "from_pretrained",
        lambda *a, **kw: (_ for _ in ()).throw(AssertionError("unexpected reload")),
    )
    assert ensure_canonical_weights(str(bf16_dir)) == str(bf16_dir)
    assert not os.path.exists(bf16_dir / ".canonical.safetensors")
    with load_weights(str(bf16_dir)) as weights:
        assert isinstance(weights, MmapWeightStore)
        assert len(weights) > 0


def test_ensure_canonical_weights_filters_buffers_and_dedups_offset_views(
    tmp_path, monkeypatch
):
    from safetensors import safe_open

    from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch import (
        reference as ref_mod,
    )

    model_path = tiny_model_dir(tmp_path / "src", kind="llama", num_layers=1)
    canonical = os.path.join(model_path, ".canonical.safetensors")
    meta_model = build_model(model_path)
    param_shapes = {name: tuple(p.shape) for name, p in meta_model.named_parameters()}
    del meta_model

    # Build a synthetic state_dict that:
    # (a) includes a persistent buffer absent from named_parameters(), and
    # (b) backs two parameters as non-overlapping views at different offsets into
    #     the same underlying storage (different data_ptr, identical untyped_storage().data_ptr()).
    orig_state = AutoModelForCausalLM.from_pretrained(
        model_path, dtype=torch.bfloat16, local_files_only=True
    ).state_dict()
    keys = list(param_shapes)
    k0, k1 = keys[0], keys[1]
    n0, n1 = orig_state[k0].numel(), orig_state[k1].numel()
    shared_storage = torch.arange(n0 + n1, dtype=torch.bfloat16)
    view0 = shared_storage[:n0].view(param_shapes[k0])
    view1 = shared_storage[n0 : n0 + n1].view(param_shapes[k1])
    assert view0.data_ptr() != view1.data_ptr()
    assert view0.untyped_storage().data_ptr() == view1.untyped_storage().data_ptr()

    synthetic_state = dict(orig_state)
    synthetic_state[k0] = view0
    synthetic_state[k1] = view1
    synthetic_state["model.rotary_emb.inv_freq"] = torch.ones(8, dtype=torch.bfloat16)

    class _FakeModel:
        def state_dict(self):
            return synthetic_state

    load_calls = []

    def _fake_from_pretrained(*args, **kwargs):
        load_calls.append((args, kwargs))
        return _FakeModel()

    monkeypatch.setattr(
        ref_mod.AutoModelForCausalLM, "from_pretrained", _fake_from_pretrained
    )

    assert ensure_canonical_weights(model_path) == model_path
    assert len(load_calls) == 1
    assert os.path.isfile(canonical)

    with safe_open(canonical, framework="pt", device="cpu") as sf:
        assert set(sf.keys()) == set(param_shapes.keys())
        assert "model.rotary_emb.inv_freq" not in sf.keys()

    # Subsequent load_weights() reuses .canonical.safetensors without reloading HF model.
    monkeypatch.setattr(
        ref_mod.AutoModelForCausalLM,
        "from_pretrained",
        lambda *a, **kw: (_ for _ in ()).throw(AssertionError("unexpected reload")),
    )
    with load_weights(model_path) as weights:
        assert set(weights.keys()) == set(param_shapes.keys())
        assert torch.equal(weights[k0], view0)
        assert torch.equal(weights[k1], view1)

    # Missing expected parameter raises a clear KeyError.
    os.unlink(canonical)
    incomplete_state = dict(orig_state)
    incomplete_state.pop(k0)

    class _IncompleteModel:
        def state_dict(self):
            return incomplete_state

    monkeypatch.setattr(
        ref_mod.AutoModelForCausalLM,
        "from_pretrained",
        lambda *a, **kw: _IncompleteModel(),
    )
    with pytest.raises(KeyError, match=re.escape(k0)):
        ensure_canonical_weights(model_path)


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
    run_test_ranks(None, make_params(world_size=4), _local_slice_rank, 4)


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
        assert isinstance(weights, MmapWeightStore)
        assert len(weights) > 0
        for fqn, tensor in weights.items():
            assert tensor.dtype == torch.bfloat16, fqn

        fresh = AutoModelForCausalLM.from_pretrained(
            params.model_id, dtype=torch.bfloat16
        ).state_dict()
        key = "model.layers.0.self_attn.q_proj.weight"
        assert torch.equal(weights[key], fresh[key])
        q.put(None)
    finally:
        weights.close()
        dist.destroy_process_group()


def test_weights_shared_with_spawned_ranks(tmp_path):
    model_path = tiny_model_dir(tmp_path, num_layers=2)
    with load_weights(model_path) as weights:
        target = functools.partial(_shared_weights_rank, weights=weights)
        run_test_ranks(None, make_params(model_path, world_size=2), target, 2)


def test_random_local_matches_full_slice_with_local_storage():
    cpu = torch.device("cpu")
    for shape in ((), (16,), (8, 12), (4, 6, 8)):
        plain = torch.empty(shape, dtype=torch.float32)
        got = random_local(7, "k", plain, cpu)
        ref = random_full(7, "k", shape, cpu)
        assert torch.equal(got, ref)
        assert got.untyped_storage().nbytes() == plain.numel() * 4

    replicas = {}
    for rank in range(4):
        dist.init_process_group("fake", store=FakeStore(), rank=rank, world_size=4)
        try:
            m1 = init_device_mesh("cpu", (4,))
            m2 = init_device_mesh("cpu", (2, 2), mesh_dim_names=("dp", "tp"))
            cases = [
                DTensor.from_local(torch.empty(4), m1, [Shard(0)], run_check=False),
                DTensor.from_local(torch.empty(2, 12), m1, [Shard(0)], run_check=False),
                DTensor.from_local(torch.empty(8, 3), m1, [Shard(1)], run_check=False),
                DTensor.from_local(
                    torch.empty(2, 6),
                    m2,
                    [_StridedShard(0, split_factor=2), Shard(0)],
                    run_check=False,
                ),
                DTensor.from_local(
                    torch.empty(4, 6, 2), m2, [Replicate(), Shard(0)], run_check=False
                ),
            ]
            for idx, dt in enumerate(cases):
                got = random_local(11, f"p.{idx}", dt, cpu)
                ref = local_slice(random_full(11, f"p.{idx}", dt.shape, cpu), dt)
                assert torch.equal(got, ref), (rank, idx)
                assert got.untyped_storage().nbytes() == got.numel() * 4
                if idx == 4:
                    replicas[rank] = got
        finally:
            dist.destroy_process_group()
    assert torch.equal(replicas[0], replicas[2])
    assert torch.equal(replicas[1], replicas[3])
    assert not torch.equal(replicas[0], replicas[1])

    if torch.cuda.is_available():
        cuda = torch.device("cuda", 0)
        assert torch.equal(
            random_local(11, "cuda.k", torch.empty(8, 12), cuda).cpu(),
            random_full(11, "cuda.k", (8, 12), cpu),
        )


def test_model_and_reference_from_pretrained_use_local_files_only(
    tmp_path, monkeypatch
):
    from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch import (
        model as model_mod,
    )
    from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch import (
        reference as ref_mod,
    )

    local_dir = tiny_model_dir(tmp_path / "local", num_layers=1)
    config_calls = []
    orig_cfg_from_pretrained = model_mod.AutoConfig.from_pretrained

    def spy_cfg_from_pretrained(pretrained_path, *args, **kwargs):
        config_calls.append((pretrained_path, dict(kwargs)))
        return orig_cfg_from_pretrained(pretrained_path, *args, **kwargs)

    model_calls = []
    orig_model_from_pretrained = ref_mod.AutoModelForCausalLM.from_pretrained

    def spy_model_from_pretrained(pretrained_path, *args, **kwargs):
        model_calls.append((pretrained_path, dict(kwargs)))
        return orig_model_from_pretrained(pretrained_path, *args, **kwargs)

    monkeypatch.setattr(
        model_mod.AutoConfig, "from_pretrained", spy_cfg_from_pretrained
    )
    monkeypatch.setattr(
        ref_mod.AutoModelForCausalLM, "from_pretrained", spy_model_from_pretrained
    )

    # 1. Local directory path in build_model
    build_model(local_dir)

    # 2. Pre-staged /tmp/<name>/config.json branch in build_model
    staged_name = f"test-local-only-staged-{uuid.uuid4().hex}"
    staged_tmp = f"/tmp/{staged_name}"
    os.makedirs(staged_tmp, exist_ok=True)
    try:
        shutil.copyfile(
            os.path.join(local_dir, "config.json"),
            os.path.join(staged_tmp, "config.json"),
        )
        build_model(f"gs://bucket/{staged_name}")
    finally:
        shutil.rmtree(staged_tmp, ignore_errors=True)

    # 3. Unstaged GCS config.json fetch branch in build_model
    unstaged_name = f"test-local-only-unstaged-{uuid.uuid4().hex}"

    class _FakeConfigFS:
        def get_file(self, rpath, lpath):
            assert rpath == f"gs://bucket/{unstaged_name}/config.json"
            shutil.copyfile(os.path.join(local_dir, "config.json"), lpath)

    monkeypatch.setattr(gcsfs, "GCSFileSystem", _FakeConfigFS)
    build_model(f"gs://bucket/{unstaged_name}")

    # 4. Reference weight load in load_weights
    weights = load_weights(local_dir)
    weights.clear()

    assert len(config_calls) >= 3
    for path, kwargs in config_calls:
        assert kwargs.get("local_files_only") is True, (path, kwargs)
    assert len(model_calls) == 1
    assert model_calls[0][1].get("local_files_only") is True
