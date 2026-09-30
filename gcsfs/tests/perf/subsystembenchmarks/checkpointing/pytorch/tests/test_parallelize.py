import json
import os

import pytest
import torch
import torch.distributed as dist
from torch.distributed.tensor import DTensor, Replicate, Shard
from torch.distributed.tensor.placement_types import _StridedShard
from torch.testing._internal.distributed.fake_pg import FakeStore

pytest.importorskip("transformers")

from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.model import (
    build_model,
    build_optimizer,
    materialize,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.parallelize import (
    build_mesh,
    parallelize,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.parameters import (
    PyTorchCheckpointParameters,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.state import (
    expected_bytes_requested,
    get_fqn_state_dict,
    state_checksum,
)


def _create_tiny_llama_config_dir(tmp_path):
    config = {
        "model_type": "llama",
        "hidden_size": 64,
        "num_hidden_layers": 2,
        "num_attention_heads": 8,
        "num_key_value_heads": 8,
        "intermediate_size": 128,
        "vocab_size": 256,
        "max_position_embeddings": 512,
        "rope_scaling": None,
    }
    config_file = tmp_path / "config.json"
    config_file.write_text(json.dumps(config))
    return str(tmp_path)


def test_build_materialize_optimizer(tmp_path):
    config_dir = _create_tiny_llama_config_dir(tmp_path)
    params = PyTorchCheckpointParameters(
        name="test-case",
        bucket_name="test-bucket",
        rounds=1,
        framework="pytorch",
        scenario="checkpoint_read",
        strategy="fsdp",
        bucket_type="regional",
        model_id=config_dir,
        world_size=8,
    )

    model = build_model(params)

    # 1. Before materialize: all parameters must be on meta device
    model_params = list(model.parameters())
    assert len(model_params) > 0
    assert all(p.is_meta for p in model_params)

    # 2. Materialize
    materialize(model, seed=42)

    # 3. After materialize: all parameters on CPU, all require grad
    assert all(not p.is_meta and p.device.type == "cpu" for p in model.parameters())
    assert all(p.requires_grad for p in model.parameters())

    # 4. Optimizer: AdamW with exp_avg/exp_avg_sq populated for every parameter
    optimizer = build_optimizer(model)
    assert len(optimizer.state) == len(list(model.parameters()))
    for p in model.parameters():
        assert p in optimizer.state
        state = optimizer.state[p]
        assert "exp_avg" in state
        assert "exp_avg_sq" in state
        assert "step" in state
        assert state["exp_avg"].shape == p.shape
        assert state["exp_avg_sq"].shape == p.shape
        assert state["exp_avg"].device == p.device
        assert state["exp_avg_sq"].device == p.device
        assert torch.all(state["exp_avg"] == 0)
        assert torch.all(state["exp_avg_sq"] == 0)


def test_materialize_seed_determinism(tmp_path):
    config_dir = _create_tiny_llama_config_dir(tmp_path)
    params = PyTorchCheckpointParameters(
        name="test-case",
        bucket_name="test-bucket",
        rounds=1,
        framework="pytorch",
        scenario="checkpoint_read",
        strategy="fsdp",
        bucket_type="regional",
        model_id=config_dir,
        world_size=8,
    )

    model1 = build_model(params)
    materialize(model1, seed=123)

    model2 = build_model(params)
    materialize(model2, seed=123)

    model3 = build_model(params)
    materialize(model3, seed=456)

    # Check identical seeds produce identical values
    for p1, p2 in zip(model1.parameters(), model2.parameters()):
        assert torch.equal(p1, p2)

    # Check different seeds produce different values
    has_diff = any(
        not torch.equal(p1, p3)
        for p1, p3 in zip(model1.parameters(), model3.parameters())
    )
    assert has_diff


def test_resolve_model_config_staged(tmp_path, monkeypatch):
    from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.model import (
        _resolve_model_config,
    )

    staged_model_dir = tmp_path / "my_model"
    staged_model_dir.mkdir()
    config_data = {
        "model_type": "llama",
        "hidden_size": 32,
        "num_hidden_layers": 1,
        "num_attention_heads": 2,
        "num_key_value_heads": 1,
        "intermediate_size": 64,
        "vocab_size": 128,
        "max_position_embeddings": 256,
    }
    (staged_model_dir / "config.json").write_text(json.dumps(config_data))

    orig_join = os.path.join
    monkeypatch.setattr(
        "os.path.join",
        lambda *args: (
            str(staged_model_dir) if args == ("/tmp", "my_model") else orig_join(*args)
        ),
    )
    cfg = _resolve_model_config("gs://my-bucket/models/my_model")
    assert cfg.hidden_size == 32


def test_resolve_model_config_gcs_fallback(tmp_path, monkeypatch):
    from unittest.mock import MagicMock

    from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.model import (
        _resolve_model_config,
    )

    config_data = {
        "model_type": "llama",
        "hidden_size": 48,
        "num_hidden_layers": 1,
        "num_attention_heads": 2,
        "num_key_value_heads": 1,
        "intermediate_size": 96,
        "vocab_size": 128,
        "max_position_embeddings": 256,
    }

    mock_fs = MagicMock()

    def fake_get_file(rpath, lpath):
        with open(lpath, "w") as f:
            json.dump(config_data, f)

    mock_fs.get_file.side_effect = fake_get_file
    monkeypatch.setattr("gcsfs.GCSFileSystem", lambda: mock_fs)

    cfg = _resolve_model_config("gs://my-bucket/models/unstaged_model")
    assert cfg.hidden_size == 48
    assert mock_fs.get_file.call_count == 1
    assert mock_fs.get_file.call_args[0][0] == (
        "gs://my-bucket/models/unstaged_model/config.json"
    )


@pytest.fixture
def fake_pg():
    dist.init_process_group("fake", store=FakeStore(), rank=0, world_size=8)
    yield
    dist.destroy_process_group()


def _params(config_dir, strategy, dpr=1, dp=1, tp=1):
    return PyTorchCheckpointParameters(
        name="test-case",
        bucket_name="test-bucket",
        rounds=1,
        framework="pytorch",
        scenario="checkpoint_read",
        strategy=strategy,
        bucket_type="regional",
        model_id=config_dir,
        world_size=8,
        data_parallel_replicate_size=dpr,
        data_parallel_size=dp,
        tensor_parallel_size=tp,
    )


_LAYOUTS = [
    pytest.param(
        dict(strategy="fsdp", dp=8),
        ("dp_shard",),
        (8,),
        (Shard(0),),
        (Shard(0),),
        (Shard(0),),
        id="fsdp",
    ),
    pytest.param(
        dict(strategy="hsdp", dpr=2, dp=4),
        ("dp_replicate", "dp_shard"),
        (2, 4),
        (Replicate(), Shard(0)),
        (Replicate(), Shard(0)),
        (Replicate(), Shard(0)),
        id="hsdp-r2s4",
    ),
    pytest.param(
        dict(strategy="hsdp", dpr=4, dp=2),
        ("dp_replicate", "dp_shard"),
        (4, 2),
        (Replicate(), Shard(0)),
        (Replicate(), Shard(0)),
        (Replicate(), Shard(0)),
        id="hsdp-r4s2",
    ),
    pytest.param(
        dict(strategy="tp", tp=8),
        ("tp",),
        (8,),
        (Shard(0),),
        (Shard(1),),
        None,
        id="tp",
    ),
    pytest.param(
        dict(strategy="fsdp_tp", dp=2, tp=4),
        ("dp_shard", "tp"),
        (2, 4),
        (_StridedShard(0, split_factor=4), Shard(0)),
        (Shard(0), Shard(1)),
        (Shard(0),),
        id="fsdp_tp",
    ),
    pytest.param(
        dict(strategy="hsdp_tp", dpr=2, dp=2, tp=2),
        ("dp_replicate", "dp_shard", "tp"),
        (2, 2, 2),
        (Replicate(), _StridedShard(0, split_factor=2), Shard(0)),
        (Replicate(), Shard(0), Shard(1)),
        (Replicate(), Shard(0)),
        id="hsdp_tp",
    ),
]


@pytest.mark.parametrize(
    "sizes, dim_names, shape, q_placements, o_placements, norm_placements", _LAYOUTS
)
def test_parallelize_placements(
    tmp_path,
    fake_pg,
    sizes,
    dim_names,
    shape,
    q_placements,
    o_placements,
    norm_placements,
):
    params = _params(_create_tiny_llama_config_dir(tmp_path), **sizes)
    model = build_model(params)
    mesh = build_mesh(params)
    assert mesh.mesh_dim_names == dim_names
    assert tuple(mesh.shape) == shape

    model = parallelize(model, mesh, params)
    layer = model.model.layers[0]
    assert layer.self_attn.q_proj.weight.placements == q_placements
    assert layer.self_attn.o_proj.weight.placements == o_placements
    norm = layer.input_layernorm.weight
    if norm_placements is None:
        assert not isinstance(norm, DTensor)
    else:
        assert norm.placements == norm_placements


def test_parallelize_rejects_unknown_strategy(tmp_path, fake_pg):
    params = _params(_create_tiny_llama_config_dir(tmp_path), "fsdp", dp=8)
    mesh = build_mesh(params)
    params.strategy = "ddp"
    with pytest.raises(ValueError, match="ddp"):
        parallelize(build_model(params), mesh, params)


def _parallel_state(tmp_path, **sizes):
    params = _params(_create_tiny_llama_config_dir(tmp_path), **sizes)
    model = parallelize(build_model(params), build_mesh(params), params)
    materialize(model, seed=0)
    optim = build_optimizer(model)
    return model, get_fqn_state_dict(model, optim)


def test_get_fqn_state_dict_is_sharded_and_fqn_keyed(tmp_path, fake_pg):
    model, state = _parallel_state(tmp_path, strategy="fsdp", dp=8)
    fqn = "model.layers.0.self_attn.q_proj.weight"
    assert set(state) == {"model", "optim"}
    assert isinstance(state["model"][fqn], DTensor)
    assert state["model"][fqn].to_local().shape == (8, 64)
    assert set(state["optim"]["state"][fqn]) == {"step", "exp_avg", "exp_avg_sq"}
    assert fqn in state["optim"]["param_groups"][0]["params"]


@pytest.mark.parametrize(
    "sizes",
    [
        pytest.param(dict(strategy="fsdp", dp=8), id="fsdp"),
        pytest.param(dict(strategy="hsdp", dpr=2, dp=4), id="hsdp-r2s4"),
        pytest.param(dict(strategy="tp", tp=8), id="tp"),
        pytest.param(dict(strategy="fsdp_tp", dp=2, tp=4), id="fsdp_tp"),
    ],
)
def test_expected_bytes_requested(tmp_path, fake_pg, sizes):
    model, state = _parallel_state(tmp_path, **sizes)
    shard_degree = 8 // sizes.get("dpr", 1)
    expected = 0
    for name, p in model.named_parameters():
        nbytes = p.numel() * p.element_size()
        tp_only_norm = sizes["strategy"] == "tp" and "norm" in name
        local = nbytes if tp_only_norm else nbytes // shard_degree
        if sizes["strategy"] == "fsdp_tp" and "norm" in name:
            local = nbytes // sizes["dp"]
        # param + exp_avg + exp_avg_sq (bf16) + int64 step
        expected += 3 * local + 8
    assert expected_bytes_requested(state) == expected


def test_expected_bytes_requested_ignores_non_tensors():
    state = {
        "a": torch.zeros(4, dtype=torch.float32),
        "b": {"c": torch.zeros(2, 3, dtype=torch.bfloat16), "lr": 1e-3},
        "groups": [{"params": ["a"], "lr": 1e-3}],
    }
    assert expected_bytes_requested(state) == 16 + 12


def test_state_checksum(tmp_path, fake_pg):
    _, state = _parallel_state(tmp_path, strategy="fsdp", dp=8)
    checksum = state_checksum(state)
    fqn = "model.layers.0.self_attn.q_proj.weight"
    assert checksum == state_checksum(state)
    assert checksum[f"model.{fqn}"] != checksum[f"optim.state.{fqn}.exp_avg"]
    assert f"optim.state.{fqn}.step" in checksum
    assert all(isinstance(v, str) and len(v) == 64 for v in checksum.values())
    assert not any(k.startswith("optim.param_groups") for k in checksum)


def test_state_checksum_detects_permutation():
    a = state_checksum({"x": torch.tensor([1.0, 2.0])})
    b = state_checksum({"x": torch.tensor([2.0, 1.0])})
    assert a != b
