import json
import os

import pytest
import torch
import torch.distributed as dist
from torch.distributed.tensor import DTensor, Replicate, Shard
from torch.distributed.tensor._utils import compute_local_shape_and_global_offset
from torch.distributed.tensor.placement_types import _StridedShard
from torch.testing._internal.distributed.fake_pg import FakeStore

pytest.importorskip("transformers")

from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.layout import (  # noqa: E402
    Layout,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.model import (  # noqa: E402
    SETUP_SEED,
    build,
    build_model,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.parallelize import (  # noqa: E402
    build_mesh,
    parallelize,
    split_stage,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.reference import (  # noqa: E402
    load_weights,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.state import (  # noqa: E402
    expected_bytes_requested,
    get_fqn_state_dict,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.tests.tiny import (  # noqa: E402
    tiny_model_dir,
)


def _layout(
    strategy: str,
    world_size: int = 8,
    dpr: int = 1,
    dp: int = 1,
    tp: int = 1,
    pp: int = 1,
    ep: int = 1,
) -> Layout:
    return Layout(
        strategy=strategy,
        world_size=world_size,
        data_parallel_replicate_size=dpr,
        data_parallel_size=dp,
        tensor_parallel_size=tp,
        pipeline_parallel_size=pp,
        expert_parallel_size=ep,
    )


def test_build_model_meta_fp32(tmp_path):
    model_path = tiny_model_dir(tmp_path, num_layers=2)
    model = build_model(model_path)
    model_params = list(model.parameters())
    assert len(model_params) > 0
    assert all(p.is_meta and p.dtype == torch.float32 for p in model_params)


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
    model_path = tiny_model_dir(tmp_path, num_layers=2)
    layout = _layout(**sizes)
    model = build_model(model_path)
    mesh = build_mesh(layout, "cpu")
    assert mesh.mesh_dim_names == dim_names
    assert tuple(mesh.shape) == shape

    model = parallelize(model, mesh, layout)
    layer = model.model.layers[0]
    assert layer.self_attn.q_proj.weight.placements == q_placements
    assert layer.self_attn.o_proj.weight.placements == o_placements
    norm = layer.input_layernorm.weight
    if norm_placements is None:
        assert not isinstance(norm, DTensor)
    else:
        assert norm.placements == norm_placements


def test_parallelize_rejects_unknown_strategy(tmp_path, fake_pg):
    model_path = tiny_model_dir(tmp_path, num_layers=2)
    mesh = build_mesh(_layout("fsdp", dp=8), "cpu")
    bad = _layout("ddp", dp=8)
    with pytest.raises(ValueError, match="ddp"):
        parallelize(build_model(model_path), mesh, bad)


@pytest.mark.parametrize(
    "layout",
    [
        pytest.param(_layout("fsdp", dp=8), id="fsdp"),
        pytest.param(_layout("tp", tp=8), id="tp"),
        pytest.param(_layout("pp", pp=8), id="pp"),
    ],
)
def test_parallelize_rejects_tied_embeddings(tmp_path, fake_pg, layout):
    model_path = tiny_model_dir(tmp_path, num_layers=8)
    model = build_model(model_path)
    model.config.tie_word_embeddings = True
    mesh = build_mesh(layout, "cpu")
    with pytest.raises(ValueError, match="tie_word_embeddings"):
        parallelize(model, mesh, layout)


@pytest.mark.parametrize(
    "layout",
    [
        pytest.param(_layout("tp", tp=8), id="tp"),
        pytest.param(_layout("fsdp_tp", dp=2, tp=4), id="fsdp_tp"),
        pytest.param(_layout("hsdp_tp", dpr=2, dp=2, tp=2), id="hsdp_tp"),
        pytest.param(_layout("pp_fsdp_tp", pp=2, dp=2, tp=2), id="pp_fsdp_tp"),
    ],
)
def test_parallelize_rejects_moe_under_tp(tmp_path, fake_pg, layout):
    model_path = tiny_model_dir(tmp_path, kind="olmoe", num_layers=2)
    model = build_model(model_path)
    mesh = build_mesh(layout, "cpu")
    with pytest.raises(ValueError, match="MoE"):
        parallelize(model, mesh, layout)


def _parallel_state(tmp_path, **sizes):
    model_path = tiny_model_dir(tmp_path, num_layers=2)
    weights = load_weights(model_path)
    layout = _layout(**sizes)
    model, optim = build(model_path, layout, SETUP_SEED, weights, "cpu")
    return model, get_fqn_state_dict(model, optim)


def test_get_fqn_state_dict_is_sharded_and_fqn_keyed(tmp_path, fake_pg):
    model, state = _parallel_state(tmp_path, strategy="fsdp", dp=8)
    fqn = "model.layers.0.self_attn.q_proj.weight"
    assert set(state) == {"model", "optim"}
    assert isinstance(state["model"][fqn], DTensor)
    assert state["model"][fqn].to_local().shape == (8, 64)
    for key in ("step", "exp_avg", "exp_avg_sq"):
        assert f"state.{fqn}.{key}" in state["optim"]
    assert state["optim"][f"param_groups.{fqn}.lr"] == 1e-3


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
        # param + exp_avg + exp_avg_sq (fp32) + fp32 AdamW step
        expected += 3 * local + 4
    assert expected_bytes_requested(state) == expected


def test_expected_bytes_requested_ignores_non_tensors():
    state = {
        "a": torch.zeros(4, dtype=torch.float32),
        "b": {"c": torch.zeros(2, 3, dtype=torch.bfloat16), "lr": 1e-3},
        "groups": [{"params": ["a"], "lr": 1e-3}],
    }
    assert expected_bytes_requested(state) == 16 + 12


_PP_STRATEGIES = [
    pytest.param(dict(strategy="pp", pp=8), 8, None, id="pp"),
    pytest.param(dict(strategy="pp_fsdp", pp=2, dp=4), 8, (Shard(0),), id="pp_fsdp"),
    pytest.param(
        dict(strategy="pp_fsdp_tp", pp=2, dp=2, tp=2),
        8,
        (_StridedShard(0, split_factor=2), Shard(0)),
        id="pp_fsdp_tp",
    ),
    pytest.param(
        dict(strategy="pp_hsdp_tp", pp=2, dpr=2, dp=2, tp=2),
        16,
        (Replicate(), _StridedShard(0, split_factor=2), Shard(0)),
        id="pp_hsdp_tp",
    ),
]


@pytest.mark.parametrize("sizes, world_size, q_placements", _PP_STRATEGIES)
def test_pp_stages_partition_state(tmp_path, sizes, world_size, q_placements):
    model_path = tiny_model_dir(tmp_path, num_layers=8)
    weights = load_weights(model_path)
    layout = _layout(world_size=world_size, **sizes)
    num_stages = sizes["pp"]
    layers_per_stage = 8 // num_stages
    full_fqns = set(build_model(model_path).state_dict())
    stage_fqns = []
    for stage in range(num_stages):
        rank = stage * world_size // num_stages
        dist.init_process_group(
            "fake", store=FakeStore(), rank=rank, world_size=world_size
        )
        try:
            mesh = build_mesh(layout, "cpu")
            assert mesh.mesh_dim_names[0] == "pp"
            assert mesh["pp"].get_local_rank() == stage
            model, optim = build(model_path, layout, SETUP_SEED, weights, "cpu")
            state = get_fqn_state_dict(model, optim)
        finally:
            dist.destroy_process_group()

        model_fqns = set(state["model"])
        for kind in ("state", "param_groups"):
            optim_fqns = {
                key[len(kind) + 1 :].rsplit(".", 1)[0]
                for key in state["optim"]
                if key.startswith(f"{kind}.")
            }
            assert optim_fqns == model_fqns
        first = stage * layers_per_stage
        q_fqn = f"model.layers.{first}.self_attn.q_proj.weight"
        q = state["model"][q_fqn]
        if q_placements is None:
            assert not isinstance(q, DTensor)
        else:
            assert q.placements == q_placements
        layers = {fqn.split(".")[2] for fqn in model_fqns if ".layers." in fqn}
        assert layers == {str(i) for i in range(first, first + layers_per_stage)}
        assert ("model.embed_tokens.weight" in model_fqns) == (stage == 0)
        is_last = stage == num_stages - 1
        assert ("model.norm.weight" in model_fqns) == is_last
        assert ("lm_head.weight" in model_fqns) == is_last
        stage_fqns.append(model_fqns)

    assert sum(len(f) for f in stage_fqns) == len(set().union(*stage_fqns))
    assert set().union(*stage_fqns) == full_fqns


def test_split_stage_rejects_tied_embeddings(tmp_path):
    model_path = tiny_model_dir(tmp_path, num_layers=8)
    model = build_model(model_path)
    model.config.tie_word_embeddings = True
    with pytest.raises(ValueError, match="tie_word_embeddings"):
        split_stage(model, 0, 2)


def test_split_stage_rejects_uneven_layers(tmp_path):
    model_path = tiny_model_dir(tmp_path, num_layers=6)
    with pytest.raises(ValueError, match="6 layers"):
        split_stage(build_model(model_path), 0, 4)


def _is_expert(fqn):
    # transformers>=5 fuses each layer's experts into two 3-D weights,
    # mlp.experts.gate_up_proj [E, 2I, H] and mlp.experts.down_proj [E, H, I].
    return ".mlp.experts." in fqn


def test_fsdp_olmoe_slices_every_expert(tmp_path, fake_pg):
    model_path = tiny_model_dir(tmp_path, kind="olmoe", num_layers=2)
    layout = _layout("fsdp", dp=8)
    model = parallelize(build_model(model_path), build_mesh(layout, "cpu"), layout)
    names = [name for name, _ in model.named_parameters()]
    assert "model.layers.0.self_attn.q_norm.weight" in names
    assert "model.layers.0.mlp.gate.weight" in names
    assert sum(_is_expert(n) for n in names) == 2 * 2
    for name, p in model.named_parameters():
        expected = (Shard(1),) if _is_expert(name) else (Shard(0),)
        assert p.placements == expected, name
        assert p.device_mesh.mesh_dim_names == ("dp_shard",)
    # Every rank holds a slice of all 16 experts.
    down = model.model.layers[1].mlp.experts.down_proj
    assert down.to_local().shape == (16, 64 // 8, 32)


def test_ep_ranks_own_disjoint_experts(tmp_path):
    model_path = tiny_model_dir(tmp_path, kind="olmoe", num_layers=2)
    weights = load_weights(model_path)
    layout = _layout("ep", dp=8, ep=8)
    full = build_model(model_path)
    full_names = {n for n, _ in full.named_parameters()}
    full_experts = {n for n in full_names if _is_expert(n)}
    assert len(full_experts) == 2 * 2
    param_bytes = {n: p.numel() * p.element_size() for n, p in full.named_parameters()}
    owned = {fqn: [] for fqn in full_experts}
    for rank in range(8):
        dist.init_process_group("fake", store=FakeStore(), rank=rank, world_size=8)
        try:
            mesh = build_mesh(layout, "cpu")
            # EP reuses the dp_shard ranks; it is not a mesh dim of its own.
            assert mesh.mesh_dim_names == ("dp_shard",)
            model, optim = build(model_path, layout, SETUP_SEED, weights, "cpu")
            state = get_fqn_state_dict(model, optim)

            assert set(state["model"]) == full_names
            for fqn, t in state["model"].items():
                assert t.placements == (Shard(0),), fqn
                assert t.device_mesh.mesh_dim_names == ("dp_shard",)
            for fqn in full_experts:
                t = state["model"][fqn]
                # Offsets from this rank's mesh: re-initializing the fake process
                # group in one process leaves FSDP's DTensors on a mesh cached
                # from rank 0.
                shape, offset = compute_local_shape_and_global_offset(
                    t.shape, mesh, t.placements
                )
                # Whole experts: only the expert dim is split.
                assert tuple(shape) == (2, *t.shape[1:]), fqn
                assert tuple(offset[1:]) == (0,) * (t.dim() - 1), fqn
                owned[fqn].append(list(range(offset[0], offset[0] + shape[0])))
                exp_avg = state["optim"][f"state.{fqn}.exp_avg"]
                assert exp_avg.placements == (Shard(0),), fqn
                assert model.get_parameter(fqn).requires_grad, fqn
        finally:
            dist.destroy_process_group()

        optim_fqns = {
            key[len("state.") :].rsplit(".", 1)[0]
            for key in state["optim"]
            if key.startswith("state.")
        }
        assert optim_fqns == full_names
        expected = sum(3 * param_bytes[f] // 8 + 4 for f in full_names)
        assert expected_bytes_requested(state) == expected

    # Rank r owns experts 2r and 2r+1 of every expert weight: disjoint, and
    # together they cover all 16.
    for fqn, per_rank in owned.items():
        assert per_rank == [[2 * r, 2 * r + 1] for r in range(8)], fqn


def test_ep_rejects_uneven_experts(tmp_path, fake_pg):
    model_path = tiny_model_dir(tmp_path, kind="olmoe", num_layers=2, num_experts=12)
    layout = _layout("ep", dp=8, ep=8)
    with pytest.raises(ValueError, match="12 experts"):
        parallelize(build_model(model_path), build_mesh(layout, "cpu"), layout)
