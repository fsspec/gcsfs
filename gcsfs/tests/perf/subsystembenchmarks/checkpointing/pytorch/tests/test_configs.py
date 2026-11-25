import json
import pathlib

import pytest

from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch import configs
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.configs import (
    HostConfig,
    PyTorchCheckpointConfigurator,
    case_params,
    host_config,
    placement_skip_reason,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.layout import Layout
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.tests.tiny import (
    make_params,
)

CONFIG = configs.__file__
LLAMA = "gs://gcs-aiml-huggingface-model-weights/Llama-3.1-8B"
OLMOE = "gs://gcs-aiml-huggingface-model-weights/OLMoE-1B-7B-0924"

EXPECTED_LOAD_IDS = [
    "load-llama_3_1_8b-fsdp-ws8-reg",
    "load-llama_3_1_8b-hsdp-dpr2dp4-ws8-reg",
    "load-llama_3_1_8b-hsdp-dpr4dp2-ws8-reg",
    "load-llama_3_1_8b-tp-tp8-ws8-reg",
    "load-llama_3_1_8b-fsdp-tp-dp2tp4-ws8-reg",
    "load-llama_3_1_8b-hsdp-tp-dpr2dp2tp2-ws8-reg",
    "load-llama_3_1_8b-pp-pp8-ws8-reg",
    "load-llama_3_1_8b-pp-fsdp-pp2dp4-ws8-reg",
    "load-llama_3_1_8b-pp-fsdp-tp-pp2dp2tp2-ws8-reg",
    "load-llama_3_1_8b-pp-hsdp-tp-pp2dpr2dp2tp2-ws16-reg",
    "load-olmoe_1b_7b_0924-fsdp-ws8-reg",
    "load-olmoe_1b_7b_0924-ep-dp8ep8-ws8-reg",
]


def _cases():
    return PyTorchCheckpointConfigurator(CONFIG).generate_cases()


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for key in (
        "GCSFS_SUBSYSTEM_MODEL_ID",
        "GCSFS_SUBSYSTEM_BUCKET_TYPE",
        "GCSFS_SUBSYSTEM_SWEEP_AXES",
        "GCSFS_BENCHMARK_FILTER",
        "GCSFS_SUBSYSTEM_CONFIG",
        "GCSFS_SUBSYSTEM_DEVICE",
    ):
        monkeypatch.delenv(key, raising=False)


def test_row_count_and_framework():
    cases = _cases()
    assert len(cases) == 12
    assert {c.scenario for c in cases} == {"checkpoint_read"}
    assert {c.framework for c in cases} == {"pytorch"}
    assert all(c.source is None and "-from-" not in c.name for c in cases)


def test_load_ids_unchanged(monkeypatch):
    monkeypatch.setenv("GCSFS_SUBSYSTEM_BUCKET_TYPE", "regional")
    assert [c.name for c in _cases()] == EXPECTED_LOAD_IDS


def test_ws16_rows_skip_on_8_rank_host():
    params = case_params(PyTorchCheckpointConfigurator, CONFIG, host=HostConfig(1, 8))
    assert len(params) == 12
    assert all(p.id == p.values[0].name for p in params)
    skipped = {}
    for param in params:
        if param.marks:
            (mark,) = param.marks
            assert mark.mark.name == "skip"
            skipped[param.id] = mark.mark.kwargs["reason"]
    assert skipped == {
        "load-llama_3_1_8b-pp-hsdp-tp-pp2dpr2dp2tp2-ws16-reg": (
            "needs 16 ranks, run has 1x8"
        )
    }


def test_case_params_default_host_follows_cuda_device_count(monkeypatch):
    import torch

    from gcsfs.tests.perf.subsystembenchmarks.checkpointing._dist import MAX_RANKS

    assert HostConfig() == HostConfig(1, MAX_RANKS)
    assert host_config("cpu") == HostConfig(1, MAX_RANKS)
    monkeypatch.setattr(torch.cuda, "device_count", lambda: 8)
    assert host_config("cuda") == HostConfig(1, 8)

    monkeypatch.setenv("GCSFS_SUBSYSTEM_DEVICE", "cuda")
    monkeypatch.setattr(torch.cuda, "device_count", lambda: 4)
    default_params = case_params(PyTorchCheckpointConfigurator, CONFIG)
    assert len(default_params) == 12
    assert all(p.marks for p in default_params)


@pytest.mark.parametrize("model_id", [OLMOE, "gs://b/Llama-3.1-70B"])
def test_model_id_override_skips_rows_that_pin_model_id(monkeypatch, model_id):
    # The OLMoE rows pin their own model_id. With --model-id they are skipped,
    # so the override means "exactly this model" (and OLMoE as the override
    # does not collide with the OLMoE fsdp row on the same benchmark id).
    monkeypatch.setenv("GCSFS_SUBSYSTEM_MODEL_ID", model_id)
    cases = _cases()
    assert len(cases) == 10
    assert {c.model_id for c in cases} == {model_id}
    assert not any("olmoe" in c.name for c in cases if model_id != OLMOE)


@pytest.mark.parametrize(
    "sizes",
    [
        pytest.param(dict(strategy="tp", dp=8), id="tp-without-tp"),
        pytest.param(dict(strategy="fsdp", dpr=2, dp=4), id="fsdp-with-replicate"),
    ],
)
def test_validate_case_rejects_sizes_unused_by_strategy(sizes):
    p = make_params(world_size=8, **sizes)
    with pytest.raises(ValueError, match="needs non-unit sizes"):
        PyTorchCheckpointConfigurator(CONFIG).validate_case(p)


def test_validate_case_allows_world_size_above_max_ranks():
    p = make_params(dp=32, world_size=32)
    PyTorchCheckpointConfigurator(CONFIG).validate_case(p)


def test_benchmark_name_reshard():
    fsdp8 = {"strategy": "fsdp", "data_parallel_size": 8, "world_size": 8}
    reshard = make_params(LLAMA, dp=4, world_size=4, source=fsdp8)
    assert reshard.benchmark_name() == "load-llama_3_1_8b-fsdp-ws4-from-fsdp-ws8-reg"

    plain_load = make_params(LLAMA, dp=4, world_size=4)
    assert plain_load.benchmark_name() == "load-llama_3_1_8b-fsdp-ws4-reg"

    plain_save = make_params(LLAMA, dp=4, world_size=4, scenario="checkpoint_write")
    assert plain_save.benchmark_name() == "save-llama_3_1_8b-fsdp-ws4-reg"


def test_requires_source_rule():
    class _ReshardConfigurator(PyTorchCheckpointConfigurator):
        REQUIRES_SOURCE = True

    assert PyTorchCheckpointConfigurator.REQUIRES_SOURCE is False
    plain = PyTorchCheckpointConfigurator(CONFIG)
    reshard = _ReshardConfigurator(CONFIG)

    def _p(source):
        return make_params(dp=4, world_size=4, source=source)

    without_source = _p(None)
    with_source = _p({"strategy": "fsdp", "data_parallel_size": 8, "world_size": 8})

    # Default configurator (REQUIRES_SOURCE=False) accepts no-source, rejects source.
    plain.validate_case(without_source)
    with pytest.raises(ValueError, match="source"):
        plain.validate_case(with_source)

    # REQUIRES_SOURCE=True rejects no-source and same-layout sources, accepts a
    # different valid source, and validates the source layout.
    with pytest.raises(ValueError, match="source"):
        reshard.validate_case(without_source)
    reshard.validate_case(with_source)
    with pytest.raises(ValueError, match="needs non-unit sizes"):
        reshard.validate_case(
            _p({"strategy": "tp", "data_parallel_size": 8, "world_size": 8})
        )
    with pytest.raises(ValueError, match="source"):
        reshard.validate_case(
            _p({"strategy": "fsdp", "data_parallel_size": 4, "world_size": 4})
        )


def _write_config(tmp_path, baseline: str, model_id: str = LLAMA) -> str:
    (tmp_path / "configs.yaml").write_text(f"""
common:
  rounds: 1
  model_id: "{model_id}"
  baseline:
{baseline}
scenarios:
  - name: "checkpoint_read"
    scenario: "checkpoint_read"
""")
    return str(tmp_path / "configs.py")


@pytest.mark.parametrize(
    "setup_world_size",
    [pytest.param(4, id="different-from-world"), pytest.param(8, id="equal-to-world")],
)
def test_validate_case_rejects_setup_keys(tmp_path, setup_world_size):
    config = _write_config(
        tmp_path,
        "    strategy: fsdp\n    data_parallel_size: 8\n    world_size: 8\n"
        f"    setup_world_size: {setup_world_size}",
    )
    with pytest.raises(ValueError, match="not setup_"):
        PyTorchCheckpointConfigurator(config).generate_cases()


def test_expert_parallel_size_must_match_dp(tmp_path):
    config = _write_config(
        tmp_path,
        "    strategy: ep\n    data_parallel_size: 8\n"
        "    expert_parallel_size: 4\n    world_size: 8",
        model_id=OLMOE,
    )
    with pytest.raises(ValueError, match="expert_parallel_size"):
        PyTorchCheckpointConfigurator(config).generate_cases()


def test_parameters_layouts_max_world_size_and_extra_columns(monkeypatch):
    p = make_params(
        OLMOE,
        strategy="fsdp_tp",
        dp=4,
        tp=2,
        world_size=8,
        source={
            "strategy": "pp_hsdp_tp",
            "pipeline_parallel_size": 2,
            "data_parallel_replicate_size": 2,
            "data_parallel_size": 2,
            "tensor_parallel_size": 2,
            "world_size": 16,
        },
    )
    assert p.target_layout() == Layout(
        strategy="fsdp_tp", world_size=8, data_parallel_size=4, tensor_parallel_size=2
    )
    assert p.source_layout() == Layout(
        strategy="pp_hsdp_tp",
        world_size=16,
        pipeline_parallel_size=2,
        data_parallel_replicate_size=2,
        data_parallel_size=2,
        tensor_parallel_size=2,
    )
    assert p.max_world_size() == 16
    assert p.target_layout().mesh_dims() == [("dp_shard", 4), ("tp", 2)]
    assert not hasattr(p, "mesh_dims")
    assert p.extra_columns() == {
        "device": "cpu",
        "world_size": 8,
        "tensor_parallel_size": 2,
        "data_parallel_size": 4,
        "data_parallel_replicate_size": 1,
        "pipeline_parallel_size": 1,
        "expert_parallel_size": 1,
        "source_strategy": "pp_hsdp_tp",
        "setup_world_size": 16,
        "setup_tensor_parallel_size": 2,
        "setup_data_parallel_size": 2,
        "setup_data_parallel_replicate_size": 2,
        "setup_pipeline_parallel_size": 2,
        "setup_expert_parallel_size": 1,
    }
    monkeypatch.setenv("GCSFS_SUBSYSTEM_DEVICE", "cuda")
    assert p.extra_columns()["device"] == "cuda"

    wider_target = make_params(
        dp=16,
        world_size=16,
        source={
            "strategy": "ep",
            "data_parallel_size": 8,
            "expert_parallel_size": 8,
            "world_size": 8,
        },
    )
    assert wider_target.max_world_size() == 16
    assert wider_target.extra_columns()["setup_expert_parallel_size"] == 8
    assert wider_target.extra_columns()["source_strategy"] == "ep"

    no_source = make_params(dp=8, world_size=8)
    assert no_source.source_layout() == no_source.target_layout()
    assert no_source.max_world_size() == 8


def test_baseline_is_fsdp():
    cases = _cases()
    (baseline,) = [c for c in cases if c.sweep_axis == "baseline"]
    assert baseline.strategy == "fsdp"
    assert baseline.name == "load-llama_3_1_8b-fsdp-ws8-reg"
    assert len([c for c in cases if c.sweep_axis == "strategy"]) == 10


def test_olmoe_cases():
    cases = {c.name: c for c in _cases() if c.model_id == OLMOE}
    fsdp = cases["load-olmoe_1b_7b_0924-fsdp-ws8-reg"]
    ep = cases["load-olmoe_1b_7b_0924-ep-dp8ep8-ws8-reg"]
    assert (fsdp.sweep_axis, fsdp.strategy, fsdp.expert_parallel_size) == (
        "model",
        "fsdp",
        1,
    )
    assert (ep.sweep_axis, ep.strategy, ep.expert_parallel_size) == (
        "strategy",
        "ep",
        8,
    )
    for c in (fsdp, ep):
        assert (c.data_parallel_size, c.world_size) == (8, 8)


def test_columns_in_schema():
    root = pathlib.Path(__file__).resolve().parents[7]
    schema_path = (
        root / "cloudbuild/subsystembenchmarks/subsystembenchmarks_schema.json"
    )
    with open(schema_path) as f:
        fields = json.load(f)["schema"]["fields"]
    fields_by_name = {f["name"]: f for f in fields}

    for field in fields:
        assert "mode" not in field, f"unexpected explicit mode on {field['name']}"

    populated_by = "Populated by checkpointing/pytorch."
    for col, col_type in {
        "device": "STRING",
        "source_strategy": "STRING",
        "setup_data_parallel_replicate_size": "INTEGER",
        "setup_pipeline_parallel_size": "INTEGER",
        "setup_expert_parallel_size": "INTEGER",
        "dcp_read_items_per_rank_max": "INTEGER",
    }.items():
        assert col in fields_by_name, f"column {col} missing from schema"
        assert fields_by_name[col]["type"] == col_type
        assert populated_by in fields_by_name[col]["description"], col

    for c in _cases():
        for col in c.extra_columns():
            assert col in fields_by_name, f"param column {col} missing from schema"


def test_placement_skip_reason_cuda_capacity():
    ws16 = make_params(strategy="pp_hsdp_tp", pp=2, dpr=2, dp=2, tp=2, world_size=16)
    assert placement_skip_reason(ws16, HostConfig(1, 8)) == (
        "needs 16 ranks, run has 1x8"
    )
    assert placement_skip_reason(ws16, HostConfig(1, 16)) is None
    assert placement_skip_reason(ws16, HostConfig(2, 8)) is None

    reshard = make_params(
        dp=4,
        world_size=4,
        source={"strategy": "fsdp", "data_parallel_size": 8, "world_size": 8},
    )
    assert placement_skip_reason(reshard, HostConfig(1, 4)) == (
        "needs 8 ranks, run has 1x4"
    )
    assert placement_skip_reason(reshard, HostConfig(1, 8)) is None
